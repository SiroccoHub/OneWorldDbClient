using System.Collections.Concurrent;
using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace OneWorldDbClient
{
    public sealed class OneWorldDbClientManager<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly string _connectionString;

        private readonly Func<string, IDbConnection> _dbConnectionFactory;
        private readonly Func<IDbConnection, IDbTransaction, TDbContext> _dbContextFactory;
        private readonly ILogger<OneWorldDbClientManager<TDbContext>> _logger;
        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _loggerTx;

        private readonly ConcurrentStack<Guid> _publishedTransactionStack
            = new ConcurrentStack<Guid>();

        private readonly ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>> _transactions
            = new ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>>();


        public OneWorldDbClientManager(
            string connectionString,
            Func<IDbConnection, IDbTransaction, TDbContext> dbContextFactory,
            Func<string, IDbConnection> dbConnectionFactory,
            ILogger<OneWorldDbClientManager<TDbContext>> logger,
            ILogger<OneWorldDbTransaction<TDbContext>> loggerTx)
        {
            _logger = logger;
            _loggerTx = loggerTx;

            _connectionString = connectionString;

            _dbConnectionFactory = dbConnectionFactory ?? throw new ArgumentNullException(nameof(dbConnectionFactory));
            _dbContextFactory = dbContextFactory ?? throw new ArgumentNullException(nameof(dbContextFactory));
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// トランザクションが存在すれば参加し、なければ新規に作成します。
        /// </summary>
        /// <param name="isolationLevel">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiredAsync(
            IsolationLevel? isolationLevel = null)
        {
            // current tx not found. create new tx.
            if (!_publishedTransactionStack.TryPeek(out var latestTransactionId))
                return await BeginTranRequiresNewInternalAsync(
                    isSuperRootTx: true,
                    isolationLevel: isolationLevel);

            // current tx found.
            if (!_transactions.TryGetValue(latestTransactionId, out var transaction))
                throw new InvalidProgramException($"lost transaction {latestTransactionId}");

            _logger.LogInformation(" Required:TransactionNumber=`{TransactionNumber}`/TransactionId=`{TransactionId}`/IsolationLevel=`{IsolationLevel}`", transaction.TransactionNumber, transaction.TransactionId, transaction.IsolationLevel);

            if (!isolationLevel.HasValue || isolationLevel == transaction.IsolationLevel)
                return await transaction.CreateTransactionScopeAsync();

            throw new InvalidOperationException(
                $"{nameof(isolationLevel)} で指定されたトランザクション {isolationLevel} は、既に存在するトランザクションレベル {transaction.IsolationLevel} と相違しています。");
        }


        /// <summary>
        /// トランザクションを新規に開始します。
        /// </summary>
        /// <param name="isolationLevel">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiresNewAsync(
            IsolationLevel? isolationLevel = null)
        {
            return await BeginTranRequiresNewInternalAsync(
                isSuperRootTx: false,
                isolationLevel: isolationLevel);
        }


        /// <summary>
        /// トランザクションを新規に開始する内部メソッド
        /// </summary>
        /// <param name="isSuperRootTx"></param>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        private async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiresNewInternalAsync(
            bool isSuperRootTx, 
            IsolationLevel? isolationLevel = null)
        {
            var transactionNumber = isSuperRootTx ? -1 : _transactions.Count;
            var firstTran = 
                OneWorldDbTransaction<TDbContext>.CreateOneWorldDbTransaction(
                    transactionId: Guid.NewGuid(),
                    transactionNumber: transactionNumber,
                    dbConnection: _dbConnectionFactory.Invoke(_connectionString),
                    isolationLevel: isolationLevel ?? IsolationLevel.ReadCommitted,
                    oneWorldDbClientManager: this,
                    dbContextFactory: _dbContextFactory,
                    logger: _loggerTx);

            _logger.LogInformation(" RequiresNew:isSuperRootTx=`{isSuperRootTx}`,TransactionNumber=`{TransactionNumber}`/TransactionId=`{TransactionId}`/IsolationLevel=`{IsolationLevel}`", isSuperRootTx, firstTran.TransactionNumber, firstTran.TransactionId, firstTran.IsolationLevel);

            if (!_transactions.TryAdd(firstTran.TransactionId, firstTran))
                throw new InvalidProgramException($"duplicate transaction id {firstTran.TransactionId}");

            _publishedTransactionStack.Push(firstTran.TransactionId);

            return await firstTran.CreateTransactionScopeAsync();
        }


        internal void TransactionFinalize(Guid finalizeSubTransactionId)
        {
            lock (_publishedTransactionStack)
            {
                if (_publishedTransactionStack.TryPeek(out var tranId))
                {
                    if (finalizeSubTransactionId == tranId)
                    {
                        if (_publishedTransactionStack.TryPop(out _))
                        {
                            if (_transactions.TryRemove(finalizeSubTransactionId, out var transaction))
                            {
                                _logger.LogTrace(" sub.tx={finalizeSubTransactionId} Disposing.", finalizeSubTransactionId);
                                transaction.Dispose();
                                _logger.LogDebug(" sub.tx={finalizeSubTransactionId} Disposed and Removed.", finalizeSubTransactionId);
                            }
                            else
                            {
                                _logger.LogDebug(" sub.tx={finalizeSubTransactionId} Already Removed.", finalizeSubTransactionId);
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException($"failed {nameof(_publishedTransactionStack)} 's TryPop().");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"{nameof(tranId)} {tranId} and {finalizeSubTransactionId} are different.");
                    }
                }
                else
                {
                    throw new InvalidOperationException($"failed {nameof(_publishedTransactionStack)} 's TryPeek().");
                }
            }
        }


        private void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            _logger.LogDebug($" Dispose {nameof(OneWorldDbClientManager<TDbContext>)} start.");

            while (!_publishedTransactionStack.IsEmpty)
            {
                _publishedTransactionStack.TryPop(out var tranId);

                if (_transactions.TryRemove(tranId, out var transaction))
                {
                    _logger?.LogDebug(" tx={tranId} Disposing.", tranId);
                    transaction.Dispose();
                    _logger?.LogDebug(" tx={tranId} Disposed and Removed.", tranId);
                }
                else
                {
                    _logger?.LogDebug(" tx={tranId} Already Removed.", tranId);
                }
            }

            _logger?.LogInformation($" Dispose {nameof(OneWorldDbClientManager<TDbContext>)} ended.");
        }


        ~OneWorldDbClientManager()
        {
            Dispose(false);
        }
    }
}