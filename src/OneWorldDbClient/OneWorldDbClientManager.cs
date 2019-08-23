using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading.Tasks;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public class OneWorldDbClientManager<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly ILogger<OneWorldDbClientManager<TDbContext>> _logger;
        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _loggerTx;

        private readonly string _connectionString;

        private readonly Func<string, IDbConnection> _dbConnectionFactory;
        private readonly Func<IDbConnection, IDbTransaction, TDbContext> _dbContextFactory;

        private readonly ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>> _transactions
            = new ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>>();

        private readonly ConcurrentStack<Guid> _publishedTransactionStack
            = new ConcurrentStack<Guid>();


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
                return await BeginTranRequiresNewAsync(isolationLevel);

            // current tx found.
            if (!_transactions.TryGetValue(latestTransactionId, out var transaction))
                throw new InvalidProgramException($"lost transaction {latestTransactionId}");

            if (!isolationLevel.HasValue || isolationLevel == transaction.IsolationLevel)
                return await transaction.CreateTransactionScopeAsync();

            throw new InvalidOperationException(
                $"{nameof(isolationLevel)} で指定されたトランザクション {isolationLevel} は、" +
                $"既に存在するトランザクションレベル {transaction.IsolationLevel} と相違しています。");
        }


        /// <summary>
        /// トランザクションを新規に開始します。
        /// </summary>
        /// <param name="isolationLevel">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiresNewAsync(
            IsolationLevel? isolationLevel = null)
        {
            var firstTran = OneWorldDbTransaction<TDbContext>.CreateOneWorldDbTransaction(
                Guid.NewGuid(),
                _transactions.Count,
                _dbConnectionFactory.Invoke(_connectionString),
                isolationLevel ?? IsolationLevel.ReadCommitted,
                this,
                _dbContextFactory,
                _loggerTx);

            if (!_transactions.TryAdd(firstTran.TransactionId, firstTran))
                throw new InvalidProgramException($"duplicate transaction id {firstTran.TransactionId}");

            _publishedTransactionStack.Push(firstTran.TransactionId);

            return await firstTran.CreateTransactionScopeAsync();
        }


        public void SubTransactionFinalize(Guid endedSubTransactionId)
        {
            // strictly, strictly, strictly...
            lock (_publishedTransactionStack)
            {
                if (_publishedTransactionStack.TryPeek(out var latestTransactionId))
                {
                    if (endedSubTransactionId == latestTransactionId)
                    {
                        if (_publishedTransactionStack.TryPop(out _))
                        {
                            if (_transactions.TryRemove(endedSubTransactionId, out var transaction))
                            {
                                _logger?.LogInformation($" sub.tx={endedSubTransactionId} Disposing.");
                                transaction.Dispose();
                                _logger?.LogInformation($" sub.tx={endedSubTransactionId} Disposed and Removed.");
                            }
                            else
                            {
                                _logger?.LogInformation($" sub.tx={endedSubTransactionId} Already Removed.");
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException($"failed {nameof(_publishedTransactionStack)} 's TryPeek().");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"{nameof(latestTransactionId)} {latestTransactionId} and {endedSubTransactionId} are different.");
                    }
                }
                else
                {
                    throw new InvalidOperationException($"failed {nameof(_publishedTransactionStack)} 's TryPeek().");
                }
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _logger?.LogInformation($"Dispose {nameof(OneWorldDbClientManager<TDbContext>)} start.");

                while (_publishedTransactionStack.Count > 0)
                {
                    _publishedTransactionStack.TryPop(out var latestTransactionId);

                    if (_transactions.TryRemove(latestTransactionId, out var transaction))
                    {
                        _logger?.LogInformation($" tx={latestTransactionId} Disposing.");
                        transaction.Dispose();
                        _logger?.LogInformation($" tx={latestTransactionId} Disposed and Removed.");
                    }
                    else
                    {
                        _logger?.LogInformation($" tx={latestTransactionId} Already Removed.");
                    }
                }

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbClientManager<TDbContext>)} ended.");
            }
        }


        ~OneWorldDbClientManager()
        {
            Dispose(false);
        }
    }
}
