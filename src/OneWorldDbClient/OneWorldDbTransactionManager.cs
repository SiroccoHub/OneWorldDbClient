using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public class OneWorldDbTransactionManager<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly ILoggerFactory _loggingFactory;
        private readonly ILogger<OneWorldDbTransactionManager<TDbContext>> _logger;

        private readonly string _connectionString;

        private readonly ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>> _transactions
            = new ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>>();

        private readonly ConcurrentStack<Guid> _publishedTransactionStack
            = new ConcurrentStack<Guid>();

        private readonly Func<DbConnection, TDbContext> _dbContextFactory;


        public OneWorldDbTransactionManager(
            string connectionString,
            Func<DbConnection, TDbContext> dbContextFactory,
            ILoggerFactory loggerFactory)
        {
            _loggingFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<OneWorldDbTransactionManager<TDbContext>>();
            _connectionString = connectionString;
            _dbContextFactory = dbContextFactory;
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
            var conn = new SqlConnection(_connectionString);

            var firstTran = OneWorldDbTransaction<TDbContext>.CreateOneWorldDbTransaction(
                Guid.NewGuid(),
                _transactions.Count,
                conn,
                isolationLevel ?? IsolationLevel.ReadCommitted,
                this,
                _dbContextFactory,
                _loggingFactory.CreateLogger<OneWorldDbTransaction<TDbContext>>());


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
                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransactionManager<TDbContext>)} start.");

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

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransactionManager<TDbContext>)} ended.");
            }
        }


        ~OneWorldDbTransactionManager()
        {
            Dispose(false);
        }
    }
}
