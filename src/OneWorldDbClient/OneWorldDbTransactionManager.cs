using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public class OneWorldDbTransactionManager<T> : IDisposable
    {
        private readonly ILoggerFactory _loggingFactory;
        private readonly ILogger<OneWorldDbTransactionManager<T>> _logger;

        private readonly string _connectionString;

        private readonly ConcurrentDictionary<Guid, OneWorldDbTransaction<T>> _transactions
            = new ConcurrentDictionary<Guid, OneWorldDbTransaction<T>>();

        private readonly ConcurrentStack<Guid> _publishedTransactionStack
            = new ConcurrentStack<Guid>();


        private static readonly object Initializing = new object();
        private static bool _initialized;


        public OneWorldDbTransactionManager(
            string connectionString,
            ILoggerFactory loggerFactory)
        {
            _loggingFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<OneWorldDbTransactionManager<T>>();
            _connectionString = connectionString;

            if (_initialized)
                return;

            lock (Initializing)
            {
                if (_initialized)
                    return;

                // ColumnAttributeTypeMapperBootstrap.Init();
                _initialized = true;
            }
        }


        /// <summary>
        /// トランザクションが存在すれば参加し、なければ新規に作成します。
        /// </summary>
        /// <param name="iso">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<T>> BeginTranRequiredAsync(
            IsolationLevel? iso = null)
        {
            // 既存トランザクションがないので新規作成
            if (!_publishedTransactionStack.TryPeek(out var latestTransactionId))
                return await BeginTranRequiresNewAsync(iso);

            // 既存トランザクションが存在
            if (!_transactions.TryGetValue(latestTransactionId, out var transaction))
                throw new InvalidProgramException($"lost transaction {latestTransactionId}");

            if (!iso.HasValue || iso == transaction.IsolationLevel)
                return await transaction.CreateTransactionScopeAsync();

            throw new InvalidOperationException(
                $"{nameof(iso)} で指定されたトランザクション {iso} は、" +
                $"既に存在するトランザクションレベル {transaction.IsolationLevel} と相違しています。");
        }


        /// <summary>
        /// トランザクションを新規に開始します。
        /// </summary>
        /// <param name="iso">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<T>> BeginTranRequiresNewAsync(
            IsolationLevel? iso = null)
        {
            var conn = new SqlConnection(_connectionString);

            var firstTran = OneWorldDbTransaction<T>.CreateOneWorldDbTransaction(
                Guid.NewGuid(),
                _transactions.Count,
                conn,
                iso ?? IsolationLevel.ReadCommitted,
                this,
                _loggingFactory.CreateLogger<OneWorldDbTransaction<T>>());


            if (!_transactions.TryAdd(firstTran.TransactionId, firstTran))
                throw new InvalidProgramException($"duplicate transaction id {firstTran.TransactionId}");

            _publishedTransactionStack.Push(firstTran.TransactionId);

            return await firstTran.CreateTransactionScopeAsync();
        }


        public void SubTransactionFinalize(Guid endedSubTransactionId)
        {
            // 厳重すぎて、実行環境ではここまでやる必要ないはずなんだけど
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
                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransactionManager<T>)} start.");

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

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransactionManager<T>)} ended.");
            }
        }


        ~OneWorldDbTransactionManager()
        {
            Dispose(false);
        }
    }
}
