using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public class OneWorldDbTransaction<TDbContext> : IDisposable where TDbContext : DbContext
    {
        public readonly Guid TransactionId;

        public int TransactionNumber { get; }

        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _logger;

        private readonly OneWorldDbClientManager<TDbContext> _oneWorldDbClientManager;
        private readonly Func<IDbConnection, IDbTransaction, TDbContext> _dbContextFactory;

        private readonly IDbConnection _dbConnection;
        private IDbTransaction _dbTransaction;
        private TDbContext _dbContext;

        public IsolationLevel IsolationLevel { get; }

        private int _activeChildren = 0;
        private int _totalChildren = 0;

        private int _beCommits = 0;
        private int _beRollbacks = 0;

        private readonly ConcurrentDictionary<OneWorldDbTransactionScope<TDbContext>, bool?> _answers
            = new ConcurrentDictionary<OneWorldDbTransactionScope<TDbContext>, bool?>();


        private OneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            IDbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbClientManager<TDbContext> oneWorldDbClientManager,
            Func<IDbConnection, IDbTransaction, TDbContext> dbContextFactory,
            ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            TransactionId = transactionId;
            TransactionNumber = transactionNumber;
            _dbConnection = dbConnection;
            IsolationLevel = isolationLevel;
            _oneWorldDbClientManager = oneWorldDbClientManager;
            _dbContextFactory = dbContextFactory;
            _logger = logger;
        }


        public static OneWorldDbTransaction<TDbContext> CreateOneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            IDbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbClientManager<TDbContext> oneWorldDbClientManager,
            Func<IDbConnection, IDbTransaction, TDbContext> dbContextFactory,
            ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            return new OneWorldDbTransaction<TDbContext>(
                transactionId,
                transactionNumber,
                dbConnection,
                isolationLevel,
                oneWorldDbClientManager,
                dbContextFactory,
                logger);
        }


        public async Task<OneWorldDbTransactionScope<TDbContext>> CreateTransactionScopeAsync(
            string memberName = "",
            string sourceFilePath = "",
            int sourceLineNumber = 0)
        {
            if (_dbConnection.State != ConnectionState.Open)
                await ((DbConnection)_dbConnection).OpenAsync();

            if (_dbTransaction == null)
                _dbTransaction = ((DbConnection)_dbConnection).BeginTransaction(IsolationLevel);

            if (_dbContext == null && _dbContextFactory != null)
                _dbContext = _dbContextFactory.Invoke(_dbConnection, _dbTransaction);

            var ts = OneWorldDbTransactionScope<TDbContext>.Create(
                _dbConnection,
                _dbTransaction,
                _dbContext,
                this,
                _logger,
                memberName,
                sourceFilePath,
                sourceLineNumber);

            ++_activeChildren;
            ++_totalChildren;

            _answers.TryAdd(ts, null);

            return ts;
        }


        public VotingResult CommitPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, true, null))
                return VotingResult.AlreadyVoted;

            ++_beCommits;
            return VotingResult.VoteComplete;
        }


        public VotingResult RollbackPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, false, null))
                return VotingResult.AlreadyVoted;

            ++_beRollbacks;
            return VotingResult.VoteComplete;
        }


        public int RollbacksAlreadyDecided()
        {
            return _beRollbacks;
        }


        public void Leave()
        {
            if (--_activeChildren == 0 && TransactionNumber != 0)
                _oneWorldDbClientManager.SubTransactionFinalize(TransactionId);
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
                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransaction<TDbContext>)} start.");
                _logger?.LogTrace($" totalChildren:{_totalChildren}/commits:{_beCommits}/rollbacks:{_beRollbacks}.");

                if (_totalChildren == _beCommits)
                {
                    _dbTransaction?.Commit();
                    _logger?.LogInformation($" done Commit().");
                }
                else
                {
                    _dbTransaction?.Rollback();
                    _logger?.LogInformation($" done Rollback().");
                }

                _logger?.LogTrace($" isNull _dbContext={_dbContext == null}");
                _dbContext?.Dispose();

                _logger?.LogTrace($" isNull _dbTransaction={_dbTransaction == null}");
                _dbTransaction?.Dispose();

                _logger?.LogTrace($" isNull _dbConnection={_dbConnection == null}");
                _dbConnection?.Close();
                _dbConnection?.Dispose();

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransaction<TDbContext>)} done.");
            }
        }


        ~OneWorldDbTransaction()
        {
            Dispose(false);
        }
    }
}
