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

        public int TransactionNumber { get; private set; }

        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _logger;

        private readonly OneWorldDbTransactionManager<TDbContext> _oneWorldDbTransactionManager;
        private readonly Func<DbConnection, TDbContext> _dbContextFactory;

        private readonly DbConnection _dbConnection;
        private DbTransaction _dbTransaction;
        private TDbContext _dbContext;

        public IsolationLevel IsolationLevel { get; private set; }

        private int _activeChildren = 0;
        private int _totalChildren = 0;

        private int _beCommits = 0;
        private int _beRollbacks = 0;

        private readonly ConcurrentDictionary<OneWorldDbTransactionScope<TDbContext>, bool?> _answers
            = new ConcurrentDictionary<OneWorldDbTransactionScope<TDbContext>, bool?>();


        private OneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            DbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbTransactionManager<TDbContext> oneWorldDbTransactionManager,
            Func<DbConnection, TDbContext> dbContextFactory,
            ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            TransactionId = transactionId;
            TransactionNumber = transactionNumber;
            _dbConnection = dbConnection;
            IsolationLevel = isolationLevel;
            _oneWorldDbTransactionManager = oneWorldDbTransactionManager;
            _dbContextFactory = dbContextFactory;
            _logger = logger;
        }


        public static OneWorldDbTransaction<TDbContext> CreateOneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            DbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbTransactionManager<TDbContext> oneWorldDbTransactionManager,
            Func<DbConnection, TDbContext> dbContextFactory,
            ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            return new OneWorldDbTransaction<TDbContext>(
                transactionId,
                transactionNumber,
                dbConnection,
                isolationLevel,
                oneWorldDbTransactionManager,
                dbContextFactory,
                logger);
        }


        public async Task<OneWorldDbTransactionScope<TDbContext>> CreateTransactionScopeAsync()
        {
            if (_dbConnection.State != ConnectionState.Open)
                await _dbConnection.OpenAsync();

            if (_dbTransaction == null)
                _dbTransaction = _dbConnection.BeginTransaction(IsolationLevel);

            if (_dbContext == null && _dbContextFactory != null)
            {
                _dbContext = _dbContextFactory.Invoke(_dbConnection);
                _dbContext.Database.UseTransaction(_dbTransaction);
            }

            var ts = OneWorldDbTransactionScope<TDbContext>.Create(
                _dbConnection,
                _dbTransaction,
                _dbContext,
                this);

            ++_activeChildren;
            ++_totalChildren;

            _answers.TryAdd(ts, null);

            return ts;
        }


        public OneWorldDbTransactionVotingResult CommitPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, true, null))
                return OneWorldDbTransactionVotingResult.AlreadyVoted;

            ++_beCommits;
            return OneWorldDbTransactionVotingResult.VoteComplete;
        }


        public OneWorldDbTransactionVotingResult RollbackPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, false, null))
                return OneWorldDbTransactionVotingResult.AlreadyVoted;

            ++_beRollbacks;
            return OneWorldDbTransactionVotingResult.VoteComplete;
        }


        public int RollbacksAlreadyDecided()
        {
            return _beRollbacks;
        }


        public void Leave()
        {
            if (--_activeChildren == 0 && TransactionNumber != 0)
                _oneWorldDbTransactionManager.SubTransactionFinalize(TransactionId);
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
                _logger?.LogInformation($" totalChildren:{_totalChildren}/commits:{_beCommits}/rollbacks:{_beRollbacks}.");

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

                _logger?.LogInformation($" isNull _dbContext={_dbContext == null}");
                _dbContext?.Dispose();

                _logger?.LogInformation($" isNull _dbTransaction={_dbTransaction == null}");
                _dbTransaction?.Dispose();

                _logger?.LogInformation($" isNull _dbConnection={_dbConnection == null}");
                _dbConnection?.Close();
                _dbConnection?.Dispose();

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransaction<TDbContext>)} ended.");
            }
        }


        ~OneWorldDbTransaction()
        {
            Dispose(false);
        }
    }
}
