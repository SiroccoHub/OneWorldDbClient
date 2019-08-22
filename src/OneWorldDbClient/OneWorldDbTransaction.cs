using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public class OneWorldDbTransaction<T> : IDisposable
    {
        public readonly Guid TransactionId;

        public int TransactionNumber { get; private set; }

        private readonly ILogger<OneWorldDbTransaction<T>> _logger;

        private readonly DbConnection _dbConnection;
        private DbTransaction _dbTransaction;

        private readonly DbContext _dbContext;

        private readonly OneWorldDbTransactionManager<T> _oneWorldDbTransactionManager;

        public IsolationLevel IsolationLevel { get; private set; }

        private int _activeChildren = 0;
        private int _totalChildren = 0;

        private int _beCommits = 0;
        private int _beRollbacks = 0;

        private readonly ConcurrentDictionary<OneWorldDbTransactionScope<T>, bool?> _answers
            = new ConcurrentDictionary<OneWorldDbTransactionScope<T>, bool?>();


        private OneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            DbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbTransactionManager<T> oneWorldDbTransactionManager,
            ILogger<OneWorldDbTransaction<T>> logger)
        {
            TransactionId = transactionId;
            TransactionNumber = transactionNumber;
            _dbConnection = dbConnection;
            IsolationLevel = isolationLevel;
            _oneWorldDbTransactionManager = oneWorldDbTransactionManager;
            _logger = logger;
        }


        public static OneWorldDbTransaction<T> CreateOneWorldDbTransaction(
            Guid transactionId,
            int transactionNumber,
            DbConnection dbConnection,
            IsolationLevel isolationLevel,
            OneWorldDbTransactionManager<T> oneWorldDbTransactionManager,
            ILogger<OneWorldDbTransaction<T>> logger)
        {
            return new OneWorldDbTransaction<T>(
                transactionId,
                transactionNumber,
                dbConnection,
                isolationLevel,
                oneWorldDbTransactionManager,
                logger);
        }


        public async Task<OneWorldDbTransactionScope<T>> CreateTransactionScopeAsync()
        {
            if (_dbConnection.State != ConnectionState.Open)
                await _dbConnection.OpenAsync();

            if (_dbTransaction == null)
                _dbTransaction = _dbConnection.BeginTransaction(IsolationLevel);

            if (_dbContext == null)
            {
                //_dbContext = new YourSomeDbContext(
                //    new DbContextOptionsBuilder<YourSomeDbContext>()
                //        .UseSqlServer(_dbConnection)
                //        .Options);

                _dbContext.Database.UseTransaction(_dbTransaction);
            }

            var ts = OneWorldDbTransactionScope<T>.Create(
                _dbConnection,
                _dbTransaction,
                _dbContext,
                this);

            ++_activeChildren;
            ++_totalChildren;

            _answers.TryAdd(ts, null);

            return ts;
        }


        public OneWorldDbTransactionVotingResult CommitPlease(OneWorldDbTransactionScope<T> ts)
        {
            if (!_answers.TryUpdate(ts, true, null))
                return OneWorldDbTransactionVotingResult.AlreadyVoted;

            ++_beCommits;
            return OneWorldDbTransactionVotingResult.VoteComplete;
        }


        public OneWorldDbTransactionVotingResult RollbackPlease(OneWorldDbTransactionScope<T> ts)
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
                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransaction<T>)} start.");
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

                _logger?.LogInformation($"Dispose {nameof(OneWorldDbTransaction<T>)} ended.");
            }
        }


        ~OneWorldDbTransaction()
        {
            Dispose(false);
        }
    }
}
