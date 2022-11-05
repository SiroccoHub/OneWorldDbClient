using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using IsolationLevel = System.Data.IsolationLevel;


namespace OneWorldDbClient
{
    public sealed class OneWorldDbTransaction<TDbContext> : IDisposable where TDbContext : DbContext
    {
        public readonly Guid TransactionId;

        public int TransactionNumber { get; }

        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _logger;

        private readonly OneWorldDbClientManager<TDbContext> _oneWorldDbClientManager;
        private readonly Func<IDbConnection, IDbTransaction, TDbContext>? _dbContextFactory;

        private readonly IDbConnection _dbConnection;
        private IDbTransaction? _dbTransaction;
        private TDbContext? _dbContext;

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


        internal static OneWorldDbTransaction<TDbContext> CreateOneWorldDbTransaction(
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


        internal async Task<OneWorldDbTransactionScope<TDbContext>> CreateTransactionScopeAsync()
        {
            if (_dbConnection.State != ConnectionState.Open)
                await ((DbConnection)_dbConnection).OpenAsync();

            _dbTransaction ??= await ((DbConnection) _dbConnection).BeginTransactionAsync(IsolationLevel);

            if (_dbContext == null && _dbContextFactory != null)
                _dbContext = _dbContextFactory.Invoke(_dbConnection, _dbTransaction);

            if (_dbContext == null)
                throw new NoNullAllowedException();

            var ts = 
                OneWorldDbTransactionScope<TDbContext>.Create(
                    dbConnection: _dbConnection, 
                    dbTransaction: _dbTransaction, 
                    dbContext: _dbContext, 
                    oneWorldDbTransaction: this,
                    logger: _logger);

            ++_activeChildren;
            ++_totalChildren;

            _answers.TryAdd(ts, null);

            return ts;
        }


        internal VotingResult CommitPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, true, null))
                return VotingResult.AlreadyVoted;

            ++_beCommits;
            return VotingResult.VoteComplete;
        }


        internal VotingResult RollbackPlease(OneWorldDbTransactionScope<TDbContext> ts)
        {
            if (!_answers.TryUpdate(ts, false, null))
                return VotingResult.AlreadyVoted;

            ++_beRollbacks;
            return VotingResult.VoteComplete;
        }


        internal int RollbacksAlreadyDecided()
        {
            return _beRollbacks;
        }


        internal void Leave()
        {
            if (--_activeChildren == 0 && TransactionNumber > -1)
            {
                _logger.LogTrace(" _activeChildren:`{_activeChildren}`/TransactionNumber:`{TransactionNumber}`", _activeChildren,TransactionNumber);
                _logger.LogTrace(" do TransactionFinalize(`{TransactionId}`)", TransactionId);

                _oneWorldDbClientManager.TransactionFinalize(TransactionId);
            }
            else
            {
                _logger.LogTrace(" _activeChildren:`{_activeChildren}`/TransactionNumber:`{TransactionNumber}`", _activeChildren, TransactionNumber);
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        private void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            _logger.LogTrace($" Dispose {nameof(OneWorldDbTransaction<TDbContext>)} start.");
            _logger.LogDebug(" totalChildren:`{_totalChildren}`/commits:`{_beCommits}`/rollbacks:`{_beRollbacks}`/TransactionId:`{TransactionId}`/TransactionNumber:`{TransactionNumber}`",_totalChildren, _beCommits, _beRollbacks, TransactionId, TransactionNumber);
           

            if (_totalChildren == _beCommits)
            {
                _dbTransaction?.Commit();
                _logger.LogInformation(" done Commit()./TransactionId:`{TransactionId}`/TransactionNumber:`{TransactionNumber}`", TransactionId, TransactionNumber);
            }
            else
            {
                _dbTransaction?.Rollback();
                _logger.LogWarning(" done Rollback()./TransactionId:`{TransactionId}`/TransactionNumber:`{TransactionNumber}`", TransactionId, TransactionNumber);
            }

            _logger.LogTrace(" isNull _dbContext=`{_dbContext}`", _dbContext == null );
            _logger.LogTrace(" isNull _dbTransaction=`{_dbTransaction}`", _dbTransaction == null);

            _dbContext?.Dispose();
            _dbTransaction?.Dispose();
            _dbConnection.Close();
            _dbConnection.Dispose();

            _logger.LogDebug($" Dispose {nameof(OneWorldDbTransaction<TDbContext>)} done.");
        }


        ~OneWorldDbTransaction()
        {
            Dispose(false);
        }
    }
}
