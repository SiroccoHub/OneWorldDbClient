using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace OneWorldDbClient
{
    public sealed class OneWorldDbTransactionScope<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _logger;

        private readonly OneWorldDbTransaction<TDbContext> _oneWorldDbTransaction;

        public readonly IDbConnection DbConnection;
        public readonly DbContext DbContext;
        public readonly IDbTransaction DbTransaction;

        private bool _voted;


        private OneWorldDbTransactionScope(
            IDbConnection dbConnection,
            IDbTransaction dbTransaction,
            DbContext dbContext,
            OneWorldDbTransaction<TDbContext> oneWorldDbTransaction,
            ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            DbConnection = dbConnection;
            DbTransaction = dbTransaction;
            DbContext = dbContext;
            _oneWorldDbTransaction = oneWorldDbTransaction;

            _logger = logger;
        }

        public bool Committable => _oneWorldDbTransaction.RollbacksAlreadyDecided() == 0;


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        internal static OneWorldDbTransactionScope<TDbContext> Create(
                IDbConnection dbConnection,
                IDbTransaction dbTransaction,
                DbContext dbContext,
                OneWorldDbTransaction<TDbContext> oneWorldDbTransaction,
                ILogger<OneWorldDbTransaction<TDbContext>> logger)
        {
            return new OneWorldDbTransactionScope<TDbContext>(
                dbConnection,
                dbTransaction,
                dbContext,
                oneWorldDbTransaction,
                logger);
        }


        public void VoteCommit()
        {
            var v = _oneWorldDbTransaction.CommitPlease(this);

            if (v == VotingResult.AlreadyVoted)
                throw new InvalidOperationException("Already voted.");

            _voted = true;

            _logger.LogInformation($" VoteCommit()");
        }


        public void VoteRollback()
        {
            var v = _oneWorldDbTransaction.RollbackPlease(this);

            if (v == VotingResult.AlreadyVoted)
                throw new InvalidOperationException("Already voted.");

            _voted = true;

            _logger.LogWarning($" VoteRollback()");
        }


        private void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            if (_voted == false)
            {
                _oneWorldDbTransaction.RollbackPlease(this);

                _logger.LogCritical($" Not voting");
                throw new InvalidOperationException("Not voting.");
            }

            _oneWorldDbTransaction.Leave();
        }


        ~OneWorldDbTransactionScope()
        {
            Dispose(false);
        }
    }
}