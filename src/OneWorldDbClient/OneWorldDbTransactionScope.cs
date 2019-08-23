using Microsoft.EntityFrameworkCore;
using System;
using System.Data;

namespace OneWorldDbClient
{
    public class OneWorldDbTransactionScope<TDbContext> : IDisposable where TDbContext : DbContext
    {
        public readonly IDbConnection DbConnection;
        public readonly IDbTransaction DbTransaction;
        public readonly DbContext DbContext;

        private readonly OneWorldDbTransaction<TDbContext> _oneWorldDbTransaction;

        private bool _voted;


        private OneWorldDbTransactionScope(
            IDbConnection dbConnection,
            IDbTransaction dbTransaction,
            DbContext dbContext,
            OneWorldDbTransaction<TDbContext> oneWorldDbTransaction)
        {
            DbConnection = dbConnection;
            DbTransaction = dbTransaction;
            _oneWorldDbTransaction = oneWorldDbTransaction;
            DbContext = dbContext;
        }


        public static OneWorldDbTransactionScope<TDbContext> Create(
            IDbConnection dbConnection,
            IDbTransaction dbTransaction,
            DbContext dbContext,
            OneWorldDbTransaction<TDbContext> oneWorldDbTransaction)
        {
            return new OneWorldDbTransactionScope<TDbContext>(
                dbConnection,
                dbTransaction,
                dbContext,
                oneWorldDbTransaction);
        }

        public bool Commitable => _oneWorldDbTransaction.RollbacksAlreadyDecided() == 0;


        public void VoteCommit()
        {
            var v = _oneWorldDbTransaction.CommitPlease(this);

            if (v == OneWorldDbTransactionVotingResult.AlreadyVoted)
                throw new InvalidOperationException($"already voted.");

            _voted = true;
        }


        public void VoteRollback()
        {
            var v = _oneWorldDbTransaction.RollbackPlease(this);

            if (v == OneWorldDbTransactionVotingResult.AlreadyVoted)
                throw new InvalidOperationException($"already voted.");

            _voted = true;
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
                if (_voted == false)
                    _oneWorldDbTransaction.RollbackPlease(this);

                _oneWorldDbTransaction.Leave();
            }
        }


        ~OneWorldDbTransactionScope()
        {
            Dispose(false);
        }
    }
}
