using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Data;

namespace OneWorldDbClient
{
    public class OneWorldDbTransactionScope<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _logger;

        public readonly IDbConnection DbConnection;
        public readonly IDbTransaction DbTransaction;
        public readonly DbContext DbContext;

        private readonly OneWorldDbTransaction<TDbContext> _oneWorldDbTransaction;

        private bool _voted;

        private readonly string _memberName;
        private readonly string _sourceFilePath;
        private readonly int _sourceLineNumber;


        private OneWorldDbTransactionScope(
            IDbConnection dbConnection,
            IDbTransaction dbTransaction,
            DbContext dbContext,
            OneWorldDbTransaction<TDbContext> oneWorldDbTransaction,
            ILogger<OneWorldDbTransaction<TDbContext>> logger,
            string memberName = "",
            string sourceFilePath = "",
            int sourceLineNumber = 0)
        {
            DbConnection = dbConnection;
            DbTransaction = dbTransaction;
            DbContext = dbContext;
            _oneWorldDbTransaction = oneWorldDbTransaction;

            _logger = logger;

            _memberName = memberName;
            _sourceFilePath = sourceFilePath;
            _sourceLineNumber = sourceLineNumber;
        }


        public static OneWorldDbTransactionScope<TDbContext> Create(
            IDbConnection dbConnection,
            IDbTransaction dbTransaction,
            DbContext dbContext,
            OneWorldDbTransaction<TDbContext> oneWorldDbTransaction,
            ILogger<OneWorldDbTransaction<TDbContext>> logger,
            string memberName = "",
            string sourceFilePath = "",
            int sourceLineNumber = 0)
        {
            return new OneWorldDbTransactionScope<TDbContext>(
                dbConnection,
                dbTransaction,
                dbContext,
                oneWorldDbTransaction,
                logger,
                memberName,
                sourceFilePath,
                sourceLineNumber);
        }

        public bool Committable => _oneWorldDbTransaction.RollbacksAlreadyDecided() == 0;


        public void VoteCommit()
        {
            var v = _oneWorldDbTransaction.CommitPlease(this);

            if (v == VotingResult.AlreadyVoted)
                throw new InvalidOperationException($"already voted.");

            _voted = true;

            _logger.LogInformation($"VoteCommit(),{_memberName},{_sourceFilePath},{_sourceLineNumber}");
        }


        public void VoteRollback()
        {
            var v = _oneWorldDbTransaction.RollbackPlease(this);

            if (v == VotingResult.AlreadyVoted)
                throw new InvalidOperationException($"already voted.");

            _voted = true;

            _logger.LogWarning($"VoteCommit(),{_memberName},{_sourceFilePath},{_sourceLineNumber}");
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
                {
                    _oneWorldDbTransaction.RollbackPlease(this);

                    _logger.LogError($"Not voting,{_memberName},{_sourceFilePath},{_sourceLineNumber}");
                    throw new InvalidOperationException($"Not voting.")
                    {
                        Source = _memberName,
                        Data =
                        {
                            { "memberName",_memberName},
                            { "sourceFilePath",_sourceFilePath},
                            { "sourceLineNumber",_sourceLineNumber},
                        }
                    };
                }

                _oneWorldDbTransaction.Leave();
            }
        }


        ~OneWorldDbTransactionScope()
        {
            Dispose(false);
        }
    }
}
