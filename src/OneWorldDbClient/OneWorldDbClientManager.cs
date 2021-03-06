﻿using System;
using System.Collections.Concurrent;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace OneWorldDbClient
{
    public sealed class OneWorldDbClientManager<TDbContext> : IDisposable where TDbContext : DbContext
    {
        private readonly string _connectionString;

        private readonly Func<string, IDbConnection> _dbConnectionFactory;
        private readonly Func<IDbConnection, IDbTransaction, TDbContext> _dbContextFactory;
        private readonly ILogger<OneWorldDbClientManager<TDbContext>> _logger;
        private readonly ILogger<OneWorldDbTransaction<TDbContext>> _loggerTx;

        private readonly ConcurrentStack<Guid> _publishedTransactionStack
            = new ConcurrentStack<Guid>();

        private readonly ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>> _transactions
            = new ConcurrentDictionary<Guid, OneWorldDbTransaction<TDbContext>>();


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


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// トランザクションが存在すれば参加し、なければ新規に作成します。
        /// </summary>
        /// <param name="isolationLevel">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <param name="memberName"></param>
        /// <param name="sourceFilePath"></param>
        /// <param name="sourceLineNumber"></param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiredAsync(
            IsolationLevel? isolationLevel = null,
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string sourceFilePath = "",
            [CallerLineNumber] int sourceLineNumber = 0)
        {
            // current tx not found. create new tx.
            if (!_publishedTransactionStack.TryPeek(out var latestTransactionId))
                return await BeginTranRequiresNewInternalAsync(
                    isSuperRootTx: true,
                    isolationLevel: isolationLevel,
                    memberName: memberName,
                    sourceFilePath: sourceFilePath,
                    sourceLineNumber: sourceLineNumber);

            // current tx found.
            if (!_transactions.TryGetValue(latestTransactionId, out var transaction))
                throw new InvalidProgramException($"lost transaction {latestTransactionId}");

            _logger.LogInformation($" Required:TransactionNumber=`{transaction.TransactionNumber}`/TransactionId=`{transaction.TransactionId}`/IsolationLevel=`{transaction.IsolationLevel}`");

            if (!isolationLevel.HasValue || isolationLevel == transaction.IsolationLevel)
                return await transaction.CreateTransactionScopeAsync(memberName, sourceFilePath, sourceLineNumber);

            throw new InvalidOperationException(
                $"{nameof(isolationLevel)} で指定されたトランザクション {isolationLevel} は、既に存在するトランザクションレベル {transaction.IsolationLevel} と相違しています。");
        }


        /// <summary>
        /// トランザクションを新規に開始します。
        /// </summary>
        /// <param name="isolationLevel">null の場合、最初のトランザクションであれば IsolationLevel.ReadCommitted に、参加する場合は上位に依存します。</param>
        /// <param name="memberName"></param>
        /// <param name="sourceFilePath"></param>
        /// <param name="sourceLineNumber"></param>
        /// <returns></returns>
        public async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiresNewAsync(
            IsolationLevel? isolationLevel = null,
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string sourceFilePath = "",
            [CallerLineNumber] int sourceLineNumber = 0)
        {
            return await BeginTranRequiresNewInternalAsync(
                isSuperRootTx: false,
                isolationLevel: isolationLevel,
                memberName: memberName,
                sourceFilePath: sourceFilePath,
                sourceLineNumber: sourceLineNumber);
        }


        /// <summary>
        /// トランザクションを新規に開始する内部メソッド
        /// </summary>
        /// <param name="isSuperRootTx"></param>
        /// <param name="isolationLevel"></param>
        /// <param name="memberName"></param>
        /// <param name="sourceFilePath"></param>
        /// <param name="sourceLineNumber"></param>
        /// <returns></returns>
        private async Task<OneWorldDbTransactionScope<TDbContext>> BeginTranRequiresNewInternalAsync(
            bool isSuperRootTx,
            IsolationLevel? isolationLevel = null,
            string memberName = "",
            string sourceFilePath = "",
            int sourceLineNumber = 0)
        {
            var transactionNumber = isSuperRootTx ? -1 : _transactions.Count;
            var firstTran = 
                OneWorldDbTransaction<TDbContext>.CreateOneWorldDbTransaction(
                    transactionId: Guid.NewGuid(),
                    transactionNumber: transactionNumber,
                    dbConnection: _dbConnectionFactory.Invoke(_connectionString),
                    isolationLevel: isolationLevel ?? IsolationLevel.ReadCommitted,
                    oneWorldDbClientManager: this,
                    dbContextFactory: _dbContextFactory,
                    logger: _loggerTx);

            _logger.LogInformation($" RequiresNew:isSuperRootTx=`{isSuperRootTx}`,TransactionNumber=`{firstTran.TransactionNumber}`/TransactionId=`{firstTran.TransactionId}`/IsolationLevel=`{firstTran.IsolationLevel}`");

            if (!_transactions.TryAdd(firstTran.TransactionId, firstTran))
                throw new InvalidProgramException($"duplicate transaction id {firstTran.TransactionId}");

            _publishedTransactionStack.Push(firstTran.TransactionId);

            return await firstTran.CreateTransactionScopeAsync(memberName, sourceFilePath, sourceLineNumber);
        }


        public void TransactionFinalize(Guid endedSubTransactionId)
        {
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
                                _logger?.LogTrace($" sub.tx={endedSubTransactionId} Disposing.");
                                transaction.Dispose();
                                _logger?.LogDebug($" sub.tx={endedSubTransactionId} Disposed and Removed.");
                            }
                            else
                            {
                                _logger?.LogDebug($" sub.tx={endedSubTransactionId} Already Removed.");
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                $"failed {nameof(_publishedTransactionStack)} 's TryPeek().");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"{nameof(latestTransactionId)} {latestTransactionId} and {endedSubTransactionId} are different.");
                    }
                }
                else
                {
                    throw new InvalidOperationException($"failed {nameof(_publishedTransactionStack)} 's TryPeek().");
                }
            }
        }


        private void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            _logger?.LogDebug($" Dispose {nameof(OneWorldDbClientManager<TDbContext>)} start.");

            while (_publishedTransactionStack.Count > 0)
            {
                _publishedTransactionStack.TryPop(out var latestTransactionId);

                if (_transactions.TryRemove(latestTransactionId, out var transaction))
                {
                    _logger?.LogDebug($" tx={latestTransactionId} Disposing.");
                    transaction.Dispose();
                    _logger?.LogDebug($" tx={latestTransactionId} Disposed and Removed.");
                }
                else
                {
                    _logger?.LogDebug($" tx={latestTransactionId} Already Removed.");
                }
            }

            _logger?.LogInformation($" Dispose {nameof(OneWorldDbClientManager<TDbContext>)} ended.");
        }


        ~OneWorldDbClientManager()
        {
            Dispose(false);
        }
    }
}