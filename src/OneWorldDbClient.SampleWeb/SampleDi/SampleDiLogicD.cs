using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.Data;

namespace OneWorldDbClient.SampleWeb.SampleDi
{
    public class SampleDiLogicD
    {
        private readonly OneWorldDbClientManager<ApplicationDbContext> _dbManager;
        private readonly ILogger<SampleDiLogicD> _logger;

        public SampleDiLogicD(
            OneWorldDbClientManager<ApplicationDbContext> dbManager,
            ILogger<SampleDiLogicD> logger)
        {
            _dbManager = dbManager;
            _logger = logger;
        }

        public async Task<int> SampleMethodAsync()
        {
            int i;

            var identifierAll = new List<string>
            {
                "X000A", "X000B"
            };

            using (var txScope = await _dbManager.BeginTranRequiredAsync())
            {
                _logger.LogInformation($"txScope.Committable={txScope.Committable}");

                // if you want to short-circuit evaluation.
                if (!txScope.Committable)
                {
                    txScope.VoteRollback();
                    return -1;
                }

                i = (await txScope.DbConnection.QueryAsync<int>(
                        "SELECT COUNT(*) FROM [SampleTable01] WHERE [SampleColumn01] in @Values",
                        new { Values = identifierAll },
                        txScope.DbTransaction))
                    .FirstOrDefault();
                // i == 2

                i += txScope.DbContext.Set<SampleTable01>()
                    .AsNoTracking()
                    .Count(e => identifierAll.Contains(e.SampleColumn01));
                // i == 4

                // txScope.VoteCommit();
                // An explicit declaration is required.
                // Not specifying is the same as `.VoteRollback()`.
            }

            _logger.LogInformation($"{i}");
            return i;
        }
    }
}
