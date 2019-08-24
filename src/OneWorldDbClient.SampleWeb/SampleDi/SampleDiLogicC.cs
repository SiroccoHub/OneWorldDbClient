using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.Data;

namespace OneWorldDbClient.SampleWeb.SampleDi
{
    public class SampleDiLogicC
    {
        private readonly OneWorldDbClientManager<ApplicationDbContext> _dbManager;
        private readonly ILogger<SampleDiLogicC> _logger;

        public SampleDiLogicC(
            OneWorldDbClientManager<ApplicationDbContext> dbManager,
            ILogger<SampleDiLogicC> logger)
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

            using (var txScope = await _dbManager.BeginTranRequiresNewAsync())
            {
                i = (await txScope.DbConnection.QueryAsync<int>(
                        "SELECT COUNT(*) FROM [SampleTable01] WHERE [SampleColumn01] in @Values",
                        new { Values = identifierAll },
                        txScope.DbTransaction))
                    .FirstOrDefault();
                // i == 0

                i += txScope.DbContext.Set<SampleTable01>()
                    .AsNoTracking()
                    .Count(e => identifierAll.Contains(e.SampleColumn01));
                // i == 0

                txScope.VoteCommit();
            }

            _logger.LogInformation($"{i}");
            return i;
        }
    }
}
