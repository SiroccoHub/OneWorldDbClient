using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.Data;

namespace OneWorldDbClient.SampleWeb.SampleDi
{
    public class SampleDiLogicB
    {
        private readonly OneWorldDbClientManager<ApplicationDbContext> _dbManager;
        private readonly ILogger<SampleDiLogicB> _logger;

        public SampleDiLogicB(
            OneWorldDbClientManager<ApplicationDbContext> dbManager,
            ILogger<SampleDiLogicB> logger)
        {
            _dbManager = dbManager;
            _logger = logger;
        }

        public async Task<int> SampleMethodAsync(int baseNum)
        {
            int i;

            const string identifier = @"X000B";

            using (var txScope = await _dbManager.BeginTranRequiredAsync())
            {
                i = await txScope.DbConnection.ExecuteAsync(
                    "INSERT INTO [SampleTable01] VALUES (@SampleColumn01)",
                    new { SampleColumn01 = identifier },
                    txScope.DbTransaction);
                // i == 1

                i += txScope.DbContext.Set<SampleTable01>()
                    .AsNoTracking()
                    .Count(e => e.SampleColumn01 == identifier);
                // i == 2

                txScope.VoteCommit();
            }

            _logger.LogInformation($"{i}");
            return baseNum + i;
        }
    }
}
