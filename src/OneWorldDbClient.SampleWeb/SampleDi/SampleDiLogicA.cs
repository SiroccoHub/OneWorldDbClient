using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.Data;

namespace OneWorldDbClient.SampleWeb.SampleDi
{
    public class SampleDiLogicA
    {
        private readonly OneWorldDbClientManager<ApplicationDbContext> _dbManager;
        private readonly ILogger<SampleDiLogicA> _logger;

        public SampleDiLogicA(
            OneWorldDbClientManager<ApplicationDbContext> dbManager,
            ILogger<SampleDiLogicA> logger)
        {
            _dbManager = dbManager;
            _logger = logger;
        }

        public async Task<int> SampleMethodAsync()
        {
            int i;

            using (var txScope = await _dbManager.BeginTranRequiredAsync())
            {
                const string identifier = @"X000A";

                // Dapper
                i = (await txScope.DbConnection.QueryAsync<int>(
                        "SELECT COUNT(*) FROM [SampleTable01] WHERE [SampleColumn01] = @Value ",
                        new { Value = identifier },
                        txScope.DbTransaction))
                    .FirstOrDefault();
                // i == 0

                // EF Core
                i += txScope.DbContext.Set<SampleTable01>()
                    .AsNoTracking()
                    .Count(e => e.SampleColumn01 == identifier);
                // i == 0


                // add By EF Core with SaveChangesAsync()
                await txScope.DbContext.Set<SampleTable01>().AddAsync(new SampleTable01
                {
                    SampleColumn01 = identifier
                });
                await txScope.DbContext.SaveChangesAsync();


                i += txScope.DbContext.Set<SampleTable01>()
                    .FromSqlInterpolated($"SELECT * FROM [SampleTable01] WHERE [SampleColumn01] = {identifier}")
                    .Count();
                // i == 1

                i += (await txScope.DbConnection.QueryAsync<int>(
                        "SELECT COUNT(*) FROM [SampleTable01] WHERE [SampleColumn01] = @Value ",
                        new { Value = identifier },
                        txScope.DbTransaction))
                    .FirstOrDefault();
                // i == 2

                txScope.VoteCommit();
            }

            _logger.LogInformation($"{i}");
            return i;
        }
    }
}
