using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace N2UnitTestBase
{
    internal static class N2SqlLocalDbTestBase
    {
        public static readonly object Lock = new object();
        public static bool Initialized;
    }

    public class N2SqlLocalDbTestBase<TDbContext> where TDbContext : DbContext
    {
        public IConfigurationRoot ConfigurationRoot { get; private set; }

        public void Setup()
        {
            if (!N2SqlLocalDbTestBase.Initialized)
            {
                lock (N2SqlLocalDbTestBase.Lock)
                {
                    if (!N2SqlLocalDbTestBase.Initialized)
                    {
                        var app = new ProcessStartInfo
                        {
                            FileName = ".\\Scripts\\InitN2SqlLocalDB.cmd",
                            CreateNoWindow = true,
                            UseShellExecute = false
                        };
                        var p = Process.Start(app);
                        p.WaitForExit(10000);
                        N2SqlLocalDbTestBase.Initialized = true;
                    }
                }
            }

            var builder = new ConfigurationBuilder();
            builder.AddEnvironmentVariables();
            ConfigurationRoot = builder.Build();
        }

        public void TearDown()
        {
            // if you want to do some task at closing.
        }


        /// <summary>
        /// DB 元ネタ初期化
        /// </summary>
        /// <param name="context"></param>
        /// <param name="pathSeedDataQuery"></param>
        /// <returns></returns>
        public async Task<int> SeedingByQueryAsync(N2SqlLocalDbContext<TDbContext> context, string pathSeedDataQuery)
        {
            string q;

            if (!string.IsNullOrEmpty(pathSeedDataQuery))
                q = File.ReadAllText(pathSeedDataQuery);
            else
                throw new ArgumentNullException(nameof(pathSeedDataQuery));

            return await context.RawContext.ExecuteNonQueryAsync(q);
        }



        private string InstanceName { get; } = "N2SqlLocalDB";

        private string DatabaseNamePlaceHolder { get; } = "@DatabaseName@";

        public string SqlLocalDbConnectionStringTemplate { get; set; } =
            $"Data Source=(LocalDB)\\N2SqlLocalDB;Database=@DatabaseName@;Connect Timeout=60;";

        public string CreateConnectionString(string additionalId = null, string customDatabaseName = null)
        {
            return SqlLocalDbConnectionStringTemplate
                .Replace(DatabaseNamePlaceHolder,
                    $"{customDatabaseName ?? InstanceName}.{additionalId ?? GetMd5Hash(GetType().Name)}");
        }

        private static string GetMd5Hash(string input)
        {
            using (var md5 = MD5.Create())
            {
                var data = md5.ComputeHash(Encoding.UTF8.GetBytes(input));

                var sBuilder = new StringBuilder();

                foreach (var t in data)
                    sBuilder.Append(t.ToString("x2"));

                return sBuilder.ToString();
            }
        }
    }
}
