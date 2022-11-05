using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace N2UnitTestBase
{
    internal static class N2SqlLocalDbTestBase
    {
        public static readonly object Lock = new object();
        public static bool Initialized;
    }

    public class N2SqlLocalDbTestBase<TDbContext> where TDbContext : DbContext
    {
        public IConfigurationRoot? ConfigurationRoot { get; private set; }

        public void Setup(int dbNumber = 0)
        {
            if (!N2SqlLocalDbTestBase.Initialized)
            {
                lock (N2SqlLocalDbTestBase.Lock)
                {
                    if (!N2SqlLocalDbTestBase.Initialized)
                    {
                        var app = new ProcessStartInfo
                        {
                            FileName = $".\\Scripts\\InitN2SqlLocalDB{dbNumber}.cmd",
                            CreateNoWindow = true,
                            UseShellExecute = false
                        };
                        var p = Process.Start(app);
                        p.WaitForExit(10000);
                        N2SqlLocalDbTestBase.Initialized = true;
                    }
                }
            }

            DbNumber = dbNumber;

            var builder = new ConfigurationBuilder();
            builder.AddEnvironmentVariables();
            ConfigurationRoot = builder.Build();
        }

        public void TearDown()
        {
            // if you want to do some task at closing.
        }


        public int DbNumber { get; set; } = 0;


        private string InstanceName => $"N2SqlLocalDB{DbNumber}";

        private string DatabaseNamePlaceHolder { get; } = "@DatabaseName@";

        public string SqlLocalDbConnectionStringTemplate => 
            $"Data Source=(LocalDB)\\{InstanceName};Database=@DatabaseName@;Connect Timeout=60;";

        public string CreateConnectionString(string? additionalId = null, string? customDatabaseName = null)
        {
            return SqlLocalDbConnectionStringTemplate
                .Replace(DatabaseNamePlaceHolder,
                    $"{customDatabaseName ?? InstanceName}.{additionalId ?? GetMd5Hash(GetType().Name)}");
        }

        private static string GetMd5Hash(string input)
        {
            using var md5 = MD5.Create();

            var data = md5.ComputeHash(Encoding.UTF8.GetBytes(input));

            var sBuilder = new StringBuilder();

            foreach (var t in data)
                sBuilder.Append(t.ToString("x2"));

            return sBuilder.ToString();
        }
    }
}
