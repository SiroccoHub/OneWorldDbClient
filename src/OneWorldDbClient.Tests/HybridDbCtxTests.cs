using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using N2UnitTestBase;
using NUnit.Framework;
using OneWorldDbClient.Tests.Model.AliceDb;


namespace OneWorldDbClient.Tests
{
    [Parallelizable]
    public class HybridDbCtxTests : N2SqlLocalDbTestBase<AliceDbContext>
    {
        private readonly ILogger _logger = new N2Logger<HybridDbCtxTests>();

        [SetUp]
        public new void Setup()
        {
            base.Setup();
        }

        [TearDown]
        public new void TearDown()
        {
            base.TearDown();
        }


        private OneWorldDbClientManager<AliceDbContext> CreateManager()
        {
            return new OneWorldDbClientManager<AliceDbContext>(
                CreateConnectionString(),
                (conn, tx) =>
                {
                    var dbContext = new AliceDbContext(
                        new DbContextOptionsBuilder<AliceDbContext>()
                            .UseSqlServer((DbConnection)conn)
                            .Options);
                    dbContext.Database.UseTransaction((SqlTransaction)tx);
                    return dbContext;
                },
                connStr => new SqlConnection(connStr),
                new N2Logger<OneWorldDbClientManager<AliceDbContext>>(),
                new N2Logger<OneWorldDbTransaction<AliceDbContext>>());
        }


        [Test]
        public async Task DapperSingleLayerMultiTxScope()
        {
            using (var n2DbContext = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                    _ => new AliceDbContext((DbContextOptions<AliceDbContext>)_)))
            {

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();

                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());



                // ========================================
                // BeginTranRequiredAsync コミット = Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }
                }

                // 1件 Commit されてる
                Assert.AreEqual(
                    1,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiresNewAsync コミット = Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }
                }

                // 1件 Commit されてる
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット  + BeginTranRequiredAsync コミット  = Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx.DbTransaction);

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // 全て Commit されてる
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット  + BeginTranRequiredAsync コミット  = Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbContext.Set<Table00>().Add(new Table00 { Column00 = @"Z9999" });
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        // 見えるべき
                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z____" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }
                }

                // 全て Commit されてる
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync ReadCommitted コミット + BeginTranRequiresNewAsync ReadUncommitted コミット = Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadUncommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z____" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }
                }

                // 全て Commit されてる
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット + BeginTranRequiredAsync ロールバック = Rollback
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync())
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync())
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z____" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteRollback();
                    }
                }

                // Rollback されてる
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>().SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>().SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット + BeginTranRequiresNewAsync ロールバック = 前者 Commit /後者 Rollback
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        // 見えないべき
                        Assert.AreEqual(0, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z____" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteRollback();
                    }
                }

                // 前者 Commit /後者 Rollback
                Assert.AreEqual(1, n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>().SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();
            }

            Assert.Pass();
        }




        [Test]
        public async Task DapperMultiLayersMultiTxScope()
        {
            using (var ciDbCtx = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                    _ => new AliceDbContext((DbContextOptions<AliceDbContext>)_)))
            {
                // reset
                ciDbCtx.RawContext.Set<Table00>().RemoveRange(ciDbCtx.RawContext.Table00);
                await ciDbCtx.RawContext.SaveChangesAsync();

                Assert.AreEqual(
                    0,
                    ciDbCtx.RawContext.Set<Table00>().Count());



                // ========================================
                // BeginTranRequiredAsync コミット + (BeginTranRequiresNewAsync ロールバック > BeginTranRequiredAsync コミット ) + BeginTranRequiredAsync コミット = Commit / Rollback / Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z9999" },
                            tx.DbTransaction);

                        Assert.AreEqual(1,
                            (await tx.DbConnection.QueryAsync<int>(
                                "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                new { Value = "Z9999" }, tx.DbTransaction)).FirstOrDefault());

                        tx.VoteCommit();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        // BeginTranRequiresNewAsync だけど ReadUncommitted なので見える
                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx0.DbConnection.Execute(
                            @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                            new { Value = "Z____" },
                            tx0.DbTransaction);

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            tx1.DbConnection.Execute(
                                @"INSERT INTO [Table00] ( [Column00] ) VALUES ( @Value )",
                                new { Value = "Z@@@@" },
                                tx1.DbTransaction);

                            Assert.AreEqual(1,
                                (await tx1.DbConnection.QueryAsync<int>(
                                    "SELECT COUNT(*) FROM [Table00] WHERE [Column00] = @Value ",
                                    new { Value = "Z@@@@" }, tx1.DbTransaction)).FirstOrDefault());

                            tx1.VoteCommit();
                        }

                        tx0.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 { Column00 = @"Z!!!!" });
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                        tx.VoteCommit();
                    }
                }

                // Commit / Rollback / Commit
                Assert.AreEqual(1, ciDbCtx.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(ciDbCtx.RawContext.Set<Table00>().SingleOrDefault(e => e.Column00 == @"Z____"));
                Assert.IsNull(ciDbCtx.RawContext.Set<Table00>().SingleOrDefault(e => e.Column00 == @"Z@@@@"));
                Assert.AreEqual(1, ciDbCtx.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                // reset
                ciDbCtx.RawContext.Set<Table00>().RemoveRange(ciDbCtx.RawContext.Table00);
                await ciDbCtx.RawContext.SaveChangesAsync();
            }

            Assert.Pass();
        }
    }
}
