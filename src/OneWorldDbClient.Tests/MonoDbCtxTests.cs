using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using N2UnitTestBase;
using NUnit.Framework;
using OneWorldDbClient.Tests.Model.AliceDb;


namespace OneWorldDbClient.Tests
{
    [Parallelizable]
    public class MonoDbCtxTests : N2SqlLocalDbTestBase<AliceDbContext>
    {
        private readonly ILogger _logger = new N2Logger<MonoDbCtxTests>();

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
                            .UseSqlServer((DbConnection) conn)
                            .Options);
                    dbContext.Database.UseTransaction((SqlTransaction) tx);
                    return dbContext;
                },
                connStr => new SqlConnection(connStr),
                new N2Logger<OneWorldDbClientManager<AliceDbContext>>(),
                new N2Logger<OneWorldDbTransaction<AliceDbContext>>());
        }


        [Test]
        public async Task TxScopeErr()
        {
            using (var n2DbContext = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                _ => new AliceDbContext((DbContextOptions<AliceDbContext>) _)))
            {

                // ========================================
                // コミット ReadCommitted + コミット ReadUncommitted => Error
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        tx.VoteCommit();
                    }

                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync(
                            IsolationLevel.ReadUncommitted))
                        {
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo(
                            "isolationLevel で指定されたトランザクション ReadUncommitted は、既に存在するトランザクションレベル ReadCommitted と相違しています。"));
                }


                // ========================================
                // Double コミット #0
                // ========================================
                using (var diInstance = CreateManager())
                {
                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync())
                        {
                            tx.VoteCommit();
                            tx.VoteCommit();
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Already voted."));
                }


                // ========================================
                // Double コミット #1
                // ========================================
                using (var diInstance = CreateManager())
                {
                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync())
                        {
                            tx.VoteRollback();
                            tx.VoteCommit();
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Already voted."));
                }


                // ========================================
                // Double コミット #2
                // ========================================
                using (var diInstance = CreateManager())
                {
                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync())
                        {
                            tx.VoteCommit();
                            tx.VoteRollback();
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Already voted."));
                }


                // ========================================
                // Double コミット #2
                // ========================================
                using (var diInstance = CreateManager())
                {
                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync())
                        {
                            tx.VoteRollback();
                            tx.VoteRollback();
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Already voted."));
                }
            }

            Assert.Pass();
        }



        [Test]
        public async Task SingleLayerMultiTxScope()
        {
            using (var n2DbContext = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                _ => new AliceDbContext((DbContextOptions<AliceDbContext>) _)))
            {

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();

                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());



                // ========================================
                // BeginTranRequiredAsync コミットして成功 
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

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
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

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
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // 全て Commit されてる
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

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
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>()
                            .Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // 全て Commit されてる
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット + BeginTranRequiredAsync ロールバック = Rollback
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteRollback();
                    }
                }

                // Rollback されてる
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));

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
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        // 見えないべき
                        Assert.AreEqual(0, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteRollback();
                    }
                }

                // 前者 Commit /後者 Rollback
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync ロールバック + BeginTranRequiresNewAsync コミット = 前者 Rollback /後者 Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiresNewAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        // 見えないべき
                        Assert.AreEqual(0, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // 前者 Commit /後者 Rollback
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット + 忘れロールバック => Exception
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }


                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync(
                            IsolationLevel.ReadCommitted))
                        {
                            Assert.IsTrue(tx.Committable);

                            tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                            await tx.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                            // tx.VoteCommit(); // VoteCommit 忘れ　==> VoteRollback() の表現
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Not voting."));
                }

                // Rollback されてる
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // ロールバック + コミット
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsFalse(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // 忘れロールバック + コミット
                // ========================================
                using (var diInstance = CreateManager())
                {
                    var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        using (var tx = await diInstance.BeginTranRequiredAsync(
                            IsolationLevel.ReadCommitted))
                        {
                            Assert.IsTrue(tx.Committable);

                            tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                            await tx.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            // tx.VoteCommit(); // VoteCommit 忘れ　==> VoteRollback() の表現
                        }
                    });
                    Assert.That(
                        ex.Message,
                        Is.EqualTo("Not voting."));


                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsFalse(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット + BeginTranRequiresNewAsync ロールバック + BeginTranRequiredAsync コミット = Commit / Rollback / Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx0.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                        tx.VoteCommit();
                    }
                }

                // Commit / Rollback / Commit
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z@@@@"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // BeginTranRequiredAsync コミット + BeginTranRequiresNewAsync ロールバック + BeginTranRequiresNewAsync コミット + BeginTranRequiredAsync ロールバック = Rollback / Rollback / Commit / Rollback
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                        tx0.VoteRollback();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z%%%%"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z%%%%"));

                        tx0.VoteCommit();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                        tx.VoteRollback();
                    }
                }

                // Rollback / Rollback / Commit / Rollback
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z%%%%"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"!!!!"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();


            }

            Assert.Pass();
        }



        [Test]
        public async Task MultiLayersMultiTxScope()
        {
            using (var n2DbContext = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                _ => new AliceDbContext((DbContextOptions<AliceDbContext>) _)))
            {
                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();

                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());



                // ========================================
                // BeginTranRequiredAsync コミット + (BeginTranRequiresNewAsync ロールバック > BeginTranRequiredAsync コミット ) + BeginTranRequiredAsync コミット = Commit / Rollback / Commit
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        // BeginTranRequiresNewAsync だけど ReadUncommitted なので見える
                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z@@@@"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z@@@@"));

                            tx1.VoteCommit();
                        }

                        tx0.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                        tx.VoteCommit();
                    }
                }

                // Commit / Rollback / Commit
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z@@@@"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット > コミット > ロールバック 失敗
                // ========================================
                using (var diInstance = CreateManager())
                {
                    // Layer 1
                    using (var tx0 = await diInstance.BeginTranRequiredAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));


                        // Layer 2
                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                            // Layer 3
                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                                Assert.IsTrue(tx2.Committable);

                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                                await tx2.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                tx2.VoteRollback();
                            }

                            tx1.VoteCommit();
                        }

                        tx0.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット > コミット > ロールバック 失敗
                // ========================================
                using (var diInstance = CreateManager())
                {
                    // Layer 1
                    using (var tx0 = await diInstance.BeginTranRequiredAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));


                        // Layer 2
                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                            // Layer 3
                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                                Assert.IsTrue(tx2.Committable);

                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                                await tx2.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                tx2.VoteRollback();
                            }

                            tx1.VoteCommit();
                        }

                        tx0.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット > コミット > ロールバック + コミット 失敗
                // ========================================
                using (var diInstance = CreateManager())
                {
                    // Layer 1
                    using (var tx0 = await diInstance.BeginTranRequiredAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));


                        // Layer 2
                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                            // Layer 3A
                            using (var tx2A = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2A.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                                Assert.IsTrue(tx2A.Committable);

                                tx2A.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                                await tx2A.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2A.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                tx2A.VoteRollback();
                            }

                            // Layer 3B
                            using (var tx2B = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));
                                Assert.AreEqual(1,
                                    tx2B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                Assert.IsFalse(tx2B.Committable); // tx2A.VoteRollback();

                                tx2B.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"ZZ$$$$"});
                                await tx2B.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2B.DbContext.Set<Table00>().Count(e => e.Column00 == @"ZZ$$$$"));

                                tx2B.VoteCommit();
                            }

                            tx1.VoteCommit();
                        }

                        tx0.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット > ロールバック > コミット 失敗
                // ========================================
                using (var diInstance = CreateManager())
                {
                    // Layer 1
                    using (var tx0 = await diInstance.BeginTranRequiredAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));


                        // Layer 2
                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                            // Layer 3
                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                                Assert.IsTrue(tx2.Committable);

                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                                await tx2.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                tx2.VoteCommit();
                            }

                            tx1.VoteRollback();
                        }

                        tx0.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // コミット > ( ロールバック > コミット ) + コミット 失敗
                // ========================================
                using (var diInstance = CreateManager())
                {
                    // Layer 1
                    using (var tx0 = await diInstance.BeginTranRequiredAsync(IsolationLevel.ReadUncommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));


                        // Layer 2A
                        using (var tx1A = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1A.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                            Assert.IsTrue(tx1A.Committable);
                            tx1A.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});

                            await tx1A.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1A.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                            // Layer 3
                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                // 見えてるべき
                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));

                                Assert.IsTrue(tx2.Committable);

                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                                await tx2.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                                tx2.VoteCommit();
                            }

                            tx1A.VoteRollback();
                        }

                        // Layer 2B
                        using (var tx1B = await diInstance.BeginTranRequiredAsync())
                        {
                            // 見えてるべき
                            Assert.AreEqual(1,
                                tx1B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                            Assert.AreEqual(1,
                                tx1B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));
                            Assert.AreEqual(1,
                                tx1B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                            Assert.IsFalse(tx1B.Committable); // tx1A.VoteRollback();

                            tx1B.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z$$$$"});
                            await tx1B.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1B.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z$$$$"));

                            tx1B.VoteCommit();
                        }

                        tx0.VoteCommit();
                    }
                }

                // Rollback されてる
                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // COMPLEX 
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx0.Committable);

                        // BeginTranRequiresNewAsync ReadCommitted 見えない
                        Assert.AreEqual(0, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z____"});
                        await tx0.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z____"));


                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            Assert.IsTrue(tx1.Committable);

                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z@@@@"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z@@@@"));

                            tx1.VoteCommit();
                        }

                        Assert.IsTrue(tx0.Committable);
                        tx0.VoteRollback();
                    }

                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z!!!!"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));


                        using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadUncommitted))
                        {
                            Assert.IsTrue(tx0.Committable);

                            // BeginTranRequiresNewAsync ReadUncommitted 見える
                            Assert.AreEqual(1,
                                tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));

                            tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z++++"});
                            await tx0.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z++++"));


                            using (var tx1 = await diInstance.BeginTranRequiredAsync())
                            {
                                Assert.IsTrue(tx1.Committable);

                                tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z----"});
                                await tx1.DbContext.SaveChangesAsync();

                                Assert.AreEqual(1,
                                    tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z----"));


                                using (var tx2 =
                                    await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadCommitted))
                                {
                                    Assert.IsTrue(tx2.Committable);

                                    // BeginTranRequiresNewAsync ReadUncommitted 見える
                                    Assert.AreEqual(1,
                                        tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                                    Assert.AreEqual(1,
                                        tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));


                                    tx2.DbContext.Set<Table00>()
                                        .Add(new Table00 {Column00 = @"Z[[[["});
                                    await tx2.DbContext.SaveChangesAsync();

                                    Assert.AreEqual(1,
                                        tx2.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z[[[["));


                                    using (var tx3 = await diInstance.BeginTranRequiredAsync())
                                    {
                                        Assert.IsTrue(tx3.Committable);

                                        tx3.DbContext.Set<Table00>().Add(new Table00
                                            {Column00 = @"Z]]]]"});
                                        await tx3.DbContext.SaveChangesAsync();

                                        Assert.AreEqual(1,
                                            tx3.DbContext.Set<Table00>()
                                                .Count(e => e.Column00 == @"Z]]]]"));

                                        tx3.VoteRollback();
                                    }

                                    Assert.IsFalse(tx2.Committable);
                                    tx2.VoteCommit();
                                }

                                Assert.IsTrue(tx1.Committable);
                                tx1.VoteCommit();
                            }

                            Assert.IsTrue(tx0.Committable);
                            tx0.VoteCommit();
                        }

                        Assert.IsTrue(tx.Committable);
                        tx.VoteCommit();
                    }
                }

                // Commit / Rollback / Commit
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z____"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z@@@@"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z!!!!"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z++++"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z----"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z[[[["));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z]]]]"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // COMPLEX
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx0 = await diInstance.BeginTranRequiredAsync())
                    {
                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0001"});
                        await tx0.DbContext.SaveChangesAsync();

                        tx0.VoteCommit();

                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0002"});
                            await tx1.DbContext.SaveChangesAsync();

                            tx1.VoteCommit();

                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0003"});
                                await tx2.DbContext.SaveChangesAsync();

                                tx2.VoteCommit();

                                using (var tx3 = await diInstance.BeginTranRequiredAsync())
                                {
                                    tx3.DbContext.Set<Table00>()
                                        .Add(new Table00 {Column00 = @"Z0004"});
                                    await tx3.DbContext.SaveChangesAsync();

                                    tx3.VoteCommit();

                                    using (var tx4 = await diInstance.BeginTranRequiredAsync())
                                    {
                                        tx4.DbContext.Set<Table00>().Add(new Table00
                                            {Column00 = @"Z0005"});
                                        await tx4.DbContext.SaveChangesAsync();

                                        tx4.VoteCommit();
                                    }
                                }
                            }
                        }
                    }
                }

                Assert.AreEqual(
                    5,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // COMPLEX
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx0 = await diInstance.BeginTranRequiredAsync())
                    {
                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0001"});
                        await tx0.DbContext.SaveChangesAsync();

                        tx0.VoteCommit();

                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0002"});
                            await tx1.DbContext.SaveChangesAsync();

                            tx1.VoteRollback();

                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0003"});
                                await tx2.DbContext.SaveChangesAsync();

                                tx2.VoteCommit();

                                using (var tx3 = await diInstance.BeginTranRequiredAsync())
                                {
                                    tx3.DbContext.Set<Table00>()
                                        .Add(new Table00 {Column00 = @"Z0004"});
                                    await tx3.DbContext.SaveChangesAsync();

                                    tx3.VoteCommit();

                                    using (var tx4 = await diInstance.BeginTranRequiredAsync())
                                    {
                                        tx4.DbContext.Set<Table00>().Add(new Table00
                                            {Column00 = @"Z0005"});
                                        await tx4.DbContext.SaveChangesAsync();

                                        tx4.VoteRollback();
                                    }
                                }
                            }
                        }
                    }
                }

                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();



                // ========================================
                // COMPLEX
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync())
                    {
                        tx0.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0001"});
                        await tx0.DbContext.SaveChangesAsync();

                        tx0.VoteCommit();

                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0002"});
                            await tx1.DbContext.SaveChangesAsync();

                            tx1.VoteCommit();

                            using (var tx2 = await diInstance.BeginTranRequiredAsync())
                            {
                                tx2.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z0003"});
                                await tx2.DbContext.SaveChangesAsync();

                                tx2.VoteCommit();

                                using (var tx3 = await diInstance.BeginTranRequiredAsync())
                                {
                                    tx3.DbContext.Set<Table00>()
                                        .Add(new Table00 {Column00 = @"Z0004"});
                                    await tx3.DbContext.SaveChangesAsync();

                                    tx3.VoteCommit();

                                    using (var tx4 = await diInstance.BeginTranRequiresNewAsync())
                                    {
                                        tx4.DbContext.Set<Table00>().Add(new Table00
                                            {Column00 = @"Z0005"});
                                        await tx4.DbContext.SaveChangesAsync();

                                        using (var tx5 = await diInstance.BeginTranRequiresNewAsync())
                                        {
                                            tx5.DbContext.Set<Table00>().Add(new Table00
                                                {Column00 = @"Z0006"});
                                            await tx5.DbContext.SaveChangesAsync();

                                            tx5.VoteCommit();
                                        }

                                        tx4.VoteRollback();
                                    }
                                }
                            }
                        }
                    }
                }

                Assert.AreEqual(
                    5,
                    n2DbContext.RawContext.Set<Table00>().Count());

                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z0004"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z0005"));
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z0006"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();
            }

            Assert.Pass();
        }




        [Test]
        public async Task AdditionsTxScope()
        {
            using (var n2DbContext = new N2SqlLocalDbContext<AliceDbContext>(
                CreateConnectionString(),
                _ => new AliceDbContext((DbContextOptions<AliceDbContext>) _)))
            {
                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();

                Assert.AreEqual(
                    0,
                    n2DbContext.RawContext.Set<Table00>().Count());

                using (var diInstance = CreateManager())
                {
                    using (var tx = await diInstance.BeginTranRequiredAsync(
                        IsolationLevel.ReadCommitted))
                    {
                        Assert.IsTrue(tx.Committable);

                        tx.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z9999"});
                        await tx.DbContext.SaveChangesAsync();

                        Assert.AreEqual(1, tx.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        tx.VoteCommit();
                    }
                }

                // ========================================
                // First Tx is BeginTranRequiresNewAsync, with Sub Tx is BeginTranRequiredAsync, and VoteRollback first Tx.
                // ========================================
                using (var diInstance = CreateManager())
                {
                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadCommitted))
                    {
                        using (var tx1 = await diInstance.BeginTranRequiredAsync())
                        {
                            tx1.DbContext.Set<Table00>().Add(new Table00 {Column00 = @"Z@@@1"});
                            await tx1.DbContext.SaveChangesAsync();

                            Assert.AreEqual(1,
                                tx1.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z@@@1"));

                            tx1.VoteCommit();
                        }

                        tx0.VoteRollback();
                    }

                    using (var tx0 = await diInstance.BeginTranRequiresNewAsync(IsolationLevel.ReadCommitted))
                    {
                        Assert.AreEqual(1, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));

                        Assert.AreEqual(0, tx0.DbContext.Set<Table00>().Count(e => e.Column00 == @"Z@@@1"));

                        tx0.VoteCommit();
                    }
                }

                // Commit / Rollback / Commit
                Assert.AreEqual(1,
                    n2DbContext.RawContext.Set<Table00>().Count(e => e.Column00 == @"Z9999"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z@@@1"));
                Assert.IsNull(n2DbContext.RawContext.Set<Table00>()
                    .SingleOrDefault(e => e.Column00 == @"Z@@@2"));

                // reset
                n2DbContext.RawContext.Set<Table00>().RemoveRange(n2DbContext.RawContext.Table00);
                await n2DbContext.RawContext.SaveChangesAsync();
            }
        }
    }
}