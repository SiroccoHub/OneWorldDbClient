using Microsoft.EntityFrameworkCore;

namespace OneWorldDbClient.Tests.Model
{
    public partial class OneWorldDbUnitTestDbContext : DbContext
    {
        public OneWorldDbUnitTestDbContext(string connectionString) : base()
        { }

        public OneWorldDbUnitTestDbContext(DbContextOptions<OneWorldDbUnitTestDbContext> options) : base(options)
        { }

        public virtual DbSet<SampleTable01> SampleTable01 { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SampleTable01>(entity =>
            {
                entity.HasKey(e => e.SampleColumn01);

                entity.Property(e => e.SampleColumn01)
                    .HasMaxLength(10)
                    .ValueGeneratedNever();
            });
        }
    }
}
