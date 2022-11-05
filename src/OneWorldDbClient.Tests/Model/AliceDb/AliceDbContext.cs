using Microsoft.EntityFrameworkCore;

namespace OneWorldDbClient.Tests.Model.AliceDb
{
    public class AliceDbContext : DbContext
    {
        public AliceDbContext(DbContextOptions<AliceDbContext> options) : base(options)
        { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Table00>(entity =>
            {
                entity.HasKey(e => e.Column00);

                entity.Property(e => e.Column00)
                    .HasMaxLength(10)
                    .ValueGeneratedNever();
            });
        }

        public DbSet<Table00> Table00 { get; set; }
    }
}
