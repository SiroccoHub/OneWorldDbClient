using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace OneWorldDbClient.SampleWeb.Data
{
    public class ApplicationDbContext : IdentityDbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<SampleTable01> SampleTable01 { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

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
