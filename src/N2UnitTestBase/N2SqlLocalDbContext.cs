﻿using Microsoft.EntityFrameworkCore;
using System;

namespace N2UnitTestBase
{
    public class N2SqlLocalDbContext<TDbContext> : IDisposable where TDbContext : DbContext
    {
        public TDbContext RawContext { get; }

        public string ConnectionString { get; }

        public N2SqlLocalDbContext(
            string connectionString,
            Func<DbContextOptions, TDbContext> factoryDbContext)
        {
            ConnectionString = connectionString;

            if (factoryDbContext == null)
                throw new ArgumentNullException(nameof(factoryDbContext));

            RawContext = factoryDbContext
                .Invoke(new DbContextOptionsBuilder<TDbContext>()
                    .UseSqlServer(connectionString)
                    .Options);

            EnsureCreated();
        }


        private bool EnsureCreated()
        {
            return RawContext.Database.EnsureCreated();
        }


        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    RawContext.Dispose();
                }

                disposedValue = true;
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
