using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace OneWorldDbClient
{
    public static class OneWorldDbTransactionScopeExtensions
    {
        public static async Task<List<TResult>> ExecuteQueryAsync<TDbContext, TResult>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            Func<SqlDataReader, TResult> mapper) where TResult : new() where TDbContext : DbContext
        {
            return await txScope.ExecuteQueryAsync(query, null, mapper);
        }


        public static async Task<List<TResult>> ExecuteQueryAsync<TDbContext, TResult>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            SqlParameter[] parameters,
            Func<SqlDataReader, TResult> mapper) where TResult : new() where TDbContext : DbContext
        {
            return await txScope.DbContext.ExecuteQueryAsync(txScope.DbTransaction, query, parameters, mapper);
        }


        public static async Task<int> ExecuteNonQueryAsync<TDbContext>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query) where TDbContext : DbContext
        {
            return await txScope.ExecuteNonQueryAsync(query, null);
        }


        public static async Task<int> ExecuteNonQueryAsync<TDbContext>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            SqlParameter[] parameters) where TDbContext : DbContext
        {
            return await txScope.DbContext.ExecuteNonQueryAsync(txScope.DbTransaction, query, parameters);
        }

    }
}
