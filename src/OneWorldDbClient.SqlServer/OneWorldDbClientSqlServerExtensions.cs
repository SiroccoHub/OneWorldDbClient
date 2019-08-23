using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace OneWorldDbClient.SqlServer
{
    public static class OneWorldDbClientSqlServerExtensions
    {
        public static async Task<List<TResult>> ExecuteQueryAsync<TDbContext, TResult>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            Func<IDataReader, TResult> mapper
        ) where TResult : new() where TDbContext : DbContext
        {
            return await txScope.ExecuteQueryAsync(query, null, mapper);
        }


        public static async Task<List<TResult>> ExecuteQueryAsync<TDbContext, TResult>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            IDataParameter[] parameters,
            Func<IDataReader, TResult> mapper
        ) where TResult : new() where TDbContext : DbContext
        {
            return await txScope.DbContext.ExecuteQueryAsync(txScope.DbTransaction, query, parameters, mapper);
        }


        public static async Task<List<TResult>> ExecuteQueryAsync<TResult>(
            this DbContext ctx,
            string query,
            Func<IDataReader, TResult> mapper
            ) where TResult : new()
        {
            return await ctx.ExecuteQueryAsync(null, query, null, mapper);
        }


        public static async Task<List<TResult>> ExecuteQueryAsync<TResult>(
            this DbContext ctx,
            string query,
            IDataParameter[] parameters,
            Func<IDataReader, TResult> mapper
            ) where TResult : new()
        {
            return await ctx.ExecuteQueryAsync(null, query, parameters, mapper);
        }


        public static async Task<List<TResult>> ExecuteQueryAsync<TResult>(
            this DbContext ctx,
            IDbTransaction tx,
            string query,
            IDataParameter[] parameters,
            Func<IDataReader, TResult> mapper
            ) where TResult : new()
        {
            var connection = ctx.Database.GetDbConnection();

            using (
                var command = new SqlCommand
                {
                    CommandText = query,
                    CommandType = CommandType.Text,
                    Connection = (SqlConnection)connection,
                    Transaction = (SqlTransaction)tx
                })
            {
                if (parameters != null)
                    foreach (var param in parameters)
                        command.Parameters.Add(param);

                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                var results = new List<TResult>();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        results.Add(mapper(reader));
                    }
                }

                return results;
            }
        }





        public static async Task<int> ExecuteNonQueryAsync<TDbContext>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query
        ) where TDbContext : DbContext
        {
            return await txScope.ExecuteNonQueryAsync(query, null);
        }


        public static async Task<int> ExecuteNonQueryAsync<TDbContext>(
            this OneWorldDbTransactionScope<TDbContext> txScope,
            string query,
            IDataParameter[] parameters
        ) where TDbContext : DbContext
        {
            return await txScope.DbContext.ExecuteNonQueryAsync(txScope.DbTransaction, query, parameters);
        }


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            string query)
        {
            return await ctx.ExecuteNonQueryAsync(null, query, null);
        }


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            string query,
            IDataParameter[] parameters)
        {
            return await ctx.ExecuteNonQueryAsync(null, query, parameters);
        }


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            IDbTransaction tx,
            string query,
            IDataParameter[] parameters)
        {
            var connection = ctx.Database.GetDbConnection();

            using (
                var command = new SqlCommand
                {
                    CommandText = query,
                    CommandType = CommandType.Text,
                    Connection = (SqlConnection)connection,
                    Transaction = (SqlTransaction)tx
                })
            {
                if (parameters != null)
                    foreach (var param in parameters)
                        command.Parameters.Add(param);

                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                return await command.ExecuteNonQueryAsync();
            }
        }
    }
}
