using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace N2UnitTestBase
{
    internal static class ExecuteAsyncExtensions
    {
        public static async Task<List<T>> ExecuteQueryAsync<T>(
            this DbContext ctx,
            string query,
            Func<SqlDataReader, T> mapper) where T : new()
        {
            return await ctx.ExecuteQueryAsync(null, query, null, mapper);
        }

        public static async Task<List<T>> ExecuteQueryAsync<T>(
            this DbContext ctx,
            string query,
            SqlParameter[] parameters,
            Func<SqlDataReader, T> mapper) where T : new()
        {
            return await ctx.ExecuteQueryAsync(null, query, parameters, mapper);
        }

        public static async Task<List<T>> ExecuteQueryAsync<T>(
            this DbContext ctx,
            IDbTransaction tx,
            string query,
            SqlParameter[] parameters,
            Func<SqlDataReader, T> mapper) where T : new()
        {
            var connection = (SqlConnection)ctx.Database.GetDbConnection();

            using (var command = new SqlCommand
            {
                CommandText = query,
                CommandType = CommandType.Text,
                Connection = connection,
                Transaction = (SqlTransaction)tx
            })
            {
                if (parameters != null)
                    foreach (var sqlParameter in parameters)
                        command.Parameters.Add(sqlParameter);

                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                var results = new List<T>();

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


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            string query)
        {
            return await ctx.ExecuteNonQueryAsync(null, query, null);
        }


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            string query,
            SqlParameter[] parameters)
        {
            return await ctx.ExecuteNonQueryAsync(null, query, parameters);
        }


        public static async Task<int> ExecuteNonQueryAsync(
            this DbContext ctx,
            IDbTransaction tx,
            string query,
            SqlParameter[] parameters)
        {
            var connection = (SqlConnection)ctx.Database.GetDbConnection();

            using (var command = new SqlCommand()
            {
                CommandText = query,
                CommandType = CommandType.Text,
                Connection = connection,
                Transaction = (SqlTransaction)tx
            })
            {
                if (parameters != null)
                    foreach (var sqlParameter in parameters)
                        command.Parameters.Add(sqlParameter);

                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                return await command.ExecuteNonQueryAsync();
            }
        }
    }
}
