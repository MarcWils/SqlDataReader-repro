using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace SqlReaderTest
{
    internal class Program
    {
        const string connectionString = "Server=tcp:***.database.windows.net,1433;Initial Catalog=***;Persist Security Info=False;User ID=***;Password=***!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

        static async Task Main(string[] args)
        {
            await InitializeSchema();

            ThreadPool.SetMinThreads(100, 100);

            var cts = new CancellationTokenSource();

            for (int i = 0; i < 100; i++)
            {
                _ = Task.Factory.StartNew(() => TestDataReaderAsync(cts.Token));
            }
            Console.WriteLine("Test started");
            await Task.Delay(TimeSpan.FromMinutes(10));
            cts.Cancel();
            Console.WriteLine("Test completed");
        }

        private static async Task TestDataReaderAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using (var transactionScope = 
                        new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted }, TransactionScopeAsyncFlowOption.Enabled))
                    using (var sqlConn = new SqlConnection(connectionString))
                    {
                        await sqlConn.OpenAsync();
                        var cmd = sqlConn.CreateCommand();
                        cmd.CommandText = @"
insert into dbo.cars values (newid());

SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(1) *
    FROM dbo.cars WITH (UPDLOCK, READPAST, ROWLOCK)
    ORDER BY Id)
DELETE FROM message;

SELECT CAST(NEWID() AS VARCHAR(MAX));
";
                        using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess))
                        {
                            if (!await reader.ReadAsync())
                            {
                                var hasRows = reader.HasRows;
                                while (await reader.ReadAsync()) { }
                                while (await reader.NextResultAsync()) { }
                                throw new InvalidOperationException("Captured");
                            }
                            else
                            {
                                var name = reader.GetString(0);
                            }
                        }
                        sqlConn.Close();
                        transactionScope.Complete();
                    }
                }
                catch (Exception ex)
                {

                }
            }
        }

        private static async Task InitializeSchema()
        {
            using (var sqlConn = new SqlConnection(connectionString))
            {
                sqlConn.Open();
                var cmd = sqlConn.CreateCommand();

                cmd.CommandText = @"
drop table if exists dbo.cars;
create table dbo.cars (
    Id int identity(1,1) not null primary key,
    Name varchar(64) not null
);
";
                await cmd.ExecuteNonQueryAsync();
                sqlConn.Close();
            }
        }
    }
}
