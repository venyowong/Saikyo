using Dapper;
using Microsoft.Extensions.Configuration;
using Saikyo.Core;
using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.Data;
using System.IO;
using System.Linq;

namespace Saikyo.Adapter.Sql
{
    public static class ConnectionExtension
    {
        public static void FromDatabase(this IDbConnection connection, string db, string table)
        {
            var list = connection?.Query($"SELECT * FROM {db}.{table}").ToList();
            if (list == null || !list.Any())
            {
                Log.Warning($"There are no data in {db}.{table}");
                return;
            }

            var database = Instance.Use(db);
            using (var collection = database.GetCollection(table))
            {
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetParent(AppContext.BaseDirectory)?.FullName)
                    .AddJsonFile("collections.json")
                    .Build();
                collection.Configure(configuration);
                list.AsParallel().ForAll(f => collection.Insert(f));
            }
        }
    }
}
