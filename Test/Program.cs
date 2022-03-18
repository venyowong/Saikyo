using Dapper;
using MySql.Data.MySqlClient;
using Saikyo.Core;
using Saikyo.Core.Extensions;
using Serilog;
using Test;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

using var connection = new MySqlConnection("Server=localhost;Database=resader;Uid=root;Pwd=123456;");
var db = new Database("resader");
DateTime time;
using (var collection = db.GetCollection("article"))
{
    collection.Configure(File.ReadAllText("collections.json"));
    for (var i = 0; i < 1; i++)
    {
        time = DateTime.Now;
        var skip = 0;
        while (true)
        {
            if (skip >= 20000)
            {
                break;
            }

            Console.WriteLine(skip);
            var articles = await connection.QueryAsync($"SELECT * FROM resader.article limit {skip}, 1000;");
            if (articles == null || !articles.Any())
            {
                break;
            }

            skip = skip + 1000;
            foreach (var article in articles)
            {
                collection.Insert((object)article);
            }
        }
        Console.WriteLine(DateTime.Now - time);
    }
    time = DateTime.Now;
}
Console.WriteLine(DateTime.Now - time);