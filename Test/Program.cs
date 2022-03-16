using Dapper;
using MySql.Data.MySqlClient;
using Saikyo.Core;
using Saikyo.Core.Extensions;
using Test;

using var connection = new MySqlConnection("Server=localhost;Database=resader;Uid=root;Pwd=123456;");
var feeds = (await connection.QueryAsync("SELECT * FROM resader.feed;")).ToList();
var db = new Database("resader");
using var collection = db.GetCollection("feed");
collection.SetProperty<string>("id", 50, true)
    .SetProperty<string>("url", 500)
    .SetProperty<string>("title", 500)
    .SetProperty<string>("label", 100)
    .SetProperty<string>("description")
    .SetProperty<string>("image", 500)
    .SetProperty<DateTime>("create_time")
    .SetProperty<DateTime>("update_time");
//var time = DateTime.Now;
//feeds.ForEach(f => collection.Insert((object)f));
////collection.Insert((object)feeds[0]);
//Console.WriteLine(DateTime.Now - time);
var result = collection.Query().Build().Select();
Console.WriteLine(result);