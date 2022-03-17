using Dapper;
using MySql.Data.MySqlClient;
using Saikyo.Core;
using Saikyo.Core.Extensions;
using Test;

using var connection = new MySqlConnection("Server=localhost;Database=resader;Uid=root;Pwd=123456;");
var articles = (await connection.QueryAsync("SELECT * FROM resader.article;")).ToList();
var db = new Database("resader");
using var collection = db.GetCollection("article");
collection.Configure(File.ReadAllText("collections.json"));
//var time = DateTime.Now;
//articles.ForEach(f => collection.Insert((object)f));
//Console.WriteLine(DateTime.Now - time);
var result = collection.Query().Build().Select();
Console.WriteLine(result);