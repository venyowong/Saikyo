// See https://aka.ms/new-console-template for more information
using MySql.Data.MySqlClient;
using Saikyo.Adapter.Sql;
using Saikyo.Core;

Instance.Init();
using var connection = new MySqlConnection("Server=localhost;Database=resader;Uid=root;Pwd=123456;");
var time = DateTime.Now;
connection.FromDatabase("resader", "article");
Console.WriteLine(DateTime.Now - time);