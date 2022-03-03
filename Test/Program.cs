// See https://aka.ms/new-console-template for more information
using Saikyo.Core;
using Saikyo.Core.Storage;
using Test;

Instance.Init();
var db = Instance.Use("test");
using var collection = db.GetCollection<Model>("model");
//for (int i = 0; i < 10; i++)
//{
//    collection.Insert(new Model { Id = 1 + i, Age = 10 + i, Name = "foo" + i, Value = "bar" + i });
//}
collection.Query("Id == 1 || Id == 3").Build().Delete();
Console.WriteLine(collection);
//var list = collection.GetAll();
//var result = collection.Query("Age >= 20 && Age <= 30").Build();
//result = collection.Query("Age >= 50").Build();
//var list = result.Select();
//Console.WriteLine(list.Count);
