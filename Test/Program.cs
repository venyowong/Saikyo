// See https://aka.ms/new-console-template for more information
using Saikyo.Core;
using Saikyo.Core.Storage;
using Test;

Instance.Init();
var db = Instance.Use("test");
using var collection = db.GetCollection<Model>("model");
//for (int i = 0; i < 2000; i++)
//{
//    collection.Insert(new Model { Id = 1 + i, Age = 10 + i, Name = "foo" + i, Value = "bar" + i });
//}
var list = collection.GetAll();
Console.WriteLine(list.Count);
