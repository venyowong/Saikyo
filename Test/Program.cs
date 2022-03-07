// See https://aka.ms/new-console-template for more information
using Saikyo.Core;
using Test;

Instance.Init();
var db = Instance.Use("test");
using var collection = db.GetCollection<Model>("model");
//for (int i = 0; i < 10; i++)
//{
//    collection.Insert(new Model { Id = 1 + i, Age = 10 + i, Name = "foo" + i, Value = "bar" + i });
//}
//Console.WriteLine(collection.Query("Id == 1 || Id == 3").Build().Delete());
collection.Query("Age == 25")
    .Build()
    .Update("Age", 15)
    .Update("Name", "foo15");
Console.WriteLine(collection);
