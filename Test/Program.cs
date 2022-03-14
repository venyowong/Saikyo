using Saikyo.Core;
using Saikyo.Core.Extensions;
using Test;

var time = DateTime.Now;
var db = new Database("test");
using var collection = db.GetCollection("model");
collection.SetProperty<int>("Id", 0, true)
    .SetProperty<string>("Name", 100)
    .SetProperty<int>("Age");
//collection.Insert(new Model { Id = 0, Name = "Foo", Age = 10 });
//collection.Insert(new Model { Id = 1, Name = "Bar", Age = 12 });
var result = collection.Query().Build().Select();
Console.WriteLine(DateTime.Now - time);