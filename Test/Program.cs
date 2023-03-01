using Bogus;
using Saikyo.Core;
using Saikyo.Core.Extensions;
using Serilog;
using System.Diagnostics;
using Test;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

var db = new Saikyo.Core.Database("test");
var models = new Faker<Model>()
    .RuleFor(m => m.Id, f => f.Random.Int(0))
    .RuleFor(m => m.Name, f => f.Name.FullName())
    .RuleFor(m => m.Age, f => f.Random.Int(0, 100))
    .RuleFor(m => m.Value, f => f.Random.Word())
    .RuleFor(m => m.Value2, f => f.Random.String());

var collection = db.GetCollection<Model>("model");
var times = 20000;
var stopwatch = new Stopwatch();
foreach (var i in Enumerable.Range(0, times))
{
    var model = models.Generate();
    stopwatch.Start();
    collection.Insert(model);
    stopwatch.Stop();
}
Console.WriteLine($"insert: {stopwatch.Elapsed}");
stopwatch.Restart();
collection.Dispose();
stopwatch.Stop();
Console.WriteLine($"dispose: {stopwatch.Elapsed}");
Console.ReadLine();