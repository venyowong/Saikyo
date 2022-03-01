using Saikyo.Core.Attributes;

namespace Test;

internal class Model
{
    [Key]
    public int Id { get; set; }

    [Size(100)]
    public string Name { get; set; } = string.Empty;

    public int Age { get; set; }

    public string Value { get; set; } = string.Empty;

    [Ignore]
    public object Value2 { get; set; } = new object();
}
