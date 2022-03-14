using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Saikyo.Core.Attributes;
using Saikyo.Core.Extensions;
using Saikyo.Core.Helpers;
using Saikyo.Core.Query;
using Saikyo.Core.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Saikyo.Core
{
    /// <summary>
    /// This collection type is designed for anonymous classes or dynamic objects
    /// </summary>
    public class Collection : BaseCollection
    {
        private Dictionary<string, PropertyInfo> properties;
        private Dictionary<string, int> sizes = new Dictionary<string, int>();
        private Dictionary<string, Type> types = new Dictionary<string, Type>();

        public Collection(string db, string name) : base(db, name)
        {
            var directory = new DirectoryInfo(Path.Combine(Instance.Config.DataPath, Path.Combine(db, name)));
            if (!directory.Exists)
            {
                directory.Create();
            }
            var file = new FileInfo(Path.Combine(directory.FullName, "collection.json"));
            if (file.Exists)
            {
                var json = File.ReadAllText(file.FullName);
                var jObject = JsonConvert.DeserializeObject<JObject>(json);
                this.Key = jObject["key"].ToString();
                if (jObject.ContainsKey("types"))
                {
                    this.types = jObject["types"].ToObject<Dictionary<string, string>>()
                        .Select(x => (x.Key, Value: TypeHelper.GetType(x.Value)))
                        .Where(x => x.Value != null)
                        .ToDictionary(x => x.Key, x => x.Value);
                }
                if (jObject.ContainsKey("sizes"))
                {
                    this.sizes = jObject["sizes"].ToObject<Dictionary<string, int>>();
                }
                if (this.types.Any())
                {
                    foreach (var pair in this.types)
                    {
                        var blockSize = 0;
                        if (this.sizes.ContainsKey(pair.Key))
                        {
                            blockSize = this.sizes[pair.Key];
                        }
                        var gather = Instance.Kernel.GetGather(this.Database, this.Name, pair.Key, pair.Value, blockSize);
                        if (gather != null)
                        {
                            this.ColumnGathers.Add(pair.Key, gather);
                        }
                    }
                }
            }
        }

        public override IQueryBuilder Query(string condition = null)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }
            if (!this.ColumnGathers.Any())
            {
                throw new InvalidOperationException("There are no columns in this collection, please use InitType first");
            }

            return new QueryBuilder(this, condition);
        }

        public Collection SetProperty(string property, Type type, int size = 0, bool key = false)
        {
            if (this.types.ContainsKey(property))
            {
                throw new InvalidOperationException($"Duplicate property({property})");
            }
            if (string.IsNullOrWhiteSpace(this.Key))
            {
                this.Key = property; // default use first property as key
            }
            if (key)
            {
                this.Key = property;
            }

            this.types.Add(property, type);
            if (size > 0)
            {
                this.sizes.Add(property, size);
            }
            var gather = Instance.Kernel.GetGather(this.Database, this.Name, property, type, size);
            if (gather != null)
            {
                if (property == this.Key)
                {
                    if (gather is TextGather)
                    {
                        throw new NotSupportedException($"{property} is a string with no specified size, so it cannot be used as key");
                    }
                }
                this.ColumnGathers.Add(property, gather);
            }

            return this;
        }

        public override void Dispose()
        {
            if (!this.disposed)
            {
                base.Dispose();

                var json = JsonConvert.SerializeObject(new
                {
                    key = this.Key,
                    types = this.types.ToDictionary(x => x.Key, x => x.Value.Name),
                    this.sizes
                });
                var directory = new DirectoryInfo(Path.Combine(Instance.Config.DataPath, Path.Combine(this.Database, this.Name)));
                File.WriteAllText(Path.Combine(directory.FullName, "collection.json"), json);
            }
        }

        internal override List<dynamic> Compose(List<long> ids, Dictionary<string, Dictionary<long, Column>> indeies, params string[] properties)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }
            if (ids == null)
            {
                return new List<dynamic>();
            }

            if (properties.IsNullOrEmpty())
            {
                properties = this.ColumnGathers.Keys.ToArray();
            }

            return this.rwls.ReadLock(() =>
            {
                var columns = new ConcurrentDictionary<string, Dictionary<long, Column>>();
                this.ColumnGathers.Where(x => properties.Contains(x.Key)).AsParallel().ForAll(g =>
                {
                    Dictionary<long, Column> columnMap = null;
                    if (indeies.ContainsKey(g.Key))
                    {
                        columnMap = indeies[g.Key];
                    }
                    else
                    {
                        columnMap = new Dictionary<long, Column>();
                    }

                    foreach (var id in ids)
                    {
                        if (columnMap.ContainsKey(id))
                        {
                            continue;
                        }

                        if (g.Value is IColumnGetter columnGetter)
                        {
                            columnMap[id] = columnGetter.GetColumn(id);
                        }
                    }
                    columns.TryAdd(g.Key, columnMap);
                });

                return ids.Select(x =>
                {
                    var row = (IDictionary<string, object>)new ExpandoObject();
                    foreach (var p in properties)
                    {
                        if (!columns.ContainsKey(p))
                        {
                            continue;
                        }

                        var columnMap = columns[p];
                        if (!columnMap.ContainsKey(x))
                        {
                            continue;
                        }

                        row[p] = columnMap[x]?.Value;
                    }
                    return (dynamic)row;
                })
                .ToList();
            });
        }

        internal override Type GetPropertyType(string column)
        {
            if (this.types.ContainsKey(column))
            {
                return this.types[column];
            }
            else if (this.properties != null && this.properties.ContainsKey(column))
            {
                return this.properties[column].PropertyType;
            }
            else
            {
                throw new InvalidOperationException($"Unknown property({column}), please use InitType to enable this collection to recognize this property");
            }
        }

        protected override object GetValue(object obj, string property)
        {
            if (!this.types.ContainsKey(property))
            {
                throw new InvalidOperationException($"Unknown property({property}), please use InitType to enable this collection to recognize this property");
            }

            if (obj is IDictionary<string, object> dict)
            {
                return dict[property];
            }
            else
            {
                if (this.properties == null)
                {
                    this.properties = obj.GetType().GetProperties().ToDictionary(x => x.Name);
                }

                return this.properties[property].GetValue(obj);
            }
        }
    }

    public class Collection<T> : BaseCollection where T : new()
    {
        private Dictionary<string, PropertyInfo> properties;

        public Collection(string db, string name) : base(db, name)
        {
            this.properties = typeof(T).GetProperties().ToDictionary(x => x.Name);
            foreach (var property in this.properties.Values)
            {
                var gather = Instance.Kernel.GetGather(this.Database, this.Name, property);
                if (gather != null)
                {
                    if (string.IsNullOrWhiteSpace(this.Key))
                    {
                        this.Key = property.Name; // default use first property as key
                    }
                    if (property.Name == this.Key)
                    {
                        if (gather is TextGather)
                        {
                            throw new NotSupportedException($"{property.Name} is a string with no specified size, so it cannot be used as key");
                        }
                    }
                    this.ColumnGathers.Add(property.Name, gather);
                }
            }

            if (string.IsNullOrWhiteSpace(this.Key))
            {
                throw new NotSupportedException($"Cannot get key property from {typeof(T).Name}");
            }
        }

        public override IQueryBuilder Query(string condition = null)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            return new QueryBuilder<T>(this, condition);
        }

        internal override Type GetPropertyType(string column)
        {
            if (this.properties.ContainsKey(column))
            {
                return this.properties[column].PropertyType;
            }
            else
            {
                return null;
            }
        }

        internal override List<dynamic> Compose(List<long> ids, Dictionary<string, Dictionary<long, Column>> indeies, params string[] properties)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            if (properties.IsNullOrEmpty())
            {
                properties = this.ColumnGathers.Keys.ToArray();
            }

            return this.rwls.ReadLock(() =>
            {
                var columns = new ConcurrentDictionary<string, Dictionary<long, Column>>();
                this.ColumnGathers.Where(x => properties.Contains(x.Key)).AsParallel().ForAll(g =>
                {
                    Dictionary<long, Column> columnMap = null;
                    if (indeies.ContainsKey(g.Key))
                    {
                        columnMap = indeies[g.Key];
                    }
                    else
                    {
                        columnMap = new Dictionary<long, Column>();
                    }

                    foreach (var id in ids)
                    {
                        if (columnMap.ContainsKey(id))
                        {
                            continue;
                        }

                        if (g.Value is IColumnGetter columnGetter)
                        {
                            columnMap[id] = columnGetter.GetColumn(id);
                        }
                    }
                    columns.TryAdd(g.Key, columnMap);
                });

                return ids.Select(x =>
                {
                    var row = new T();
                    foreach (var p in this.properties)
                    {
                        if (!columns.ContainsKey(p.Key))
                        {
                            continue;
                        }

                        var columnMap = columns[p.Key];
                        if (!columnMap.ContainsKey(x))
                        {
                            continue;
                        }

                        p.Value.SetValue(row, columnMap[x]?.Value);
                    }
                    return (dynamic)row;
                })
                .ToList();
            });
        }

        protected override object GetValue(object obj, string property) => this.properties[property].GetValue(obj);
    }

    public class Collection2 : ICollection
    {
        public string Database { get; private set; }

        public string Name { get; private set; }

        public bool Disposed { get; private set; }

        public string Key { get; private set; }

        public Dictionary<string, dynamic> ColumnGathers { get; private set; }

        private Dictionary<string, int> sizes = new Dictionary<string, int>();
        private Dictionary<string, Type> types = new Dictionary<string, Type>();
        private string dataPath;

        public Collection2(string db, string name, string dataPath = "data")
        {
            this.dataPath = dataPath;
            this.Database = db;
            this.Name = name;
            var directory = new DirectoryInfo(Path.Combine(dataPath, Path.Combine(db, name)));
            if (!directory.Exists)
            {
                directory.Create();
            }
            var file = new FileInfo(Path.Combine(directory.FullName, "collection.json"));
            if (file.Exists)
            {
                var json = File.ReadAllText(file.FullName);
                var jObject = JsonConvert.DeserializeObject<JObject>(json);
                this.Key = jObject["key"].ToString();
                if (jObject.ContainsKey("types"))
                {
                    this.types = jObject["types"].ToObject<Dictionary<string, string>>()
                        .Select(x => (x.Key, Value: TypeHelper.GetType(x.Value)))
                        .Where(x => x.Value != null)
                        .ToDictionary(x => x.Key, x => x.Value);
                }
                if (jObject.ContainsKey("sizes"))
                {
                    this.sizes = jObject["sizes"].ToObject<Dictionary<string, int>>();
                }
                if (this.types.Any())
                {
                    foreach (var pair in this.types)
                    {
                        var blockSize = 0;
                        if (this.sizes.ContainsKey(pair.Key))
                        {
                            blockSize = this.sizes[pair.Key];
                        }
                        var gather = Instance.Kernel.GetGather(this.Database, this.Name, pair.Key, pair.Value, blockSize);
                        if (gather != null)
                        {
                            this.ColumnGathers.Add(pair.Key, gather);
                        }
                    }
                }
            }
        }

        public bool Delete(List<long> ids)
        {
            throw new NotImplementedException();
        }

        public void Drop()
        {
            throw new NotImplementedException();
        }

        public IQueryBuilder Query(string condition = null)
        {
            throw new NotImplementedException();
        }

        public void Update(string column, List<long> ids, object value)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
