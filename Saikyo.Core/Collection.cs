using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Saikyo.Core.Attributes;
using Saikyo.Core.Extensions;
using Saikyo.Core.Helpers;
using Saikyo.Core.Query;
using Saikyo.Core.Storage;
using Saikyo.Core.Storage.Gathers;
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
    public class Collection : IDynamicCollection
    {
        public string Database { get; private set; }

        public string Name { get; private set; }

        public bool Disposed { get; private set; }

        public string Key { get; private set; }

        public Dictionary<string, dynamic> ColumnGathers { get; private set; } = new Dictionary<string, dynamic>();

        private Dictionary<string, int> sizes = new Dictionary<string, int>();
        private Dictionary<string, Type> types = new Dictionary<string, Type>();
        private string dataPath;

        public Collection(string db, string name, string dataPath = "data")
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
                        var blockCap = 0;
                        if (this.sizes.ContainsKey(pair.Key))
                        {
                            blockCap = this.sizes[pair.Key];
                        }
                        var gather = pair.Value.CreateGather(directory.FullName, pair.Key, blockCap);
                        if (gather != null)
                        {
                            this.ColumnGathers.Add(pair.Key, gather);
                        }
                    }
                }
            }
        }

        public void Drop()
        {
            if (!this.Disposed)
            {
                this.Disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Destroy());
            }
        }

        public IQueryBuilder Query(string condition = null)
        {
            if (this.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            return new QueryBuilder(this, condition);
        }

        public void Dispose()
        {
            if (!this.Disposed)
            {
                this.Disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Dispose());

                var json = JsonConvert.SerializeObject(new
                {
                    key = this.Key,
                    types = this.types.ToDictionary(x => x.Key, x => x.Value.Name),
                    this.sizes
                });
                var directory = new DirectoryInfo(Path.Combine(this.dataPath, Path.Combine(this.Database, this.Name)));
                File.WriteAllText(Path.Combine(directory.FullName, "collection.json"), json);
            }
        }

        public Type GetPropertyType(string column)
        {
            if (this.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            if (!this.types.ContainsKey(column))
            {
                return null;
            }

            return this.types[column];
        }

        public IDynamicCollection SetProperty(string property, Type type, int size = 0, bool key = false)
        {
            if (this.types.ContainsKey(property))
            {
                return this;
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
            var gather = type.CreateGather(Path.Combine(this.dataPath, Path.Combine(this.Database, this.Name)), property, size);
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

        public IDynamicCollection SetProperty<T>(string property, int size = 0, bool key = false) => this.SetProperty(property, typeof(T), size, key);
    }

    public class Collection<T> : ICollection where T : new()
    {
        public string Database { get; private set; }

        public string Name { get; private set; }

        public bool Disposed { get; private set; }

        public string Key { get; private set; }

        public Dictionary<string, dynamic> ColumnGathers { get; private set; } = new Dictionary<string, dynamic>();

        private Dictionary<string, PropertyInfo> properties;
        private string dataPath;

        public Collection(string db, string name, string dataPath = "data")
        {
            this.dataPath = dataPath;
            this.Database = db;
            this.Name = name;
            this.properties = typeof(T).GetProperties().ToDictionary(x => x.Name);
            var directory = new DirectoryInfo(Path.Combine(dataPath, Path.Combine(db, name)));
            if (!directory.Exists)
            {
                directory.Create();
            }

            foreach (var property in this.properties.Values)
            {
                var gather = property.CreateGather(directory.FullName);
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

        public void Dispose()
        {
            if (!this.Disposed)
            {
                this.Disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Dispose());
            }
        }

        public void Drop()
        {
            if (!this.Disposed)
            {
                this.Disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Destroy());
            }
        }

        public Type GetPropertyType(string column)
        {
            if (this.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            if (!this.properties.ContainsKey(column))
            {
                return null;
            }

            return this.properties[column].PropertyType;
        }

        public IQueryBuilder Query(string condition = null)
        {
            if (this.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            return new QueryBuilder(this, condition);
        }
    }
}
