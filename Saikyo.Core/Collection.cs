using Saikyo.Core.Attributes;
using Saikyo.Core.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Saikyo.Core
{
    public class Collection<T> : IDisposable where T : new()
    {
        public string Database { get; private set; }

        public string Name { get; private set; }

        private ReaderWriterLockSlim rwl = new ReaderWriterLockSlim();
        private PropertyInfo[] properties;
        private Dictionary<string, dynamic> columnGathers = new Dictionary<string, dynamic>();
        private string key;

        public Collection(string db, string name)
        {
            this.Database = db;
            this.Name = name;
            this.properties = typeof(T).GetProperties();
            foreach (var property in this.properties)
            {
                var gather = Instance.Kernel.GetGather(db, name, property);
                if (gather != null)
                {
                    if (string.IsNullOrWhiteSpace(this.key))
                    {
                        this.key = property.Name; // default use first property as key
                    }
                    if (property.GetCustomAttribute<KeyAttribute>() != null)
                    {
                        if (gather is TextGather)
                        {
                            throw new NotSupportedException($"{property.Name} is a string with no specified size, so it cannot be used as key");
                        }

                        this.key = property.Name;
                    }
                    this.columnGathers.Add(property.Name, gather);
                }
            }
        }

        public void Insert(T t)
        {
            if (t == null)
            {
                throw new ArgumentNullException();
            }

            // todo: constraint check

            // insert
            var ids = new ConcurrentBag<long>();
            this.properties.AsParallel().ForAll(p =>
            {
                if (this.columnGathers.ContainsKey(p.Name))
                {
                    dynamic value = p.GetValue(t);
                    ids.Add(this.columnGathers[p.Name].AddData(value));
                }
            });
            if (ids.Distinct().Count() > 1)
            {
                throw new SystemException("The data block IDs are inconsistent, which will lead to data confusion");
            }
        }

        public List<T> GetAll()
        {
            if (string.IsNullOrWhiteSpace(this.key))
            {
                return new List<T>();
            }

            List<Column> keys = this.columnGathers[this.key].GetAllBlocks();
            return this.Compose(keys.Select(x => x.Id).ToList(), new Dictionary<string, Dictionary<long, Column>>
            {
                { this.key, keys.ToDictionary(x => x.Id, x => x) }
            });
        }

        public void Dispose()
        {
            this.columnGathers.Values.AsParallel().ForAll(x => x.Dispose());
            this.rwl.Dispose();
        }

        private List<T> Compose(List<long> ids, Dictionary<string, Dictionary<long, Column>> indeies)
        {
            var columns = new ConcurrentDictionary<string, Dictionary<long, Column>>();
            this.columnGathers.AsParallel().ForAll(g =>
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

                    columnMap[id] = g.Value.GetColumn(id);
                }
                columns.TryAdd(g.Key, columnMap);
            });

            return ids.Select(x =>
            {
                var row = new T();
                foreach (var p in this.properties)
                {
                    if (!columns.ContainsKey(p.Name))
                    {
                        continue;
                    }

                    var columnMap = columns[p.Name];
                    if (!columnMap.ContainsKey(x))
                    {
                        continue;
                    }

                    p.SetValue(row, columnMap[x]?.Value);
                }
                return row;
            })
            .ToList();
        }
    }
}
