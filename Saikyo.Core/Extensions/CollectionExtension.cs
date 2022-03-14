using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Saikyo.Core.Exceptions;
using Saikyo.Core.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Saikyo.Core.Extensions
{
    public static class CollectionExtension
    {
        public static IDynamicCollection Configure(this IDynamicCollection collection, IConfiguration config)
        {
            if (collection == null || config == null)
            {
                return collection;
            }

            var subConfig = config.GetSection(collection.Name);
            if (subConfig != null)
            {
                config = subConfig;
            }
            foreach (var property in config.GetChildren())
            {
                var name = property.Key;
                if (collection.ColumnGathers.ContainsKey(name))
                {
                    continue;
                }

                var type = property.GetSection("type");
                if (type == null)
                {
                    throw new ConfigurationException($"There is no type section under configuration section({name})");
                }
                int.TryParse(property.GetSection("size")?.Value, out int size);
                bool.TryParse(property.GetSection("key")?.Value, out var key);
                collection.SetProperty(name, TypeHelper.GetType(type.Value), size, key);
            }
            return collection;
        }

        public static IDynamicCollection Configure(this IDynamicCollection collection, string json)
        {
            if (collection == null || string.IsNullOrWhiteSpace(json))
            {
                return collection;
            }

            var jObject = JsonConvert.DeserializeObject<JObject>(json);
            if (jObject.ContainsKey(collection.Name))
            {
                jObject = (JObject)jObject[collection.Name];
            }
            foreach (var item in jObject)
            {
                var name = item.Key;
                if (collection.ColumnGathers.ContainsKey(name))
                {
                    continue;
                }

                var property = (JObject)item.Value;
                if (!property.ContainsKey("type"))
                {
                    throw new ConfigurationException($"There is no type path under json path({name})");
                }
                var type = property["type"].Value<string>();
                var size = 0;
                if (property.ContainsKey("size"))
                {
                    size = property["size"].Value<int>();
                }
                var key = false;
                if (property.ContainsKey("key"))
                {
                    key = property["key"].Value<bool>();
                }
                collection.SetProperty(name, TypeHelper.GetType(type), size, key);
            }

            return collection;
        }

        public static bool Insert(this ICollection collection, object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException();
            }
            if (collection.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't add data into it");
            }

            dynamic keyValue = ReflectionHelper.GetValue(obj, collection.Key);
            if (keyValue == null)
            {
                throw new ArgumentNullException($"The {collection.Key} property of inserted object is null");
            }

            long id = collection.ColumnGathers[collection.Key].AddData(keyValue);
            var inserted = new ConcurrentDictionary<string, bool>();
            collection.ColumnGathers.Where(p => p.Key != collection.Key).AsParallel().ForAll(pair =>
            {
                dynamic value = ReflectionHelper.GetValue(obj, pair.Key);
                if (value == null)
                {
                    return;
                }

                if (pair.Value.AddData(value, id) <= 0)
                {
                    inserted.TryAdd(pair.Key, false);
                }
                else
                {
                    inserted.TryAdd(pair.Key, true);
                }
            });
            if (inserted.Values.Any(x => !x))
            {
                collection.ColumnGathers.AsParallel().ForAll(pair =>
                {
                    if (pair.Key == collection.Key || (inserted.ContainsKey(pair.Key) && inserted[pair.Key]))
                    {
                        pair.Value.Delete(id);
                    }
                });
                return false;
            }
            else
            {
                return true;
            }
        }

        internal static bool Delete(this ICollection collection, List<long> ids)
        {
            if (collection.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't delete data from it");
            }

            var result = true;
            collection.ColumnGathers.Values.AsParallel().ForAll(g =>
            {
                ids.ForEach(id =>
                {
                    if (!g.Delete(id))
                    {
                        result = false;
                    }
                });
            });
            return result;
        }

        internal static void Update(this ICollection collection, string column, List<long> ids, object value)
        {
            if (collection.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't update data in it");
            }

            var gather = collection.GetGather(column);
            if (gather == null)
            {
                return;
            }

            ids.ForEach(x => gather.Update(x, value));
        }

        internal static List<dynamic> Compose(this ICollection collection, List<long> ids, Dictionary<string, Dictionary<long, Column>> indeies, params string[] properties)
        {
            if (collection.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }
            if (ids == null)
            {
                return new List<dynamic>();
            }

            if (properties.IsNullOrEmpty())
            {
                properties = collection.ColumnGathers.Keys.ToArray();
            }

            var columns = new ConcurrentDictionary<string, Dictionary<long, Column>>();
            collection.ColumnGathers.Where(x => properties.Contains(x.Key)).AsParallel().ForAll(g =>
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

            if (collection is IDynamicCollection)
            { 
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
            }
            else
            {
                return Compose(collection, columns, ids);
            }
        }

        internal static dynamic GetGather(this ICollection collection, string column)
        {
            if (collection.Disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }
            if (string.IsNullOrWhiteSpace(column))
            {
                return null;
            }

            if (!collection.ColumnGathers.ContainsKey(column))
            {
                return null;
            }

            return collection.ColumnGathers[column];
        }

        private static ConcurrentDictionary<string, (Type Type, PropertyInfo[] Properties)> _collectionTypes = 
            new ConcurrentDictionary<string, (Type Type, PropertyInfo[] Properties)>();
        private static List<dynamic> Compose(ICollection collection, IDictionary<string, Dictionary<long, Column>> columns, List<long> ids)
        {
            var collectionType = collection.GetType();
            Type type;
            PropertyInfo[] properties;
            if (_collectionTypes.ContainsKey(collectionType.FullName))
            {
                var tuple = _collectionTypes[collectionType.FullName];
                type = tuple.Type;
                properties = tuple.Properties;
            }
            else
            {
                var types = collectionType.GetGenericArguments();
                if (types.IsNullOrEmpty())
                {
                    return new List<dynamic>();
                }

                type = types[0];
                properties = type.GetProperties();
                _collectionTypes.TryAdd(collectionType.FullName, (type, properties));
            }

            return ids.Select(id =>
            {
                var row = Activator.CreateInstance(type);
                foreach (var p in properties)
                {
                    if (!columns.ContainsKey(p.Name))
                    {
                        continue;
                    }

                    var columnMap = columns[p.Name];
                    if (!columnMap.ContainsKey(id))
                    {
                        continue;
                    }

                    p.SetValue(row, columnMap[id]?.Value);
                }
                return (dynamic)row;
            })
            .ToList();
        }
    }
}
