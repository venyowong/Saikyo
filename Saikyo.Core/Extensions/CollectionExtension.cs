using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Saikyo.Core.Exceptions;
using Saikyo.Core.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Extensions
{
    public static class CollectionExtension
    {
        public static Collection Configure(this Collection collection, IConfiguration config)
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

        public static Collection Configure(this Collection collection, string json)
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

            var keyValue = ReflectionHelper.GetValue(obj, collection.Key);
            if (keyValue == null)
            {
                throw new ArgumentNullException($"The {collection.Key} property of inserted object is null");
            }

            long id = collection.ColumnGathers[collection.Key].AddData(keyValue);
            var inserted = new ConcurrentDictionary<string, bool>();
            collection.ColumnGathers.Where(p => p.Key != collection.Key).AsParallel().ForAll(pair =>
            {
                var value = ReflectionHelper.GetValue(obj, pair.Key);
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
    }
}
