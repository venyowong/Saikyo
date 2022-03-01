using Saikyo.Core.Attributes;
using Saikyo.Core.Storage;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace Saikyo.Core
{
    internal class Kernel
    {
        private ConcurrentDictionary<string, Database> databases = new ConcurrentDictionary<string,Database>();
        private ConcurrentDictionary<string, object> collections = new ConcurrentDictionary<string, object>();
        private ConcurrentDictionary<string, ConstructorInfo> gatherConstractors = new ConcurrentDictionary<string, ConstructorInfo>();
        private ConcurrentDictionary<PropertyInfo, int> propertySizes = new ConcurrentDictionary<PropertyInfo, int>();

        public Kernel()
        {
            Log.Information("SaikyoDB init kernel");
        }

        public Database GetDatabase(string db)
        {
            if (this.databases.ContainsKey(db))
            {
                return this.databases[db];
            }

            lock (this.databases)
            {
                if (this.databases.ContainsKey(db))
                {
                    return this.databases[db];
                }

                var database = new Database(db);
                this.databases.TryAdd(db, database);
                return database;
            }
        }

        public Collection<T> GetCollection<T>(string db, string name) where T : new()
        {
            var key = $"{db}.{name}";
            if (this.collections.ContainsKey(key))
            {
                return this.collections[key] as Collection<T>;
            }

            lock (this.collections)
            {
                if (this.collections.ContainsKey(key))
                {
                    return this.collections[key] as Collection<T>;
                }

                var collection = new Collection<T>(db, name);
                this.collections.TryAdd(key, collection);
                return collection;
            }
        }

        public dynamic GetGather(string database, string collection, PropertyInfo property)
        {
            var blockSize = GetBlockSize(property);
            if (blockSize == 0 && Type.GetTypeCode(property.PropertyType) != TypeCode.String)
            {
                if (property.GetCustomAttribute<IgnoreAttribute>() == null)
                {
                    throw new NotSupportedException($"The property {property.Name} is {property.PropertyType.Name}, which is not supported. Please use IgnoreAttribute to ignore this property");
                }
                else
                {
                    return null;
                }
            }

            if (blockSize == 0)
            {
                return new TextGather(database, collection, property.Name);
            }

            if (this.gatherConstractors.ContainsKey(property.PropertyType.Name))
            {
                return this.gatherConstractors[property.PropertyType.Name].Invoke(new object[] { database, collection, property.Name, GetBlockSize(property) });
            }

            lock (this.gatherConstractors)
            {
                if (this.gatherConstractors.ContainsKey(property.PropertyType.Name))
                {
                    return this.gatherConstractors[property.PropertyType.Name].Invoke(new object[] { database, collection, property.Name, GetBlockSize(property) });
                }

                var constructor = typeof(BinaryGather<>).MakeGenericType(property.PropertyType).GetConstructors().First();
                this.gatherConstractors.TryAdd(property.PropertyType.Name, constructor);
                return constructor.Invoke(new object[] { database, collection, property.Name, GetBlockSize(property) });
            }
        }

        private int GetBlockSize(PropertyInfo property)
        {
            if (this.propertySizes.ContainsKey(property))
            {
                return this.propertySizes[property];
            }

            lock (this.propertySizes)
            {
                if (this.propertySizes.ContainsKey(property))
                {
                    return this.propertySizes[property];
                }

                var result = default(int);
                var code = Type.GetTypeCode(property.PropertyType);
                switch (code)
                {
                    case TypeCode.Boolean:
                    case TypeCode.SByte:
                    case TypeCode.Byte:
                        result = 1 + Const.AVLBlockHeaderSize;
                        break;
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                        result = 2 + Const.AVLBlockHeaderSize;
                        break;
                    case TypeCode.Char:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Single:
                        result = 4 + Const.AVLBlockHeaderSize;
                        break;
                    case TypeCode.DateTime:
                    case TypeCode.Double:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                        result = 8 + Const.AVLBlockHeaderSize;
                        break;
                    case TypeCode.Decimal:
                        result = 200 + Const.AVLBlockHeaderSize;
                        break;
                    case TypeCode.String:
                        var size = property.GetCustomAttribute<SizeAttribute>();
                        if (size != null)
                        {
                            result = size.Size + Const.AVLBlockHeaderSize;
                        }
                        break;
                }
                this.propertySizes.TryAdd(property, result);
                return result;
            }
        }
    }
}
