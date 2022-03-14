using Saikyo.Core.Attributes;
using Saikyo.Core.Storage;
using Saikyo.Core.Storage.Gathers;
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

        public Collection GetCollection(string db, string name)
        {
            var key = $"{db}.{name}";
            if (this.collections.ContainsKey(key))
            {
                return this.collections[key] as Collection;
            }

            lock (this.collections)
            {
                if (this.collections.ContainsKey(key))
                {
                    return this.collections[key] as Collection;
                }

                var collection = new Collection(db, name);
                this.collections.TryAdd(key, collection);
                return collection;
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

        public IGather GetGather(string database, string collection, PropertyInfo property)
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
                return (IGather)this.gatherConstractors[property.PropertyType.Name].Invoke(new object[] { database, collection, property.Name, blockSize });
            }

            lock (this.gatherConstractors)
            {
                if (this.gatherConstractors.ContainsKey(property.PropertyType.Name))
                {
                    return (IGather)this.gatherConstractors[property.PropertyType.Name].Invoke(new object[] { database, collection, property.Name, blockSize });
                }

                var constructor = typeof(BinaryGather<>).MakeGenericType(property.PropertyType).GetConstructors().First();
                this.gatherConstractors.TryAdd(property.PropertyType.Name, constructor);
                return (IGather)constructor.Invoke(new object[] { database, collection, property.Name, blockSize });
            }
        }

        public IGather GetGather(string database, string collection, string key, Type type, int size = 0)
        {
            var blockSize = GetBlockSize(type);
            if (blockSize == 0 && size > 0)
            {
                blockSize = size + Const.AVLBlockHeaderSize;
            }
            if (blockSize == 0 && Type.GetTypeCode(type) != TypeCode.String)
            {
                throw new NotSupportedException($"The property {key} is {type.Name}, which is not supported. Please use IgnoreAttribute to ignore this property");
            }

            if (blockSize == 0)
            {
                return new TextGather(database, collection, key);
            }

            if (this.gatherConstractors.ContainsKey(type.Name))
            {
                return (IGather)this.gatherConstractors[type.Name].Invoke(new object[] { database, collection, key, blockSize });
            }

            lock (this.gatherConstractors)
            {
                if (this.gatherConstractors.ContainsKey(type.Name))
                {
                    return (IGather)this.gatherConstractors[type.Name].Invoke(new object[] { database, collection, key, blockSize });
                }

                var constructor = typeof(BinaryGather<>).MakeGenericType(type).GetConstructors().First();
                this.gatherConstractors.TryAdd(type.Name, constructor);
                return (IGather)constructor.Invoke(new object[] { database, collection, key, blockSize });
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

        private int GetBlockSize(Type type)
        {
            var result = default(int);
            var code = Type.GetTypeCode(type);
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
            }
            return result;
        }
    }
}
