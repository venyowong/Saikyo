using Saikyo.Core.Extensions;
using Saikyo.Core.Query;
using Saikyo.Core.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Saikyo.Core
{
    public abstract class BaseCollection : ICollection
    {
        public string Database { get; private set; }

        public string Name { get; private set; }

        public string Key { get; protected set; }

        internal Dictionary<string, IGather> ColumnGathers { get; set; } = new Dictionary<string, IGather>();

        protected ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
        protected bool disposed = false;

        public BaseCollection(string db, string name)
        {
            this.Database = db;
            this.Name = name;
        }

        public bool Insert(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException();
            }
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't add data into it");
            }

            return this.rwls.WriteLock(() =>
            {
                var keyValue = this.GetValue(obj, this.Key);
                if (keyValue == null)
                {
                    throw new ArgumentNullException($"The {this.Key} property of inserted object is null");
                }

                long id = this.ColumnGathers[this.Key].AddData(keyValue);
                var inserted = new ConcurrentDictionary<string, bool>();
                this.ColumnGathers.Where(p => p.Key != this.Key).AsParallel().ForAll(pair =>
                {
                    var value = this.GetValue(obj, pair.Key);
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
                    this.ColumnGathers.AsParallel().ForAll(pair =>
                    {
                        if (pair.Key == this.Key || (inserted.ContainsKey(pair.Key) && inserted[pair.Key]))
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
            });
        }

        public abstract IQueryBuilder Query(string condition = null);

        public bool Delete(List<long> ids)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't delete data from it");
            }

            return this.rwls.WriteLock(() =>
            {
                var result = true;
                this.ColumnGathers.Values.AsParallel().ForAll(g =>
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
            });
        }

        public void Update(string column, List<long> ids, object value)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't update data in it");
            }

            this.rwls.WriteLock(() =>
            {
                var gather = this.GetGather(column);
                if (gather == null)
                {
                    return;
                }

                ids.ForEach(x => gather.Update(x, value));
            });
        }

        public void Drop()
        {
            if (!this.disposed)
            {
                this.disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Destroy());
                this.rwls.Dispose();
            }
        }

        public virtual void Dispose()
        {
            if (!this.disposed)
            {
                this.disposed = true;
                this.ColumnGathers.Values.AsParallel().ForAll(x => x.Dispose());
                this.rwls.Dispose();
            }
        }

        internal IGather GetGather(string column)
        {
            if (this.disposed)
            {
                throw new InvalidOperationException("This collection has been disposed, you can't get data from it");
            }

            if (!this.ColumnGathers.ContainsKey(column))
            {
                return null;
            }

            return this.ColumnGathers[column];
        }

        internal abstract Type GetPropertyType(string column);

        internal abstract List<dynamic> Compose(List<long> ids, Dictionary<string, Dictionary<long, Column>> indeies, params string[] properties);

        protected abstract object GetValue(object obj, string property);
    }
}
