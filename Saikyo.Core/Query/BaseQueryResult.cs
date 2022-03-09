using Saikyo.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Query
{
    public abstract class BaseQueryResult : IQueryResult
    {
        protected ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
        protected List<long> ids;
        protected Dictionary<string, Dictionary<long, Column>> indeies = new Dictionary<string, Dictionary<long, Column>>();
        protected ICollection collection;

        public BaseQueryResult(ICollection collection)
        {
            this.collection = collection;
        }

        public BaseQueryResult(ICollection collection, string column, List<Column> values)
        {
            this.collection = collection;
            this.ids = values.Select(x => x.Id).ToList();
            this.indeies[column] = values.ToDictionary(x => x.Id, x => x);
        }

        public void And(IQueryResult result)
        {
            if (result == null)
            {
                return;
            }

            if (result is BaseQueryResult other)
            {
                this.rwls.WriteLock(() =>
                {
                    if (other.ids.IsNullOrEmpty())
                    {
                        this.ids.Clear();
                    }
                    else if (this.ids.Any())
                    {
                        this.ids = this.ids.Intersect(other.ids).ToList();
                    }

                    this.indeies.Merge(other.indeies, (d1, d2) =>
                    {
                        d1.Merge(d2, (c1, c2) => c1);
                        return d1;
                    });
                });
            }
        }

        public int Count() => this.ids?.Count ?? 0;

        public bool Delete() => this.collection.Delete(this.ids);

        public void Or(IQueryResult result)
        {
            if (result == null)
            {
                return;
            }

            if (result is BaseQueryResult other)
            {
                this.rwls.WriteLock(() =>
                {
                    this.ids.AddRange(other.ids);
                    this.ids = this.ids.Distinct().ToList();

                    this.indeies.Merge(other.indeies, (d1, d2) =>
                    {
                        d1.Merge(d2, (c1, c2) => c1);
                        return d1;
                    });
                });
            }
        }

        public abstract List<dynamic> Select(params string[] columns);

        public IQueryResult Update(string column, object value)
        {
            this.collection.Update(column, this.ids, value);
            return this;
        }
    }
}
