using Saikyo.Core.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Query
{
    public class QueryResult<T> where T : new()
    {
        private Collection<T> collection;
        private List<long> ids;
        private Dictionary<string, Dictionary<long, Column>> indeies = new Dictionary<string, Dictionary<long, Column>>();
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public QueryResult(Collection<T> collection) 
        { 
            this.collection = collection;
        }

        public QueryResult(Collection<T> collection, string column, List<Column> values)
        {
            this.collection = collection;
            this.ids = values.Select(x => x.Id).ToList();
            this.indeies[column] = values.ToDictionary(x => x.Id, x => x);
        }

        public void And(QueryResult<T> other)
        {
            if (other == null)
            {
                return;
            }

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

        public void Or(QueryResult<T> other)
        {
            if (other == null)
            {
                return;
            }

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

        public List<T> Select(params string[] columns) => this.collection.Compose(this.ids, this.indeies, columns);

        public bool Delete() => this.collection.Delete(this.ids);
    }
}
