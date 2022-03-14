using Saikyo.Core.Extensions;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Saikyo.Core.Query
{
    public class QueryResult : IQueryResult
    {
        public List<long> Ids { get; private set; }

        public Dictionary<string, Dictionary<long, Column>> Indeies { get; private set; } = new Dictionary<string, Dictionary<long, Column>>();

        private ICollection collection;

        internal QueryResult(ICollection collection = null)
        {
            this.Ids = new List<long>();
            this.collection = collection;
        }

        internal QueryResult(ICollection collection, string column, List<Column> values)
        {
            this.collection = collection;
            this.Ids = values.Select(x => x.Id).ToList();
            this.Indeies[column] = values.ToDictionary(x => x.Id, x => x);
        }

        public void And(IQueryResult other)
        {
            if (other == null)
            {
                return;
            }

            if (other.Ids.IsNullOrEmpty())
            {
                this.Ids.Clear();
            }
            else if (this.Ids.Any())
            {
                this.Ids = this.Ids.Intersect(other.Ids).ToList();
            }

            this.Indeies.Merge(other.Indeies, (d1, d2) =>
            {
                d1.Merge(d2, (c1, c2) => c1);
                return d1;
            });
        }

        public int Count() => this.Ids.Count;

        public bool Delete() => this.collection?.Delete(this.Ids) ?? false;

        public void Or(IQueryResult other)
        {
            if (other == null)
            {
                return;
            }

            this.Ids.AddRange(other.Ids);
            this.Ids = this.Ids.Distinct().ToList();

            this.Indeies.Merge(other.Indeies, (d1, d2) =>
            {
                d1.Merge(d2, (c1, c2) => c1);
                return d1;
            });
        }

        public List<dynamic> Select(params string[] columns) => this.collection.Compose(this.Ids, this.Indeies, columns);

        public IQueryResult Update(string column, object value)
        {
            this.collection.Update(column, this.Ids, value);
            return this;
        }
    }
}
