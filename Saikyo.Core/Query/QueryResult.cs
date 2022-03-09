using Saikyo.Core.Extensions;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Saikyo.Core.Query
{
    public class QueryResult : BaseQueryResult
    {
        public QueryResult(Collection collection) : base(collection)
        {
        }

        public QueryResult(Collection collection, string column, List<Column> values) : base(collection, column, values)
        {
        }

        public override List<dynamic> Select(params string[] columns) => ((Collection)this.collection).Compose(this.ids, this.indeies, columns);
    }

    public class QueryResult<T> : BaseQueryResult where T : new()
    {
        public QueryResult(Collection<T> collection) : base(collection)
        { 
            this.collection = collection;
        }

        public QueryResult(Collection<T> collection, string column, List<Column> values) : base(collection, column, values)
        {
            this.collection = collection;
            this.ids = values.Select(x => x.Id).ToList();
            this.indeies[column] = values.ToDictionary(x => x.Id, x => x);
        }

        public override List<dynamic> Select(params string[] columns) => ((Collection<T>)this.collection).Compose(this.ids, this.indeies, columns);
    }
}
