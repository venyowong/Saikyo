using Saikyo.Core.Exceptions;
using Saikyo.Core.Extensions;
using Saikyo.Core.Storage;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Saikyo.Core.Query
{
    public class QueryBuilder : BaseQueryBuilder
    {
        internal QueryBuilder(Collection collection, string key, Symbol ope, string value) : base(collection, key, ope, value)
        {
        }

        public QueryBuilder(Collection collection, string condition) : base(collection, condition)
        {
        }

        internal override IQueryResult CreateResult() => new QueryResult((Collection)this.collection);

        internal override IQueryResult CreateResult(string column, List<Column> values) => new QueryResult((Collection)this.collection, column, values);

        internal override IQueryBuilder CreateSubQuery(string key, Symbol ope, string value) => new QueryBuilder((Collection)this.collection, key, ope, value);

        internal override IQueryBuilder CreateSubQuery(string condition) => new QueryBuilder((Collection)this.collection, condition);
    }

    public class QueryBuilder<T> : BaseQueryBuilder where T : new()
    {
        internal QueryBuilder(Collection<T> collection, string key, Symbol ope, string value) : base(collection, key, ope, value)
        {
        }

        public QueryBuilder(Collection<T> collection, string condition) : base(collection, condition)
        {
        }

        internal override IQueryResult CreateResult() => new QueryResult<T>((Collection<T>)this.collection);

        internal override IQueryResult CreateResult(string column, List<Column> values) => new QueryResult<T>((Collection<T>)this.collection, column, values);

        internal override IQueryBuilder CreateSubQuery(string key, Symbol ope, string value) => new QueryBuilder<T>((Collection<T>)this.collection, key, ope, value);

        internal override IQueryBuilder CreateSubQuery(string condition) => new QueryBuilder<T>((Collection<T>)this.collection, condition);
    }
}
