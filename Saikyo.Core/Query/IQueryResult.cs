using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Query
{
    public interface IQueryResult
    {
        IEnumerable<long> Ids { get; }

        Dictionary<string, Dictionary<long, Column>> Indeies { get; }

        void And(IQueryResult result);

        void Or(IQueryResult result);

        IQueryResult Skip(int count);

        IQueryResult Take(int count);

        List<dynamic> Select(params string[] columns);

        int Count();

        bool Delete();

        IQueryResult Update(string column, object value);
    }
}
