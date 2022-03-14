using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Query
{
    public interface IQueryResult
    {
        List<long> Ids { get; }

        Dictionary<string, Dictionary<long, Column>> Indeies { get; }

        void And(IQueryResult result);

        void Or(IQueryResult result);

        List<dynamic> Select(params string[] columns);

        int Count();

        bool Delete();

        IQueryResult Update(string column, object value);
    }
}
