using Saikyo.Core.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public interface ICollection : IDisposable
    {
        IQueryBuilder Query(string condition = null);

        bool Delete(List<long> ids);

        void Update(string column, List<long> ids, object value);

        void Drop();
    }
}
