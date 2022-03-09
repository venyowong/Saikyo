using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Query
{
    public interface IQueryBuilder
    {
        IQueryResult Build();
    }
}
