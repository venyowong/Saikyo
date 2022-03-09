using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public interface IAVLTree
    {
        List<Column> Gt(object obj);

        List<Column> Gte(object obj);

        List<Column> Lt(object obj);

        List<Column> Lte(object obj);

        List<Column> Eq(object obj);

        List<Column> GetAllColumns();
    }
}
