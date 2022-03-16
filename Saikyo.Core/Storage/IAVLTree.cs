using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IAVLTree<T> : IColumnGetter where T : IComparable<T>
    {
        long Root { get; set; }

        IAVLNode<T> GetNode(long id);

        List<Column> GetAllColumns();

        List<Column> Gt(T t);

        List<Column> Gte(T t);

        List<Column> Lt(T t);

        List<Column> Lte(T t);

        List<Column> Eq(T t);
    }
}
