using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IAVLTree<T> where T : IComparable<T>
    {
        long Root { get; set; }

        IAVLNode<T> GetNode(long id);

        Column GetColumn(long id);

        List<Column> GetAllColumns();

        List<Column> Gt(T t);

        List<Column> Gte(T t);

        List<Column> Lt(T t);

        List<Column> Lte(T t);

        List<Column> Eq(T t);
    }
}
