using Saikyo.Core.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public interface ICollection : IDisposable
    {
        string Database { get; }

        string Name { get; }

        string Key { get; }

        bool Disposed { get; }

        Dictionary<string, dynamic> ColumnGathers { get; }

        Type GetPropertyType(string column);

        IQueryBuilder Query(string condition = null);

        void Drop();
    }

    public interface IDynamicCollection : ICollection
    {
        IDynamicCollection SetProperty(string property, Type type, int size = 0, bool key = false);

        IDynamicCollection SetProperty<T>(string property, int size = 0, bool key = false);
    }
}
