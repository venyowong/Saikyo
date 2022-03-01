using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IInserter<T>
    {
        long AddData(T t);
    }
}
