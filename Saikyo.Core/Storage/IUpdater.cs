using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IUpdater<T>
    {
        void Update(long id, T t);
    }
}
