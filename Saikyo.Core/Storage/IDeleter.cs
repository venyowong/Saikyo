using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IDeleter
    {
        bool Delete(long id);
    }
}
