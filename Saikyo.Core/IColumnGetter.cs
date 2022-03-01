using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public interface IColumnGetter
    {
        Column GetColumn(long id);
    }
}
