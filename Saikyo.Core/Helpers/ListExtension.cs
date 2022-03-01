using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Helpers
{
    internal static class ListExtension
    {
        public static bool IsNullOrEmpty<T>(this IEnumerable<T> list) => list == null || list.Count() <= 0;
    }
}
