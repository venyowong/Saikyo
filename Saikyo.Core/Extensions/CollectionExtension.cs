using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Extensions
{
    internal static class CollectionExtension
    {
        public static bool IsNullOrEmpty<T>(this IEnumerable<T> list) => list == null || list.Count() <= 0;

        public static void Merge<T1, T2>(this Dictionary<T1, T2> dict1, Dictionary<T1, T2> dict2, Func<T2, T2, T2> conflictHandler)
        {
            if (dict1 == null || dict2 == null || conflictHandler == null)
            {
                return;
            }

            foreach (var item in dict2)
            {
                if (!dict1.ContainsKey(item.Key))
                {
                    dict2[item.Key] = item.Value;
                    continue;
                }

                dict1[item.Key] = conflictHandler(dict1[item.Key], item.Value);
            }
        }
    }
}
