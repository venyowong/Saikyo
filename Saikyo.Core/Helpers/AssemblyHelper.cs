using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Helpers
{
    internal static class AssemblyHelper
    {
        public static Type GetType(string fullName)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var type = assembly.GetType(fullName);
                if (type != null)
                {
                    return type;
                }
            }

            return null;
        }
    }
}
