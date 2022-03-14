using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Saikyo.Core.Helpers
{
    internal static class ReflectionHelper
    {
        private static ConcurrentDictionary<string, Dictionary<string, PropertyInfo>> _properties = 
            new ConcurrentDictionary<string, Dictionary<string, PropertyInfo>>();

        public static object GetValue(object obj, string property)
        {
            if (obj is IDictionary<string, object> dict)
            {
                return dict[property];
            }
            else
            {
                var type = obj.GetType();
                Dictionary<string, PropertyInfo> properties;
                if (_properties.ContainsKey(type.FullName))
                {
                    properties = _properties[type.FullName];
                }
                else
                {
                    properties = type.GetProperties().ToDictionary(x => x.Name);
                    _properties.TryAdd(type.FullName, properties);
                }

                return properties[property].GetValue(obj);
            }
        }
    }
}
