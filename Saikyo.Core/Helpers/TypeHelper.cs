using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Helpers
{
    internal static class TypeHelper
    {
        public static Type GetType(string type)
        {
            switch (type.ToLower())
            {
                case "bool":
                case "boolean":
                    return typeof(bool);
                case "byte":
                    return typeof(byte);
                case "char":
                    return typeof(char);
                case "datetime":
                    return typeof(DateTime);
                case "decimal":
                    return typeof(decimal);
                case "double":
                    return typeof(double);
                case "short":
                case "int16":
                    return typeof(short);
                case "int":
                case "int32":
                    return typeof(int);
                case "long":
                case "int64":
                    return typeof(long);
                case "sbyte":
                    return typeof(sbyte);
                case "float":
                case "single":
                    return typeof(float);
                case "string":
                    return typeof(string);
                case "ushort":
                case "uint16":
                    return typeof(ushort);
                case "uint":
                case "uint32":
                    return typeof(uint);
                case "ulong":
                case "uint64":
                    return typeof(ulong);
                default:
                    return default;
            }
        }
    }
}
