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

        public static int GetTypeSize(Type type)
        {
            var code = Type.GetTypeCode(type);
            switch (code)
            {
                case TypeCode.Boolean:
                case TypeCode.SByte:
                case TypeCode.Byte:
                    return 1;
                case TypeCode.Int16:
                case TypeCode.UInt16:
                    return 2;
                case TypeCode.Char:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Single:
                    return 4;
                case TypeCode.DateTime:
                case TypeCode.Double:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return 8;
                case TypeCode.Decimal:
                    return 200;
                default:
                    throw new NotSupportedException($"cannot determine the size of {type.FullName}");
            }
        }
    }
}
