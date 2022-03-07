using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Extensions
{
    internal static class StringExtension
    {
        public static T FromString<T>(this string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return default;
            }

            return (T)FromString(str, typeof(T));
        }

        public static dynamic FromString(this string str, Type type)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return default;
            }

            var code = Type.GetTypeCode(type);
            switch (code)
            {
                case TypeCode.Boolean:
                    if (!bool.TryParse(str, out bool b))
                    {
                        throw new InvalidCastException($"{str} cannot parse to bool");
                    }
                    return b;
                case TypeCode.Byte:
                    if (!byte.TryParse(str, out byte by))
                    {
                        throw new InvalidCastException($"{str} cannot parse to byte");
                    }
                    return by;
                case TypeCode.Char:
                    return str[0];
                case TypeCode.DateTime:
                    if (!DateTime.TryParse(str, out DateTime time))
                    {
                        throw new InvalidCastException($"{str} cannot parse to DateTime");
                    }
                    return time;
                case TypeCode.Decimal:
                    if (!decimal.TryParse(str, out decimal de))
                    {
                        throw new InvalidCastException($"{str} cannot parse to decimal");
                    }
                    return de;
                case TypeCode.Double:
                    if (!double.TryParse(str, out double d))
                    {
                        throw new InvalidCastException($"{str} cannot parse to double");
                    }
                    return d;
                case TypeCode.Int16:
                    if (!short.TryParse(str, out short s))
                    {
                        throw new InvalidCastException($"{str} cannot parse to short");
                    }
                    return s;
                case TypeCode.Int32:
                    if (!int.TryParse(str, out int i))
                    {
                        throw new InvalidCastException($"{str} cannot parse to int");
                    }
                    return i;
                case TypeCode.Int64:
                    if (!long.TryParse(str, out long l))
                    {
                        throw new InvalidCastException($"{str} cannot parse to long");
                    }
                    return l;
                case TypeCode.SByte:
                    if (!sbyte.TryParse(str, out sbyte sb))
                    {
                        throw new InvalidCastException($"{str} cannot parse to sbyte");
                    }
                    return sb;
                case TypeCode.Single:
                    if (!float.TryParse(str, out float f))
                    {
                        throw new InvalidCastException($"{str} cannot parse to float");
                    }
                    return f;
                case TypeCode.String:
                    return str;
                case TypeCode.UInt16:
                    if (!ushort.TryParse(str, out ushort us))
                    {
                        throw new InvalidCastException($"{str} cannot parse to ushort");
                    }
                    return us;
                case TypeCode.UInt32:
                    if (!uint.TryParse(str, out uint ui))
                    {
                        throw new InvalidCastException($"{str} cannot parse to uint");
                    }
                    return ui;
                case TypeCode.UInt64:
                    if (!ulong.TryParse(str, out ulong ul))
                    {
                        throw new InvalidCastException($"{str} cannot parse to ulong");
                    }
                    return ul;
                default:
                    return default;
            }
        }
    }
}
