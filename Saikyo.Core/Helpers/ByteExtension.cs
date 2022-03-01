using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Saikyo.Core.Helpers
{
    internal static class ByteExtension
    {
        public static byte[] ToBytes(this object obj)
        {
            if (obj is bool b)
            {
                return BitConverter.GetBytes(b);
            }
            if (obj is char c)
            {
                return BitConverter.GetBytes(c);
            }
            if (obj is double d)
            {
                return BitConverter.GetBytes(d);
            }
            if (obj is short s)
            {
                return BitConverter.GetBytes(s);
            }
            if (obj is int i)
            {
                return BitConverter.GetBytes(i);
            }
            if (obj is long l)
            {
                return BitConverter.GetBytes(l);
            }
            if (obj is float f)
            {
                return BitConverter.GetBytes(f);
            }
            if (obj is ushort us)
            {
                return BitConverter.GetBytes(us);
            }
            if (obj is uint ui)
            {
                return BitConverter.GetBytes(ui);
            }
            if (obj is ulong ul)
            {
                return BitConverter.GetBytes(ul);
            }
            if (obj is string str)
            {
                return Encoding.UTF8.GetBytes(str);
            }
            if (obj is DateTime dt)
            {
                return BitConverter.GetBytes(dt.Ticks);
            }
            if (obj is decimal de)
            {
                return de.ToBytes();
            }
            if (obj is sbyte sb)
            {
                return BitConverter.GetBytes(sb);
            }
            return null;
        }

        public static byte[] ToBytes(this decimal de)
        {
            using (var stream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(stream, de);
                return stream.ToArray();
            }
        }

        public static T FromBytes<T>(this byte[] bytes)
        {
            if (bytes.IsNullOrEmpty())
            {
                return default;
            }

            var code = Type.GetTypeCode(typeof(T));
            switch (code)
            {
                case TypeCode.Boolean:
                    return (T)(object)BitConverter.ToBoolean(bytes, 0);
                case TypeCode.Byte:
                    return (T)(object)bytes[0];
                case TypeCode.Char:
                    return (T)(object)BitConverter.ToChar(bytes, 0);
                case TypeCode.DateTime:
                    return (T)(object)DateTime.FromBinary(BitConverter.ToInt64(bytes, 0));
                case TypeCode.Decimal:
                    return (T)(object)bytes.ToDecimal();
                case TypeCode.Double:
                    return (T)(object)BitConverter.ToDouble(bytes, 0);
                case TypeCode.Int16:
                    return (T)(object)BitConverter.ToInt16(bytes, 0);
                case TypeCode.Int32:
                    return (T)(object)BitConverter.ToInt32(bytes, 0);
                case TypeCode.Int64:
                    return (T)(object)BitConverter.ToInt64(bytes, 0);
                case TypeCode.SByte:
                    return (T)(object)(sbyte)bytes[0];
                case TypeCode.Single:
                    return (T)(object)BitConverter.ToSingle(bytes, 0);
                case TypeCode.String:
                    return (T)(object)Encoding.UTF8.GetString(bytes);
                case TypeCode.UInt16:
                    return (T)(object)BitConverter.ToUInt16(bytes, 0);
                case TypeCode.UInt32:
                    return (T)(object)BitConverter.ToUInt32(bytes, 0);
                case TypeCode.UInt64:
                    return (T)(object)BitConverter.ToUInt64(bytes, 0);
                default:
                    return default;
            }
        }

        public static decimal ToDecimal(this byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return (decimal)new BinaryFormatter().Deserialize(stream);
            }
        }
    }
}
