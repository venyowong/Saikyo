using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Saikyo.Core.Extensions
{
    internal static class StreamExtension
    {
        public static int ReadAsInt32(this Stream stream, long position)
        {
            if (stream == null)
            {
                return int.MinValue;
            }
            if (position < 0)
            {
                return int.MinValue;
            }    

            stream.Position = position;
            var bytes = new byte[4];
            stream.Read(bytes, 0, 4);
            return BitConverter.ToInt32(bytes, 0);
        }

        public static long ReadAsLong(this Stream stream, long position)
        {
            if (stream == null)
            {
                return long.MinValue;
            }
            if (position < 0)
            {
                return long.MinValue;
            }

            stream.Position = position;
            var bytes = new byte[8];
            stream.Read(bytes, 0, 8);
            return BitConverter.ToInt64(bytes, 0);
        }

        public static byte ReadAsByte(this Stream stream, long position)
        {
            if (stream == null)
            {
                return byte.MinValue;
            }
            if (position < 0)
            {
                return byte.MinValue;
            }

            stream.Position = position;
            var bytes = new byte[1];
            stream.Read(bytes, 0, 1);
            return bytes[0];
        }

        public static byte[] Read(this Stream stream, long position, int size)
        {
            if (stream == null || position < 0 || size <= 0)
            {
                return new byte[0];
            }

            stream.Position = position;
            var bytes = new byte[size];
            stream.Read(bytes, 0, size);
            return bytes;
        }

        public static void Write(this Stream stream, long position, byte[] bytes)
        {
            if (stream == null)
            {
                return;
            }
            if (position < 0)
            {
                return;
            }

            stream.Position = position;
            stream.Write(bytes, 0, bytes.Length);
        }
    }
}
