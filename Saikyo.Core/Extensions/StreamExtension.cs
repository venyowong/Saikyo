using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Saikyo.Core.Extensions
{
    internal static class StreamExtension
    {
        private static ConcurrentDictionary<Stream, ReaderWriterLockSlim> _locks = new ConcurrentDictionary<Stream, ReaderWriterLockSlim>();

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
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            return rwls.ReadLock(() =>
            {
                if (stream.CanRead)
                {
                    stream.Position = position;
                    var bytes = new byte[4];
                    stream.Read(bytes, 0, 4);
                    return BitConverter.ToInt32(bytes, 0);
                }
                else
                {
                    return default;
                }
            });
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
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            return rwls.ReadLock(() =>
            {
                if (stream.CanRead)
                {
                    stream.Position = position;
                    var bytes = new byte[8];
                    stream.Read(bytes, 0, 8);
                    return BitConverter.ToInt64(bytes, 0);
                }
                else
                {
                    return default;
                }
            });
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
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            return rwls.ReadLock(() =>
            {
                if (stream.CanRead)
                {
                    stream.Position = position;
                    var bytes = new byte[1];
                    stream.Read(bytes, 0, 1);
                    return bytes[0];
                }
                else
                {
                    return default;
                }
            });
        }

        public static byte[] Read(this Stream stream, long position, int size)
        {
            if (stream == null || position < 0 || size <= 0)
            {
                return new byte[0];
            }
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            return rwls.ReadLock(() =>
            {
                if (stream.CanRead)
                {
                    stream.Position = position;
                    var bytes = new byte[size];
                    stream.Read(bytes, 0, size);
                    return bytes;
                }
                else
                {
                    return default;
                }
            });
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
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            rwls.WriteLock(() =>
            {
                if (stream.CanWrite)
                {
                    stream.Position = position;
                    stream.Write(bytes, 0, bytes.Length);
                }
            });
        }

        public static void CloseSafely(this Stream stream)
        {
            if (stream == null)
            {
                return;
            }
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            rwls.WriteLock(() =>
            {
                if (stream.CanRead)
                {
                    stream.Close();
                }
            });
        }

        public static void FlushSafely(this Stream stream)
        {
            if (stream == null)
            {
                return;
            }
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            rwls.WriteLock(() =>
            {
                if (stream.CanWrite)
                {
                    stream.Flush();
                }
            });
        }

        public static void DisposeSafely(this Stream stream)
        {
            if (stream == null)
            {
                return;
            }
            ReaderWriterLockSlim rwls;
            if (_locks.ContainsKey(stream))
            {
                rwls = _locks[stream];
            }
            else
            {
                lock (_locks)
                {
                    if (_locks.ContainsKey(stream))
                    {
                        rwls = _locks[stream];
                    }
                    else
                    {
                        rwls = new ReaderWriterLockSlim();
                        _locks.TryAdd(stream, rwls);
                    }
                }
            }

            rwls.WriteLock(() =>
            {
                if (stream.CanWrite)
                {
                    stream.Dispose();
                }
            });
        }
    }
}
