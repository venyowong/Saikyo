using Serilog;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Extensions
{
    internal static class LockerExtension
    {
        public static void WriteLock(this ReaderWriterLockSlim rwls, Action action)
        {
            try
            {
                if (rwls.TryEnterWriteLock(Instance.Config.ReaderWriterLockTimeout))
                {
                    try
                    {
                        action();
                    }
                    finally
                    {
                        rwls.ExitWriteLock();
                    }
                }
                else
                {
                    Log.Warning("failed to enter write lock");
                }
            }
            catch (ApplicationException)
            {
            }
        }

        public static T WriteLock<T>(this ReaderWriterLockSlim rwls, Func<T> func)
        {
            try
            {
                if (rwls.TryEnterWriteLock(Instance.Config.ReaderWriterLockTimeout))
                {
                    try
                    {
                        return func();
                    }
                    finally
                    {
                        rwls.ExitWriteLock();
                    }
                }
                else
                {
                    Log.Warning("failed to enter write lock");
                    return default;
                }
            }
            catch (ApplicationException)
            {
                return default;
            }
        }

        public static T ReadLock<T>(this ReaderWriterLockSlim rwls, Func<T> func)
        {
            try
            {
                if (rwls.TryEnterReadLock(Instance.Config.ReaderWriterLockTimeout))
                {
                    try
                    {
                        return func();
                    }
                    finally
                    {
                        rwls.ExitReadLock();
                    }
                }
                else
                {
                    Log.Warning("failed to enter read lock");
                    return default;
                }
            }
            catch (ApplicationException)
            {
                return default;
            }
        }
    }
}
