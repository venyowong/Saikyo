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
                rwls.TryEnterWriteLock(Instance.Config.ReaderWriterLockTimeout);
                try
                {
                    action();
                }
                finally
                {
                    rwls.ExitWriteLock();
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
                rwls.TryEnterWriteLock(Instance.Config.ReaderWriterLockTimeout);
                try
                {
                    return func();
                }
                finally
                {
                    rwls.ExitWriteLock();
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
                rwls.TryEnterReadLock(Instance.Config.ReaderWriterLockTimeout);
                try
                {
                    return func();
                }
                finally
                {
                    rwls.ExitReadLock();
                }
            }
            catch (ApplicationException)
            {
                return default;
            }
        }
    }
}
