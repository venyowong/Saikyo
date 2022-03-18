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
            rwls.EnterWriteLock();
            try
            {
                action();
            }
            finally
            {
                rwls.ExitWriteLock();
            }
        }

        public static T WriteLock<T>(this ReaderWriterLockSlim rwls, Func<T> func)
        {
            rwls.EnterWriteLock();
            try
            {
                return func();
            }
            finally
            {
                rwls.ExitWriteLock();
            }
        }

        public static T ReadLock<T>(this ReaderWriterLockSlim rwls, Func<T> func)
        {
            rwls.EnterReadLock();
            try
            {
                return func();
            }
            finally
            {
                rwls.ExitReadLock();
            }
        }
    }
}
