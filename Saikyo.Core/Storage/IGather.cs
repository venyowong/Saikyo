using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IGather : IDisposable
    {
        string Database { get; }

        string Collection { get; }

        int HeaderSize { get; }

        Stream Stream { get; }

        int BlockSize { get; }

        IBlock GetBlock(long id, bool create = false);

        IBlock GetBlock(long id, object obj, long next = 0);

        long AddData(object obj, long id = 0);

        void Update(long id, object obj);

        bool Delete(long id);

        void Destroy();
    }
}
