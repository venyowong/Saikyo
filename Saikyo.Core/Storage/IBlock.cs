using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IBlock : IDisposable
    {
        long Id { get; }

        int HeaderSize { get; }

        int DataSize { get; }

        /// <summary>
        /// 0 init 1 deleted
        /// </summary>
        byte State { get; }

        long Next { get; set; }

        byte[] Data { get; }

        void Update(object data);

        void MarkAsDeleted();
    }
}
