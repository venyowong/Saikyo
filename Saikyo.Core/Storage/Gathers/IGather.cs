using Saikyo.Core.Storage.Records;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Gathers
{
    internal interface IGather : IHeaderSize, IDisposable
    {
        string Name { get; }

        FileInfo File { get; }

        Stream Stream { get; }

        FixedSizeStreamUnit<long> LatestBlockId { get; }

        FixedSizeStreamUnit<int> BlockCap { get; }

        IChainRecord UnusedBlocks { get; }

        ConcurrentDictionary<long, IRecord> Records { get; }

        long AddData(byte[] data, long id = 0);

        IRecord GetRecord(long id);

        void Destroy();
    }

    internal interface IGather<T> : IGather
    {
        long AddData(T data, long id = 0);
    }
}
