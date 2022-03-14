using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal interface IBlock : IDisposable
    {
        long Id { get; }

        int Cap { get; }

        long Offset { get; }

        FixedSizeStreamUnit<long> Next { get; }

        StreamUnit Data { get; }

        Stream Stream { get; }
    }
}
