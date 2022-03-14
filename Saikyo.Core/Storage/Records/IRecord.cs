using Saikyo.Core.Storage.Blocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal interface IRecord : IDisposable
    {
        long Id { get; }

        IBlock Head { get; }

        Stream Stream { get; }

        int Offset { get; }

        int BlockCap { get; }
    }
}
