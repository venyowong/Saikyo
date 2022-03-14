using Saikyo.Core.Storage.Blocks;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal interface IChainRecord : IRecord
    {
        List<IBlock> Blocks { get; }
    }
}
