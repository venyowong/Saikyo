using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal interface ISizeBlock : IBlock
    {
        FixedSizeStreamUnit<int> DataSize { get; }
    }
}
