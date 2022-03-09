using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    internal static class Const
    {
        public const int BaseBlockHeaderSize = 13;
        public const int DataBlockHeaderSize = BaseBlockHeaderSize;
        public static readonly int AVLBlockHeaderSize = BaseBlockHeaderSize + 18;
    }
}
