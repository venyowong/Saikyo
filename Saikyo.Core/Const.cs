using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    internal static class Const
    {
        public const int BaseBlockHeaderSize = 5;
        public static readonly int AVLBlockHeaderSize = BaseBlockHeaderSize + 26;
        public static readonly int DataBlockHeaderSize = BaseBlockHeaderSize + 8;
    }
}
