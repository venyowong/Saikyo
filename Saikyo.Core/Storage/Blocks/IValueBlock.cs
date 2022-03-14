using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal interface IValueBlock<T> : IBlock
    {
        T Value { get; }

        void Update(T value);
    }
}
