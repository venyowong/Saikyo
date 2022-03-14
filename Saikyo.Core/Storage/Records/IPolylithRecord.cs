using Saikyo.Core.Storage.Gathers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal interface IPolylithRecord : IChainRecord
    {
        IGather Gather { get; }

        void Update(byte[] bytes);
    }
}
