using Saikyo.Core.Storage.Blocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage.Records
{
    internal class ChainRecord : IChainRecord
    {
        public List<IBlock> Blocks { get; protected set; } = new List<IBlock>();

        public long Id { get; protected set; }

        public IBlock Head => this.Blocks.FirstOrDefault();

        public Stream Stream { get; protected set; }

        public int Offset { get; protected set; }

        public int BlockCap { get; protected set; }

        public ChainRecord(Stream stream, long id, int offset, int blockCap, bool ignoreData = false)
        {
            this.Stream = stream;
            this.Id = id;
            this.Offset = offset;
            this.BlockCap = blockCap;
            var blockId = id;
            do
            {
                var block = new Block(stream, blockId, blockCap, offset, ignoreData);
                this.Blocks.Add(block);
                blockId = block.Next.Value;
            }
            while (blockId > 0);
        }

        public void Dispose() => this.Blocks.ForEach(x => x.Dispose());

        public byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }
}
