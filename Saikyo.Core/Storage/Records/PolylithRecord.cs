using Saikyo.Core.Extensions;
using Saikyo.Core.Storage.Blocks;
using Saikyo.Core.Storage.Gathers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal class PolylithRecord : IPolylithRecord
    {
        public List<IBlock> Blocks { get; protected set; } = new List<IBlock>();

        public long Id { get; protected set; }

        public IBlock Head => this.Blocks.FirstOrDefault();

        public Stream Stream { get; protected set; }

        public int Offset { get; protected set; }

        public int BlockCap { get; protected set; }

        public IGather Gather { get; private set; }

        public PolylithRecord(long id, IGather gather)
        {
            this.Stream = gather.Stream;
            this.Id = id;
            this.Offset = gather.HeaderSize;
            this.BlockCap = gather.BlockCap.Value;
            var blockId = id;
            do
            {
                var block = new SizeBlock(gather.Stream, blockId, this.BlockCap, this.Offset);
                this.Blocks.Add(block);
                blockId = block.Next.Value;
            }
            while (blockId > 0);
            this.Gather = gather;
        }

        public PolylithRecord(long id, IGather gather, byte[] bytes)
        {
            this.Stream = gather.Stream;
            this.Id = id;
            this.Offset = gather.HeaderSize;
            this.BlockCap = gather.BlockCap.Value;
            this.Gather = gather;

            var blockCount = Math.Ceiling((double)bytes.Length / (this.BlockCap - SizeBlock.HeaderSize));
            int srcOffset = 0;
            for (int i = 0; i < blockCount; i++)
            {
                var blockId = id;
                if (i > 0)
                {
                    blockId = this.Gather.GetFreeBlockId();
                }
                var size = this.BlockCap - SizeBlock.HeaderSize;
                if (i == blockCount - 1)
                {
                    size = bytes.Length - srcOffset;
                }
                var data = new byte[size];
                Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                if (this.Blocks.Any())
                {
                    this.Blocks[i - 1].Next.Update(blockId);
                }
                this.Blocks.Add(new SizeBlock(gather.Stream, blockId, this.BlockCap, this.Offset, data));
                srcOffset += size;
            }
        }

        public void Update(byte[] bytes)
        {
            var blockCount = Math.Ceiling((double)bytes.Length / (this.BlockCap - SizeBlock.HeaderSize));
            int srcOffset = 0;
            foreach (var b in this.Blocks)
            {
                var size = Math.Min(this.BlockCap - SizeBlock.HeaderSize, bytes.Length - srcOffset);
                if (size <= 0)
                {
                    this.Gather.Delete(b.Id);
                    continue;
                }

                var data = new byte[size];
                Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                b.Data.Update(data);
                srcOffset += size;
            }
            this.Blocks = this.Blocks.Take((int)blockCount).ToList();

            if (blockCount > this.Blocks.Count)
            {
                for (int i = 0; i < blockCount - this.Blocks.Count; i++)
                {
                    var blockId = this.Gather.GetFreeBlockId();
                    var size = this.BlockCap - SizeBlock.HeaderSize;
                    if (i == blockCount - 1)
                    {
                        size = bytes.Length - srcOffset;
                    }
                    var data = new byte[size];
                    Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                    if (this.Blocks.Any())
                    {
                        this.Blocks[i - 1].Next.Update(blockId);
                    }
                    this.Blocks.Add(new SizeBlock(this.Stream, blockId, this.BlockCap, this.Offset, data));
                    srcOffset += size;
                }
            }
        }

        public void Dispose() => this.Blocks.ForEach(x => x.Dispose());

        public byte[] GetBytes()
        {
            var bytes = new List<byte>();
            this.Blocks.ForEach(b => bytes.AddRange(b.Data.Data));
            return bytes.ToArray();
        }
    }
}
