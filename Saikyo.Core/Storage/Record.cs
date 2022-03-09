using Saikyo.Core.Extensions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal class Record : IDisposable
    {
        public long Id { get; private set; }

        public List<IBlock> Blocks { get; private set; } = new List<IBlock>();

        /// <summary>
        /// 0 use first block state 1 deleted
        /// </summary>
        private byte state = 0;
        private int blockSize;
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
        private IGather gather;
        private Stream stream;
        private int gatherHeaderSize;

        public Record(IGather gather, long id, bool create = false)
        {
            this.Id = id;
            this.blockSize = gather.BlockSize;
            this.stream = gather.Stream;
            this.gather = gather;
            this.gatherHeaderSize = gather.HeaderSize;
            long blockId = id;
            do
            {
                var block = this.gather.GetBlock(blockId, create);
                if (!this.Blocks.Any() && block.State == 1)
                {
                    this.state = 1;
                    break;
                }
                this.Blocks.Add(block);
                blockId = block.Next;
            }
            while (blockId > 0);
        }

        public Record(IGather gather, byte[] bytes, params long[] ids)
        {
            if (ids.IsNullOrEmpty())
            {
                throw new ArgumentOutOfRangeException("ids cannot be null or empty");
            }

            this.Id = ids[0];
            this.blockSize = gather.BlockSize;
            this.gather = gather;
            this.stream = gather.Stream;
            this.gatherHeaderSize = gather.HeaderSize;
            int srcOffset = 0;
            for (var i = 0; i < ids.Length; i++)
            {
                var size = blockSize - Const.DataBlockHeaderSize;
                if (i == ids.Length - 1)
                {
                    size = bytes.Length - srcOffset;
                }
                var data = new byte[size];
                Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                if (i < ids.Length - 1)
                {
                    this.Blocks.Add(this.gather.GetBlock(ids[i], data, ids[i + 1]));
                }
                else
                {
                    this.Blocks.Add(this.gather.GetBlock(ids[i], data));
                }
                srcOffset += size;
            }
        }

        public byte GetState()
        {
            if (this.state == 0)
            {
                return this.Blocks.First().State;
            }

            return this.state;
        }

        public byte[] GetBytes()
        {
            if (this.state != 0)
            {
                return new byte[0];
            }

            var length = this.Blocks.Sum(b => b.DataSize);
            var bytes = new byte[length];
            var offset = 0;
            foreach (var b in this.Blocks)
            {
                Buffer.BlockCopy(b.Data, 0, bytes, offset, b.DataSize);
                offset += b.DataSize;
            }
            return bytes;
        }

        public void Update(byte[] bytes, long[] ids)
        {
            int srcOffset = 0;
            foreach (var b in this.Blocks)
            {
                var size = Math.Min(this.blockSize - Const.DataBlockHeaderSize, bytes.Length - srcOffset);
                if (size <= 0)
                {
                    this.gather.Delete(b.Id);
                    continue;
                }

                var data = new byte[size];
                Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                b.Update(data);
                srcOffset += size;
            }

            if (!ids.IsNullOrEmpty())
            {
                if (this.Blocks.Any())
                {
                    this.Blocks[this.Blocks.Count - 1].Next = ids[0];
                }

                for (var i = 0; i < ids.Length; i++)
                {
                    var size = this.blockSize - Const.DataBlockHeaderSize;
                    if (i == ids.Length - 1)
                    {
                        size = bytes.Length - srcOffset;
                    }
                    var data = new byte[size];
                    Buffer.BlockCopy(bytes, srcOffset, data, 0, size);
                    if (i < ids.Length - 1)
                    {
                        this.Blocks.Add(this.gather.GetBlock(ids[i], data, ids[i + 1]));
                    }
                    else
                    {
                        this.Blocks.Add(this.gather.GetBlock(ids[i], data));
                    }
                    srcOffset += size;
                }
            }
        }

        public void PushBlock(IBlock block)
        {
            this.rwls.WriteLock(() =>
            {
                var lastBlock = this.Blocks[this.Blocks.Count - 1];
                lastBlock.Next = block.Id;
                this.Blocks.Add(block);
            });
        }

        public IBlock PopBlock(long id = 0)
        {
            return this.rwls.WriteLock(() =>
            {
                if (this.Blocks.Count <= 1)
                {
                    return null;
                }

                if (id == 0)
                {
                    this.Blocks[this.Blocks.Count - 2].Next = 0; // blocks[-2] will be tail
                    var block = this.Blocks[this.Blocks.Count - 1];
                    this.Blocks.Remove(block);
                    return block;
                }
                else
                {
                    for (int i = 1; i < this.Blocks.Count; i++)
                    {
                        var block = this.Blocks[i];
                        if (block.Id != id)
                        {
                            continue;
                        }

                        if (i + 1 < this.Blocks.Count) // if this block is not tail
                        {
                            this.Blocks[i - 1].Next = this.Blocks[i + 1].Id; // [i - 1] -> [i + 1], remove this block from the linked list
                        }
                        else
                        {
                            this.Blocks[i - 1].Next = 0; // [i - 1] will be tail
                        }
                        this.Blocks.Remove(block);
                        return block;
                    }
                    return null;
                }
            });
        }

        public void Dispose()
        {
            foreach (var block in this.Blocks)
            {
                block.Dispose();
            }
            this.rwls.Dispose();
        }
    }
}
