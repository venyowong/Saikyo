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

        public string Database { get; private set; }

        public string Collection { get; private set; }

        public List<DataBlock> Blocks { get; private set; } = new List<DataBlock>();

        /// <summary>
        /// 0 use first block state 1 deleted
        /// </summary>
        private byte state = 0;
        private int blockSize;
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
        private IBlockDeleter blockDeleter;
        private Stream stream;
        private int gatherHeaderSize;

        public Record(string database, string collection, int gatherHeaderSize, Stream stream, long id, int blockSize,
            IBlockDeleter blockDeleter, bool create = false)
        {
            this.Database = database;
            this.Collection = collection;
            this.Id = id;
            this.blockSize = blockSize;
            this.stream = stream;
            this.blockDeleter = blockDeleter;
            this.gatherHeaderSize = gatherHeaderSize;
            long blockId = id;
            do
            {
                var block = new DataBlock(stream, gatherHeaderSize, blockId, blockSize, create);
                if (!this.Blocks.Any() && block.State == 1)
                {
                    this.state = 1;
                    break;
                }
                this.Blocks.Add(block);
                blockId = block.NextBlock;
            }
            while (blockId > 0);
        }

        public Record(string database, string collection, int gatherHeaderSize, Stream stream, int blockSize, byte[] bytes,
            IBlockDeleter blockDeleter, params long[] ids)
        {
            if (ids.IsNullOrEmpty())
            {
                throw new ArgumentOutOfRangeException("ids cannot be null or empty");
            }

            this.Database = database;
            this.Collection = collection;
            this.Id = ids[0];
            this.blockSize = blockSize;
            this.blockDeleter = blockDeleter;
            this.stream = stream;
            this.gatherHeaderSize = gatherHeaderSize;
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
                    this.Blocks.Add(new DataBlock(stream, gatherHeaderSize, ids[i], blockSize, data, ids[i + 1]));
                }
                else
                {
                    this.Blocks.Add(new DataBlock(stream, gatherHeaderSize, ids[i], blockSize, data));
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
                    this.blockDeleter.Delete(b.Id);
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
                    this.Blocks[this.Blocks.Count - 1].UpdateNextBlock(ids[0]);
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
                        this.Blocks.Add(new DataBlock(this.stream, this.gatherHeaderSize, ids[i], this.blockSize, data, ids[i + 1]));
                    }
                    else
                    {
                        this.Blocks.Add(new DataBlock(this.stream, this.gatherHeaderSize, ids[i], blockSize, data));
                    }
                    srcOffset += size;
                }
            }
        }

        public void PushBlock(DataBlock block)
        {
            this.rwls.WriteLock(() =>
            {
                var lastBlock = this.Blocks[this.Blocks.Count - 1];
                lastBlock.UpdateNextBlock(block.Id);
                this.Blocks.Add(block);
            });
        }

        public DataBlock PopBlock()
        {
            return this.rwls.WriteLock(() =>
            {
                if (this.Blocks.Count <= 1)
                {
                    return null;
                }

                this.Blocks[this.Blocks.Count - 2].UpdateNextBlock(0); // blocks[-2] will be tail
                var block = this.Blocks[this.Blocks.Count - 1];
                this.Blocks.Remove(block);
                return block;
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
