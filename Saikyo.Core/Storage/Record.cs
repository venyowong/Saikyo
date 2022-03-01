using Saikyo.Core.Helpers;
using Saikyo.Core.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal class Record : IDisposable
    {
        public long Id { get; private set; }

        public string Database { get; private set; }

        public string Collection { get; private set; }

        private List<DataBlock> blocks = new List<DataBlock>();
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public Record(string database, string collection, int gatherHeaderSize, Stream stream, long id, int blockSize, bool create = false)
        {
            this.Database = database;
            this.Collection = collection;
            this.Id = id;
            long blockId = id;
            do
            {
                var block = new DataBlock(stream, gatherHeaderSize, blockId, blockSize, create);
                this.blocks.Add(block);
                blockId = block.NextBlock;
            }
            while (blockId > 0);
        }

        public Record(string database, string collection, int gatherHeaderSize, Stream stream, int blockSize, byte[] bytes, params long[] ids)
        {
            if (ids.IsNullOrEmpty())
            {
                throw new ArgumentOutOfRangeException("ids cannot be null or empty");
            }

            this.Database = database;
            this.Collection = collection;
            this.Id = ids[0];
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
                    this.blocks.Add(new DataBlock(stream, gatherHeaderSize, ids[i], blockSize, data, ids[i + 1]));
                }
                else
                {
                    this.blocks.Add(new DataBlock(stream, gatherHeaderSize, ids[i], blockSize, data));
                }
            }
        }

        public byte[] GetBytes()
        {
            var length = this.blocks.Sum(b => b.DataSize);
            var bytes = new byte[length];
            var offset = 0;
            foreach (var b in this.blocks)
            {
                Buffer.BlockCopy(b.Data, 0, bytes, offset, b.DataSize);
                offset += b.DataSize;
            }
            return bytes;
        }

        public int GetBlockCount() => this.rwls.ReadLock(() => this.blocks.Count);

        public DataBlock PopBlock()
        {
            return this.rwls.WriteLock(() =>
            {
                if (this.blocks.Count <= 1)
                {
                    return null;
                }

                this.blocks[this.blocks.Count - 2].UpdateNextBlock(0); // blocks[-2] will be tail
                var block = this.blocks[this.blocks.Count - 1];
                this.blocks.Remove(block);
                return block;
            });
        }

        public void Dispose()
        {
            foreach (var block in this.blocks)
            {
                block.Dispose();
            }
            this.rwls.Dispose();
        }
    }
}
