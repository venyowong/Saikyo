using Saikyo.Core.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal class BinaryGather<T> : BaseGather<T>, IColumnGetter where T : IComparable<T>
    {
        public long Root { get; set; }

        private ConcurrentDictionary<long, AVLBlock<T>> blocks = new ConcurrentDictionary<long, AVLBlock<T>>();

        public BinaryGather(string database, string collection, string name, int blockSize) 
            : base(database, collection, name, blockSize)
        {
        }

        public override long AddData(T t)
        {
            try
            {
                this.rwls.TryEnterWriteLock(Instance.Config.ReaderWriterLockTimeout);
                try
                {
                    var id = this.GetFreeBlockId();
                    var block = new AVLBlock<T>(this.stream, this.headerSize, id, this.blockSize, t, this);
                    this.blocks.TryAdd(id, block);

                    if (this.Root == 0)
                    {
                        this.Root = id;
                    }
                    else
                    {
                        this.GetBlock(this.Root).AddBlock(block);
                    }
                    return id;
                }
                finally
                {
                    this.rwls.ExitWriteLock();
                }
            }
            catch (ApplicationException)
            {
                return -1;
            }
        }

        public AVLBlock<T> GetBlock(long id)
        {
            if (id <= 0)
            {
                return null;
            }

            if (this.blocks.ContainsKey(id))
            {
                return this.blocks[id];
            }

            var block = new AVLBlock<T>(this.stream, this.headerSize, id, this.blockSize, this);
            this.blocks.TryAdd(id, block);
            return block;
        }

        public Column GetColumn(long id)
        {
            var block = this.GetBlock(id);
            if (block == null)
            {
                return null;
            }

            return new Column
            {
                Id = id,
                Value = block.Value
            };
        }

        public List<Column> GetAllBlocks()
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).GetTree();
        }

        public override void Dispose()
        {
            foreach (var b in this.blocks.Values)
            {
                b.Dispose();
            }
            this.stream.Write(BaseGatherHeaderSize, BitConverter.GetBytes(this.Root));

            base.Dispose();
        }

        public override string ToString()
        {
            if (this.Root <= 0)
            {
                return string.Empty;
            }

            return this.GetBlock(this.Root).ToString();
        }

        protected override void Init()
        {
            if (this.latestBlockId > 0)
            {
                this.Root = this.stream.ReadAsLong(BaseGatherHeaderSize);
            }
            this.headerSize += 8;
        }
    }
}
