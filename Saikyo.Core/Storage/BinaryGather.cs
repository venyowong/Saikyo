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

        public override bool Delete(long id)
        {
            if (!this.blocks.TryRemove(id, out var avlBlock))
            {
                return false;
            }

            avlBlock.Delete();
            var block = new DataBlock(this.stream, this.headerSize, id, this.blockSize, true);
            block.MarkAsDeleted();
            this.unusedBlocks.PushBlock(block);
            return true;
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

        public List<Column> Gt(T t)
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).Gt(t);
        }

        public List<Column> Gte(T t)
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).Gte(t);
        }

        public List<Column> Lt(T t)
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).Lt(t);
        }

        public List<Column> Lte(T t)
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).Lte(t);
        }

        public List<Column> Eq(T t)
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).Eq(t);
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
