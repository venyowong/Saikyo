using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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
            return this.rwls.WriteLock(() =>
            {
                var id = this.GetFreeBlockId();
                var block = new AVLBlock<T>(id, t, this);
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
            });
        }

        public override bool Delete(long id)
        {
            if (!this.blocks.TryRemove(id, out var avlBlock))
            {
                avlBlock = new AVLBlock<T>(id, this);
            }
            avlBlock.Delete();
            var block = new DataBlock(this.Stream, this.HeaderSize, id, this.BlockSize, true);
            block.MarkAsDeleted();
            this.unusedBlocks.PushBlock(block);
            return true;
        }

        public override void Update(long id, T t) => this.GetBlock(id).UpdateValue(t);

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

            var block = new AVLBlock<T>(id, this);
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
            var time = DateTime.Now;
            foreach (var b in this.blocks.Values)
            {
                b.Dispose();
            }
            this.Stream.Write(BaseGatherHeaderSize, BitConverter.GetBytes(this.Root));

            base.Dispose();
            Log.Information($"{this.Database}/{this.Collection}/{this.name}.gather took {DateTime.Now - time} to dispose");
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
                this.Root = this.Stream.ReadAsLong(BaseGatherHeaderSize);
            }
            this.HeaderSize += 8;
        }
    }
}
