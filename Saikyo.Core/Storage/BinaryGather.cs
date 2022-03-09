using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Saikyo.Core.Storage
{
    internal class BinaryGather<T> : BaseGather<T>, IColumnGetter, IAVLTree where T : IComparable<T>
    {
        public long Root { get; set; }

        private ConcurrentDictionary<long, AVLBlock<T>> blocks = new ConcurrentDictionary<long, AVLBlock<T>>();

        public BinaryGather(string database, string collection, string name, int blockSize) 
            : base(database, collection, name, blockSize)
        {
        }

        public override IBlock GetBlock(long id, bool create = false) => new AVLBlock<T>(id, this);

        public override IBlock GetBlock(long id, object obj, long next = 0)
        {
            if (obj == null)
            {
                return null;
            }

            if (obj is T t)
            {
                return new AVLBlock<T>(id, t, this);
            }
            else
            {
                Log.Warning($"You can't use data({obj.GetType()}) to init AVLBlock");
                return null;
            }
        }

        public override long AddData(T t, long id = 0)
        {
            return this.rwls.WriteLock(() =>
            {
                if (id == 0)
                {
                    id = this.GetFreeBlockId();
                }
                else
                {
                    if (!this.TryUseBlockId(id))
                    {
                        Log.Warning($"{this.name}.gather cannot use {id} block");
                        return 0;
                    }
                }
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

        public override void Update(long id, T t) => this.GetBlock(id).Update(t);

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

        public List<Column> GetAllColumns()
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            return this.GetBlock(this.Root).GetTree();
        }

        public List<Column> Gt(object obj)
        {
            if (this.Root <= 0 || obj == null)
            {
                return new List<Column>();
            }

            if (obj is T t)
            {
                return this.GetBlock(this.Root).Gt(t);
            }
            else
            {
                return new List<Column>();
            }
        }

        public List<Column> Gte(object obj)
        {
            if (this.Root <= 0 || obj == null)
            {
                return new List<Column>();
            }

            if (obj is T t)
            {
                return this.GetBlock(this.Root).Gte(t);
            }
            else
            {
                return new List<Column>();
            }
        }

        public List<Column> Lt(object obj)
        {
            if (this.Root <= 0 || obj == null)
            {
                return new List<Column>();
            }

            if (obj is T t)
            {
                return this.GetBlock(this.Root).Lt(t);
            }
            else
            {
                return new List<Column>();
            }
        }

        public List<Column> Lte(object obj)
        {
            if (this.Root <= 0 || obj == null)
            {
                return new List<Column>();
            }

            if (obj is T t)
            {
                return this.GetBlock(this.Root).Lte(t);
            }
            else
            {
                return new List<Column>();
            }
        }

        public List<Column> Eq(object obj)
        {
            if (this.Root <= 0 || obj == null)
            {
                return new List<Column>();
            }

            if (obj is T t)
            {
                return this.GetBlock(this.Root).Eq(t);
            }
            else
            {
                return new List<Column>();
            }
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
