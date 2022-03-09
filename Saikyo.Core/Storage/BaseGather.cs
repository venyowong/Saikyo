using Saikyo.Core.Extensions;
using System;
using System.IO;
using System.Linq;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal abstract class BaseGather<T> : IGather
    {
        public const int BaseGatherHeaderSize = 12;

        public string Database { get; protected set; }

        public string Collection { get; protected set; }

        public int HeaderSize { get; protected set; } = BaseGatherHeaderSize;

        public int BlockSize { get; protected set; }

        public Stream Stream { get; protected set; }

        protected string name;
        protected FileInfo file;
        protected long latestBlockId;
        protected Record unusedBlocks;
        protected ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public BaseGather(string database, string collection, string name, int blockSize)
        {
            this.Database = database;
            this.Collection = collection;
            this.name = name;
            this.BlockSize = blockSize;
            var directory = new DirectoryInfo(Path.Combine(Instance.Config.DataPath, Path.Combine(database, collection)));
            if (!directory.Exists)
            {
                directory.Create();
            }
            this.file = new FileInfo(Path.Combine(directory.FullName, $"{name}.gather"));
            if (this.file.Exists && this.file.Length == 0) // If the file is not closed properly, it will be a empty file, so this abnormal file need to be deleted automatically
            {
                File.Delete(this.file.FullName);
                this.file.Refresh();
            }
            if (this.file.Exists)
            {
                this.Stream = new FileStream(file.FullName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                this.latestBlockId = this.Stream.ReadAsLong(0);
                this.BlockSize = this.Stream.ReadAsInt32(8);
                if (this.BlockSize != blockSize)
                {
                    throw new ArgumentException($"{this.file.FullName} already exists, and the block size is {this.BlockSize}, which does not match the input of block size {blockSize}");
                }
                this.Init();
                this.unusedBlocks = new Record(this, 0);
            }
            else
            {
                this.Stream = new FileStream(file.FullName, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
                this.Init();
                this.unusedBlocks = new Record(this, 0, true);
            }
        }

        public abstract IBlock GetBlock(long id, bool create = false);

        public abstract IBlock GetBlock(long id, object obj, long next = 0);

        public long AddData(object obj, long id = 0)
        {
            if (obj is T t)
            {
                return this.AddData(t, id);
            }
            else
            {
                return 0;
            }
        }

        public abstract long AddData(T t, long id = 0);

        public abstract bool Delete(long id);

        public void Update(long id, object obj)
        {
            if (obj is T t)
            {
                this.Update(id, t);
            }
        }

        public abstract void Update(long id, T t);

        public void Destroy()
        {
            this.Stream.Close();
            this.file.Delete();
            this.rwls.Dispose();
        }

        public virtual void Dispose()
        {
            this.Stream.Write(0, BitConverter.GetBytes(this.latestBlockId));
            this.Stream.Write(8, BitConverter.GetBytes(this.BlockSize));
            this.unusedBlocks.Dispose();
            this.Stream.Flush();
            this.Stream.Dispose();
            this.rwls.Dispose();
        }

        protected long GetFreeBlockId()
        {
            var block = this.unusedBlocks.PopBlock();
            if (block != null)
            {
                return block.Id;
            }

            this.latestBlockId++;
            return this.latestBlockId;
        }

        protected bool TryUseBlockId(long id)
        {
            if (id <= this.latestBlockId) // old id
            {
                if (!this.unusedBlocks.Blocks.Any(b => b.Id == id))
                {
                    return false; // this id is in use
                }
                var block = this.unusedBlocks.PopBlock(id);
                if (block == null)
                {
                    return false;
                }

                return true;
            }
            else
            {
                this.latestBlockId++;
                while (this.latestBlockId < id)
                {
                    var block = new DataBlock(this.Stream, this.HeaderSize, this.latestBlockId, this.BlockSize, true);
                    block.MarkAsDeleted();
                    this.unusedBlocks.PushBlock(block);
                    this.latestBlockId++;
                }
                return true;
            }
        }

        protected abstract void Init();
    }
}
