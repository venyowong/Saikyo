using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal abstract class BaseGather<T> : IInserter<T>, IDeleter, IDestroyer
    {
        public const int BaseGatherHeaderSize = 12;

        public string Database { get; private set; }

        public string Collection { get; private set; }

        protected string name;
        protected int blockSize;
        protected FileInfo file;
        protected FileStream stream;
        protected long latestBlockId;
        protected Record unusedBlocks;
        protected int headerSize = BaseGatherHeaderSize;
        protected ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public BaseGather(string database, string collection, string name, int blockSize)
        {
            this.Database = database;
            this.Collection = collection;
            this.name = name;
            this.blockSize = blockSize;
            var directory = new DirectoryInfo(Path.Combine(Instance.Config.DataPath, Path.Combine(database, collection)));
            if (!directory.Exists)
            {
                directory.Create();
            }
            this.file = new FileInfo(Path.Combine(directory.FullName, $"{name}.gather"));
            if (this.file.Exists)
            {
                this.stream = new FileStream(file.FullName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                this.latestBlockId = this.stream.ReadAsLong(0);
                this.blockSize = this.stream.ReadAsInt32(8);
                if (this.blockSize != blockSize)
                {
                    throw new ArgumentException($"{this.file.FullName} already exists, and the block size is {this.blockSize}, which does not match the input of block size {blockSize}");
                }
                this.Init();
                this.unusedBlocks = new Record(database, collection, this.headerSize, this.stream, 0, blockSize);
            }
            else
            {
                this.stream = new FileStream(file.FullName, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
                this.Init();
                this.unusedBlocks = new Record(database, collection, this.headerSize, this.stream, 0, blockSize, true);
            }
        }

        public abstract long AddData(T t);

        public abstract bool Delete(long id);

        public void Destroy()
        {
            this.stream.Close();
            this.file.Delete();
            this.rwls.Dispose();
        }

        public virtual void Dispose()
        {
            this.stream.Write(0, BitConverter.GetBytes(this.latestBlockId));
            this.stream.Write(8, BitConverter.GetBytes(this.blockSize));
            this.unusedBlocks.Dispose();
            this.stream.Flush();
            this.stream.Dispose();
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

        protected abstract void Init();
    }
}
