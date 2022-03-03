using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Saikyo.Core.Storage
{
    internal abstract class BaseBlock : IDisposable
    {
        public long Id { get; protected set; }

        public int HeaderSize { get; protected set; } = Const.BaseBlockHeaderSize;

        public int DataSize { get; protected set; }

        /// <summary>
        /// 0 init 1 deleted
        /// </summary>
        public byte State { get; protected set; }

        public byte[] Data { get; protected set; }

        protected int blockSize;
        protected int gatherHeaderSize;
        protected Stream stream;
        protected ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
        protected volatile bool changed = false;

        public BaseBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, bool create = false)
        {
            this.stream = stream;
            this.Id = id;
            this.blockSize = blockSize;
            this.gatherHeaderSize = gatherHeaderSize;

            if (!create)
            {
                var startPosition = id * blockSize + gatherHeaderSize;
                this.DataSize = this.stream.ReadAsInt32(startPosition);
                this.State = this.stream.ReadAsByte(startPosition + 4);
                this.InitData();
                this.Data = new byte[this.DataSize];
                if (this.State != 1 && this.DataSize > 0)
                {
                    this.stream.Position = startPosition + this.HeaderSize;
                    this.stream.Read(this.Data, 0, this.DataSize);
                }
            }
            else
            {
                this.Data = new byte[0];
                this.InitData();
            }
        }

        public BaseBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, byte[] bytes)
        {
            this.stream = stream;
            this.Id = id;
            this.blockSize = blockSize;
            this.DataSize = bytes.Length;
            this.Data = bytes;
            this.changed = true;
            this.gatherHeaderSize = gatherHeaderSize;
        }

        public void MarkAsDeleted()
        {
            this.State = 1;
        }

        public virtual void Dispose()
        {
            if (!this.changed)
            {
                return;
            }

            var startPosition = this.Id * blockSize + gatherHeaderSize;
            this.stream.Write(startPosition, BitConverter.GetBytes(this.DataSize));
            this.stream.Write(startPosition + 4, new byte[1] { this.State });

            if (this.State != 1)
            {
                if (this.blockSize - this.HeaderSize < this.Data.Length)
                {
                    throw new ArgumentException($"The block size is {this.blockSize}, and header size is {this.HeaderSize}, but data size is {this.Data.Length}");
                }

                this.stream.Write(startPosition + this.HeaderSize, this.Data);
            }
        }

        protected abstract void InitData();
    }
}
