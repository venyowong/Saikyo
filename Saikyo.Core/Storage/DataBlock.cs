using Saikyo.Core.Extensions;
using System;
using System.IO;

namespace Saikyo.Core.Storage
{
    internal class DataBlock : BaseBlock
    {
        public long NextBlock { get; private set; }

        public DataBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, bool create = false) : base(stream, gatherHeaderSize, id, blockSize, create)
        {
            if (!create)
            {
                var startPosition = id * blockSize + gatherHeaderSize + Const.BaseBlockHeaderSize;
                this.NextBlock = this.stream.ReadAsLong(startPosition);
            }
            else
            {
                this.NextBlock = 0;
            }
        }

        public DataBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, byte[] bytes, long nextBlock = 0) : base(stream, gatherHeaderSize, id, blockSize, bytes)
        {
            this.NextBlock = nextBlock;
            this.HeaderSize += 8;
        }

        public void UpdateNextBlock(long nextBlock)
        {
            this.rwls.WriteLock(() =>
            {
                this.NextBlock = nextBlock;
            });
        }

        public void Update(byte[] bytes)
        {
            this.Data = bytes;
            this.DataSize = bytes.Length;
        }

        public override void Dispose()
        {
            base.Dispose();

            var startPosition = this.Id * this.blockSize + this.gatherHeaderSize + Const.BaseBlockHeaderSize;
            this.stream.Write(startPosition, BitConverter.GetBytes(this.NextBlock));
        }

        protected override void InitData()
        {
            this.HeaderSize += 8;
        }
    }
}
