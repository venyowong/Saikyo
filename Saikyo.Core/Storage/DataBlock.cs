using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.IO;

namespace Saikyo.Core.Storage
{
    internal class DataBlock : BaseBlock
    {
        private long next;

        public override long Next
        {
            get { return next; }
            set
            {
                this.rwls.WriteLock(() =>
                {
                    this.next = value;
                });
            }
        }

        public DataBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, bool create = false) : base(stream, gatherHeaderSize, id, blockSize, create)
        {
        }

        public DataBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, byte[] bytes, long nextBlock = 0) : base(stream, gatherHeaderSize, id, blockSize, bytes, nextBlock)
        {
        }

        public override void Update(object data)
        {
            if (data == null)
            {
                return;
            }

            if (data is byte[] bytes)
            {
                this.Data = bytes;
                this.DataSize = bytes.Length;
            }
            else
            {
                Log.Warning($"Failed to update data({data.GetType()}) in DataBlock");
            }
        }

        protected override void InitHeader()
        {
        }
    }
}
