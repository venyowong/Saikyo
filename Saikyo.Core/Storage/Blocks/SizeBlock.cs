using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal class SizeBlock : ISizeBlock
    {
        public const int HeaderSize = 12;

        public FixedSizeStreamUnit<int> DataSize { get; private set; }

        public long Id { get; private set; }

        public int Cap { get; private set; }

        public long Offset { get; private set; }

        public FixedSizeStreamUnit<long> Next { get; private set; }

        public StreamUnit Data { get; private set; }

        public Stream Stream { get; private set; }

        public SizeBlock(Stream stream, long id, int cap, int offset)
        {
            this.Stream = stream;
            this.Id = id;
            this.Cap = cap;
            this.Offset = offset + id * cap;
            this.Next = new FixedSizeStreamUnit<long>(stream, this.Offset);
            this.DataSize = new FixedSizeStreamUnit<int>(stream, this.Offset + this.Next.Cap);
            this.Data = new StreamUnit(stream, this.DataSize.Offset + this.DataSize.Cap, cap - HeaderSize, this.DataSize.Value);
        }

        public SizeBlock(Stream stream, long id, int cap, int offset, byte[] bytes, long next = 0)
        {
            this.Stream = stream;
            this.Id = id;
            this.Cap = cap;
            this.Offset = offset + id * cap;
            if (next > 0)
            {
                this.Next = new FixedSizeStreamUnit<long>(stream, this.Offset, next);
            }
            else
            {
                this.Next = new FixedSizeStreamUnit<long>(stream, this.Offset, 0);
            }
            this.DataSize = new FixedSizeStreamUnit<int>(stream, this.Offset + this.Next.Cap, bytes.Length);
            this.Data = new StreamUnit(stream, this.DataSize.Offset + this.DataSize.Cap, cap - HeaderSize, bytes);
        }

        public void Dispose()
        {
            this.Next?.Dispose();
            this.Data?.Dispose();
            this.DataSize?.Dispose();
        }
    }
}
