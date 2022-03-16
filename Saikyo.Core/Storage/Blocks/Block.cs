using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal class Block : IBlock
    {
        public readonly int HeaderSize = 8;

        public long Id { get; private set; }

        public int Cap { get; private set; }

        public long Offset { get; private set; }

        public FixedSizeStreamUnit<long> Next { get; private set; }

        public StreamUnit Data { get; private set; }

        public Stream Stream { get; private set; }

        public Block(Stream stream, long id, int cap, int offset, bool ignoreData = false, long next = 0)
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
                this.Next = new FixedSizeStreamUnit<long>(stream, this.Offset);
            }
            if (!ignoreData)
            {
                var dataCap = cap - HeaderSize;
                this.Data = new StreamUnit(stream, this.Next.Offset + this.Next.Cap, dataCap, dataCap);
            }
        }

        public Block(Stream stream, long id, int cap, int offset, byte[] bytes, long next = 0)
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
            var dataCap = cap - HeaderSize;
            this.Data = new StreamUnit(stream, this.Next.Offset + this.Next.Cap, dataCap, bytes);
        }

        public void Dispose()
        {
            this.Next?.Dispose();
            this.Data?.Dispose();
        }
    }
}
