using Saikyo.Core.Storage.Blocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal class Record : IRecord
    {
        public long Id { get; protected set; }

        public IBlock Head { get; protected set; }

        public Stream Stream { get; private set; }

        public int Offset { get; private set; }

        public int BlockCap { get; protected set; }

        public Record(Stream stream, long id, int offset, int blockCap)
        {
            this.Stream = stream;
            this.Id = id;
            this.Offset = offset;
            this.BlockCap = blockCap;
            this.Head = new Block(stream, id, blockCap, offset);
        }

        public Record(Stream stream, long id, int offset, int blockCap, byte[] bytes)
        {
            this.Stream = stream;
            this.Id = id;
            this.Offset = offset;
            this.BlockCap = blockCap;
            this.Head = new Block(stream, id, blockCap, offset, bytes);
        }

        public void Dispose() => this.Head.Dispose();

        public byte[] GetBytes() => this.Head.Data.Data;
    }
}
