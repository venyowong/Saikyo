using Saikyo.Core.Storage.Blocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Records
{
    internal class AVLRecord<T> : IRecord where T : IComparable<T>
    {
        public long Id { get; protected set; }

        public IBlock Head { get; protected set; }

        public Stream Stream { get; private set; }

        public int Offset { get; private set; }

        public int BlockCap { get; protected set; }

        public AVLRecord(Stream stream, long id, int offset, int blockCap, IAVLTree<T> tree)
        {
            this.Stream = stream;
            this.Id = id;
            this.Offset = offset;
            this.BlockCap = blockCap;
            this.Head = new AVLBlock<T>(stream, id, offset, tree, blockCap);
        }

        public AVLRecord(Stream stream, long id, int offset, int blockCap, T t, IAVLTree<T> tree)
        {
            this.Stream = stream;
            this.Id = id;
            this.Offset = offset;
            this.BlockCap = blockCap;
            this.Head = new AVLBlock<T>(stream, id, offset, t, tree, blockCap);
        }

        public void AddRecord(AVLRecord<T> record) => ((AVLBlock<T>)this.Head).AddNode((AVLBlock<T>)record.Head);

        public void Dispose() => this.Head.Dispose();
    }
}
