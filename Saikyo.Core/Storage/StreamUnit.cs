using Saikyo.Core.Extensions;
using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal class StreamUnit : IDisposable
    {
        public long Offset { get; protected set; }

        public int Cap { get; protected set; }

        public int Size { get; protected set; }

        public byte[] Data { get; protected set; }

        protected Stream stream;
        protected bool changed;

        public StreamUnit(Stream stream, long offset, int cap, int size)
        {
            this.stream = stream;
            this.Offset = offset;
            this.Cap = cap;
            this.Size = size;

            this.Data = this.stream.Read(this.Offset, this.Size);
        }

        public StreamUnit(Stream stream, long offset, int cap, byte[] bytes)
        {
            this.stream = stream;
            this.Offset = offset;
            this.Cap = cap;
            this.Data = bytes;
            this.Size = bytes.Length;
            this.changed = true;
        }

        public void Update(byte[] bytes)
        {
            this.Data = bytes;
            this.Size = bytes.Length;
            this.changed = true;
        }

        public virtual void Dispose()
        {
            if (this.changed)
            {
                this.stream.Write(this.Offset, this.Data);
            }
        }
    }

    internal class StreamUnit<T> : StreamUnit
    {
        public T Value { get; protected set; }

        public StreamUnit(Stream stream, long offset, int cap, int size) : base(stream, offset, cap, size)
        {
            this.Value = this.Data.FromBytes<T>();
        }

        public StreamUnit(Stream stream, long offset, int cap, T value)
            : base(stream, offset, cap, value.ToBytes())
        {
            this.Value = value;
        }

        public void Update(T t)
        {
            this.Value = t;
            this.Data = this.Value.ToBytes();
            this.Size = this.Data.Length;
            this.changed = true;
        }
    }

    internal class FixedSizeStreamUnit<T> : StreamUnit<T>
    {
        public FixedSizeStreamUnit(Stream stream, long offset) 
            : base(stream, offset, TypeHelper.GetTypeSize(typeof(T)), TypeHelper.GetTypeSize(typeof(T)))
        {
        }

        public FixedSizeStreamUnit(Stream stream, long offset, T value)
            : base(stream, offset, TypeHelper.GetTypeSize(typeof(T)), value)
        {
        }
    }
}
