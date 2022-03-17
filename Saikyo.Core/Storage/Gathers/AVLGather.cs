using Saikyo.Core.Extensions;
using Saikyo.Core.Storage.Records;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Storage.Gathers
{
    internal class AVLGather<T> : IGather<T>, IAVLTree<T> where T : IComparable<T>
    {
        public FileInfo File { get; private set; }

        public Stream Stream { get; private set; }

        public FixedSizeStreamUnit<long> LatestBlockId { get; private set; }

        public FixedSizeStreamUnit<int> BlockCap { get; private set; }

        public IChainRecord UnusedBlocks { get; private set; }

        public ConcurrentDictionary<long, IRecord> Records { get; protected set; } = new ConcurrentDictionary<long, IRecord>();

        public virtual int HeaderSize => this.BlockCap.Cap + this.LatestBlockId.Cap + this.root.Cap;

        public long Root
        {
            get => this.root.Value;
            set => this.root.Update(value);
        }

        public string Name { get; private set; }

        private FixedSizeStreamUnit<long> root;

        /// <summary>
        /// AVLGather is a powerful data storage. The data is built in the form of binary balanced tree
        /// </summary>
        /// <param name="file"></param>
        /// <param name="blockCap">The block header size is 30, so blockCap should be {size of data} + 30</param>
        /// <exception cref="ArgumentException"></exception>
        public AVLGather(string path, string name, int blockCap)
        {
            this.Name = name;
            this.File = new FileInfo(Path.Combine(path, $"{name}.gather"));
            this.Stream = new FileStream(this.File.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            if (this.File.Length == 0)
            {
                this.LatestBlockId = new FixedSizeStreamUnit<long>(this.Stream, 0, 0);
                this.BlockCap = new FixedSizeStreamUnit<int>(this.Stream, 8, blockCap);
                this.UnusedBlocks = new ChainRecord(this.Stream, 0, this.BlockCap.Cap + this.LatestBlockId.Cap, blockCap, true);
                this.root = new FixedSizeStreamUnit<long>(this.Stream, 12, 0);
            }
            else
            {
                this.LatestBlockId = new FixedSizeStreamUnit<long>(this.Stream, 0);
                this.BlockCap = new FixedSizeStreamUnit<int>(this.Stream, 8);
                if (this.BlockCap.Value != blockCap)
                {
                    throw new ArgumentException($"{this.File.FullName} already exists, and the block cap is {this.BlockCap.Value}, which does not match the input of block cap {blockCap}");
                }
                this.root = new FixedSizeStreamUnit<long>(this.Stream, 12);
                this.UnusedBlocks = new ChainRecord(this.Stream, 0, this.HeaderSize, blockCap, true);
            }
        }

        public long AddData(byte[] data, long id = 0)
        {
            throw new NotImplementedException("please use AddData(T data, long id = 0) instead");
        }

        public long AddData(T data, long id = 0)
        {
            if (data == null)
            {
                return 0;
            }

            if (id > 0)
            {
                if (!this.TryUseBlockId(id))
                {
                    Log.Warning($"{this.File.FullName} cannot use {id} block, it may be in used");
                    return 0;
                }
            }
            else
            {
                id = this.GetFreeBlockId();
            }

            var record = new AVLRecord<T>(this.Stream, id, this.HeaderSize, this.BlockCap.Value, data, this);
            this.Records.TryAdd(record.Id, record);
            if (this.Root == 0)
            {
                this.Root = id;
            }
            else
            {
                ((AVLRecord<T>)this.GetRecord(this.Root)).AddRecord(record);
            }
            return record.Id;
        }

        public bool Delete(long id)
        {
            if (!this.Records.TryRemove(id, out var record))
            {
                record = new Record(this.Stream, id, this.HeaderSize, this.BlockCap.Value);
            }

            this.UnusedBlocks.PushBlock(record.Head);
            return true;
        }

        public void Dispose()
        {
            foreach (var record in this.Records.Values)
            {
                record.Dispose();
            }

            this.LatestBlockId.Dispose();
            this.BlockCap.Dispose();
            this.root.Dispose();
            this.UnusedBlocks.Dispose();
            this.Stream.Flush();
            this.Stream.Dispose();
        }

        public IAVLNode<T> GetNode(long id)
        {
            if (id <= 0)
            {
                return null;
            }

            var record = (AVLRecord<T>)this.GetRecord(id);
            if (record == null)
            {
                return null;
            }

            return (Blocks.AVLBlock<T>)record.Head;
        }

        public IRecord GetRecord(long id)
        {
            if (this.Records.ContainsKey(id))
            {
                return this.Records[id];
            }

            var record = new AVLRecord<T>(this.Stream, id, this.HeaderSize, this.BlockCap.Value, this);
            this.Records.TryAdd(id, record);
            return record;
        }

        public T GetRecordValue(long id)
        {
            var record = this.GetRecord(id);
            if (record == null)
            {
                return default;
            }
         
            return ((AVLRecord<T>)record).GetValue();
        }

        public Column GetColumn(long id)
        {
            var node = this.GetNode(id);
            if (node == null)
            {
                return null;
            }

            return node.ToColumn();
        }

        public List<Column> GetAllColumns()
        {
            if (this.Root <= 0)
            {
                return new List<Column>();
            }

            var nodes = this.GetNode(this.Root).GetTree();
            return nodes.Select(n => n.ToColumn()).ToList();
        }

        public List<Column> Gt(T t)
        {
            if (this.Root <= 0 || t == null)
            {
                return new List<Column>();
            }

            return this.GetNode(this.Root)
                .Gt(t)
                .Select(n => n.ToColumn())
                .ToList();
        }

        public List<Column> Gte(T t)
        {
            if (this.Root <= 0 || t == null)
            {
                return new List<Column>();
            }

            return this.GetNode(this.Root)
                .Gte(t)
                .Select(n => n.ToColumn())
                .ToList();
        }

        public List<Column> Lt(T t)
        {
            if (this.Root <= 0 || t == null)
            {
                return new List<Column>();
            }

            return this.GetNode(this.Root)
                .Lt(t)
                .Select(n => n.ToColumn())
                .ToList();
        }

        public List<Column> Lte(T t)
        {
            if (this.Root <= 0 || t == null)
            {
                return new List<Column>();
            }

            return this.GetNode(this.Root)
                .Lte(t)
                .Select(n => n.ToColumn())
                .ToList();
        }

        public List<Column> Eq(T t)
        {
            if (this.Root <= 0 || t == null)
            {
                return new List<Column>();
            }

            return this.GetNode(this.Root)
                .Eq(t)
                .Select(n => n.ToColumn())
                .ToList();
        }

        public void Destroy()
        {
            this.Stream.Close();
            this.File.Delete();
        }
    }
}
