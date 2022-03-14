using Saikyo.Core.Extensions;
using Saikyo.Core.Storage.Records;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Gathers
{
    internal class DataGather : IGather
    {
        public Stream Stream { get; private set; }

        public FixedSizeStreamUnit<int> BlockCap { get; private set; }

        public FileInfo File { get; private set; }

        public FixedSizeStreamUnit<long> LatestBlockId { get; private set; }

        public IChainRecord UnusedBlocks { get; private set; }

        public virtual int HeaderSize => this.BlockCap.Cap + this.LatestBlockId.Cap;

        public ConcurrentDictionary<long, IRecord> Records { get; protected set; } = new ConcurrentDictionary<long, IRecord>();

        public DataGather(string file, int blockCap)
        {
            this.File = new FileInfo(file);
            this.Stream = new FileStream(this.File.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            if (this.File.Length == 0)
            {
                this.LatestBlockId = new FixedSizeStreamUnit<long>(this.Stream, 0, 0);
                this.BlockCap = new FixedSizeStreamUnit<int>(this.Stream, 8, blockCap);
                this.UnusedBlocks = new ChainRecord(this.Stream, 0, this.BlockCap.Cap + this.LatestBlockId.Cap, blockCap, true);
            }
            else
            {
                this.LatestBlockId = new FixedSizeStreamUnit<long>(this.Stream, 0);
                this.BlockCap = new FixedSizeStreamUnit<int>(this.Stream, 8);
                if (this.BlockCap.Value != blockCap)
                {
                    throw new ArgumentException($"{this.File.FullName} already exists, and the block cap is {this.BlockCap.Value}, which does not match the input of block cap {blockCap}");
                }
                this.UnusedBlocks = new ChainRecord(this.Stream, 0, this.HeaderSize, blockCap, true);
            }
        }

        public virtual long AddData(byte[] data, long id = 0)
        {
            if (data.IsNullOrEmpty())
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

            var record = new Records.Record(this.Stream, id, this.HeaderSize, this.BlockCap.Value, data);
            this.Records.TryAdd(record.Id, record);
            return record.Id;
        }

        public IRecord GetRecord(long id)
        {
            if (this.Records.ContainsKey(id))
            {
                return this.Records[id];
            }

            var record = new Records.Record(this.Stream, id, this.HeaderSize, this.BlockCap.Value);
            this.Records.TryAdd(id, record);
            return record;
        }

        public void Dispose()
        {
            foreach (var record in this.Records.Values)
            {
                record.Dispose();
            }

            this.LatestBlockId.Dispose();
            this.BlockCap.Dispose();
            this.UnusedBlocks.Dispose();
            this.Stream.Flush();
            this.Stream.Dispose();
        }
    }
}
