using Saikyo.Core.Extensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace Saikyo.Core.Storage
{
    internal class DataGather : BaseGather<byte[]>
    {
        /// <summary>
        /// Allow data to be stored in multiple blocks
        /// </summary>
        private bool multipleBlocks;
        private ConcurrentDictionary<long, Record> records = new ConcurrentDictionary<long, Record>();

        public DataGather(string database, string collection, string name, int blockSize, bool multipleBlocks = true) : base(database, collection, name, blockSize)
        {
            this.multipleBlocks = multipleBlocks;
        }

        public override long AddData(byte[] bytes)
        {
            if (bytes.IsNullOrEmpty())
            {
                return -1;
            }

            var blockCount = Math.Ceiling((double)bytes.Length / (this.BlockSize - 1));
            if (!this.multipleBlocks && blockCount > 1)
            {
                throw new InvalidDataException($"{this.name}.gather doesn't allow store data in multiple blocks");
            }

            return this.rwls.WriteLock(() =>
            {
                var ids = new List<long>();
                for (var i = 0; i < blockCount; i++)
                {
                    ids.Add(this.GetFreeBlockId());
                }

                var record = new Record(this.Database, this.Collection, this.HeaderSize, this.Stream, this.BlockSize, bytes, this, ids.ToArray());
                this.records.TryAdd(record.Id, record);
                return record.Id;
            });
        }

        public override bool Delete(long id)
        {
            if (!this.records.TryRemove(id, out var record))
            {
                record = new Record(this.Database, this.Collection, this.HeaderSize, this.Stream, id, this.BlockSize, this);
            }

            foreach (var block in record.Blocks)
            {
                block.MarkAsDeleted();
                this.unusedBlocks.PushBlock(block);
            }
            return true;
        }

        public override void Update(long id, byte[] bytes)
        {
            var blockCount = Math.Ceiling((double)bytes.Length / (this.BlockSize - 1));
            if (!this.multipleBlocks && blockCount > 1)
            {
                throw new InvalidDataException($"{this.name}.gather doesn't allow store data in multiple blocks");
            }

            this.rwls.WriteLock(() =>
            {
                Record record = null;
                if (this.records.ContainsKey(id))
                {
                    record = this.records[id];
                }
                if (record == null)
                {
                    record = new Record(this.Database, this.Collection, this.HeaderSize, this.Stream, id, this.BlockSize, this);
                }

                var ids = new List<long>();
                for (var i = 0; i < blockCount - record.Blocks.Count; i++)
                {
                    ids.Add(this.GetFreeBlockId());
                }
                record.Update(bytes, ids.ToArray());
            });
        }

        public Record GetRecord(long id)
        {
            if (this.records.ContainsKey(id))
            {
                return this.records[id];
            }

            return this.rwls.ReadLock(() =>
            {
                if (this.records.ContainsKey(id))
                {
                    return this.records[id];
                }

                var record = new Record(this.Database, this.Collection, this.HeaderSize, this.Stream, id, this.BlockSize, this);
                this.records.TryAdd(id, record);
                return record;
            });
        }

        public override void Dispose()
        {
            foreach (var record in this.records.Values)
            {
                record.Dispose();
            }

            base.Dispose();
        }

        protected override void Init()
        {
        }
    }
}
