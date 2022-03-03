using Saikyo.Core.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

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

            var blockCount = Math.Ceiling((double)bytes.Length / (this.blockSize - 1));
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

                var record = new Record(this.Database, this.Collection, this.headerSize, this.stream, this.blockSize, bytes, ids.ToArray());
                this.records.TryAdd(record.Id, record);
                return record.Id;
            });
        }

        public override bool Delete(long id)
        {
            throw new NotImplementedException();
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

                var record = new Record(this.Database, this.Collection, this.headerSize, this.stream, id, this.blockSize);
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
