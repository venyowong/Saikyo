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
    internal class PolylithGather : DataGather, IGather<string>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <param name="name"></param>
        /// <param name="blockCap">The block header size is 12</param>
        public PolylithGather(string path, string name, int blockCap) : base(path, name, blockCap)
        {
        }

        public override long AddData(byte[] data, long id = 0)
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

            var record = new PolylithRecord(id, this, data);
            this.Records.TryAdd(record.Id, record);
            return record.Id;
        }

        public long AddData(string data, long id = 0) => this.AddData(Encoding.UTF8.GetBytes(data), id);

        public override IRecord GetRecord(long id)
        {
            if (this.Records.ContainsKey(id))
            {
                return this.Records[id];
            }

            var record = new PolylithRecord(id, this);
            this.Records.TryAdd(id, record);
            return record;
        }

        public string GetRecordValue(long id)
        {
            var record = this.GetRecord(id);
            return record?.GetBytes().FromBytes<string>();
        }
    }
}
