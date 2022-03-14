﻿using Saikyo.Core.Extensions;
using Saikyo.Core.Storage.Records;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Gathers
{
    internal class PolylithGather : DataGather
    {
        public PolylithGather(string file, int blockCap) : base(file, blockCap)
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

            var record = new PolylithRecord(id, this);
            this.Records.TryAdd(record.Id, record);
            return record.Id;
        }
    }
}