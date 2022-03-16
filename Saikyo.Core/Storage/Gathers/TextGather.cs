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
    internal class TextGather : IGather<string>, IColumnGetter
    {
        public FileInfo File => this.keyGather.File;

        public Stream Stream => this.keyGather.Stream;

        public FixedSizeStreamUnit<long> LatestBlockId => this.keyGather.LatestBlockId;

        public FixedSizeStreamUnit<int> BlockCap => this.keyGather.BlockCap;

        public IChainRecord UnusedBlocks => this.keyGather.UnusedBlocks;

        public ConcurrentDictionary<long, IRecord> Records => this.keyGather.Records;

        public int HeaderSize => this.keyGather.HeaderSize;

        public string Name => this.keyGather.Name;

        private DataGather<long> keyGather;
        private PolylithGather valueGather;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <param name="name"></param>
        /// <param name="textCap">The block header size is 12</param>
        public TextGather(string path, string name, int textCap = 4096)
        {
            this.keyGather = new DataGather<long>(path, name);
            this.valueGather = new PolylithGather(path, $"{name}_polylith_text", textCap);
        }

        public long AddData(string data, long id = 0)
        {
            if (string.IsNullOrWhiteSpace(data))
            {
                return 0;
            }

            var valueId = 0L;
            if (!string.IsNullOrWhiteSpace(data))
            {
                valueId = this.valueGather.AddData(data);
            }
            return this.keyGather.AddData(valueId, id);
        }

        public long AddData(byte[] data, long id = 0)
        {
            if (data.IsNullOrEmpty())
            {
                return 0;
            }

            var valueId = this.valueGather.AddData(data);
            return this.keyGather.AddData(valueId, id);
        }

        public void Dispose()
        {
            this.valueGather.Dispose();
            this.keyGather.Dispose();
        }

        public IRecord GetRecord(long id) => this.keyGather.GetRecord(id);

        public void Destroy()
        {
            this.valueGather.Destroy();
            this.keyGather.Destroy();
        }

        public bool Delete(long id)
        {
            var valueId = this.keyGather.GetRecordValue(id);
            if (valueId > 0)
            {
                if (!this.valueGather.Delete(valueId))
                {
                    return false;
                }
            }

            return this.keyGather.Delete(id);
        }

        public string GetRecordValue(long id)
        {
            var valueId = this.keyGather.GetRecordValue(id);
            if (valueId <= 0)
            {
                return null;
            }

            return this.valueGather.GetRecordValue(valueId);
        }

        public Column GetColumn(long id) => new Column
        {
            Id = id,
            Value = this.GetRecordValue(id)
        };
    }
}
