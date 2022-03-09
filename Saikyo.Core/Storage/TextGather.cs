using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.IO;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal class TextGather : IGather, IColumnGetter
    {
        public string Database { get; private set; }

        public string Collection { get; private set; }

        public int HeaderSize => this.valueGather.HeaderSize;

        public Stream Stream => this.valueGather.Stream;

        public int BlockSize => this.valueGather.BlockSize;

        private string name;
        private DataGather keyGather;
        private DataGather valueGather;
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public TextGather(string database, string collection, string name)
        {
            this.Database = database;
            this.Collection = collection;
            this.name = name;
            this.keyGather = new DataGather(database, collection, name, Const.DataBlockHeaderSize + 8, false);
            this.valueGather = new DataGather(database, collection, $"{name}_text", Instance.Config.MaxTextBlockSize);
        }

        public long AddData(object obj, long id = 0)
        {
            if (obj is string str)
            {
                return this.rwls.WriteLock(() =>
                {
                    var valueId = 0L;
                    if (!string.IsNullOrWhiteSpace(str))
                    {
                        valueId = this.valueGather.AddData(Encoding.UTF8.GetBytes(str));
                    }
                    return this.keyGather.AddData(BitConverter.GetBytes(valueId), id);
                });
            }
            else
            {
                return 0;
            }
        }

        public bool Delete(long id)
        {
            var key = this.keyGather.GetRecord(id);
            if (key == null)
            {
                return false;
            }

            var bytes = key.GetBytes();
            if (bytes.IsNullOrEmpty())
            {
                return false;
            }

            var textId = BitConverter.ToInt64(bytes, 0);
            if (textId > 0)
            {
                if (!this.valueGather.Delete(textId))
                {
                    return false;
                }
            }

            return this.keyGather.Delete(id);
        }

        public void Update(long id, object obj)
        {
            var key = this.keyGather.GetRecord(id);
            if (key == null)
            {
                return;
            }

            var bytes = key.GetBytes();
            if (bytes.IsNullOrEmpty())
            {
                return;
            }

            var textId = BitConverter.ToInt64(bytes, 0);
            if (textId <= 0)
            {
                return;
            }

            if (obj is string str)
            {
                this.valueGather.Update(id, Encoding.UTF8.GetBytes(str));
            }
        }

        public IBlock GetBlock(long id, bool create = false)
        {
            throw new NotImplementedException();
        }

        public IBlock GetBlock(long id, object obj, long next = 0)
        {
            throw new NotImplementedException();
        }

        public Column GetColumn(long id)
        {
            var record = this.keyGather.GetRecord(id);
            if (record == null)
            {
                return null;
            }

            var bytes = record.GetBytes();
            if (bytes.IsNullOrEmpty())
            {
                return null;
            }

            var textId = BitConverter.ToInt64(bytes, 0);
            if (textId <= 0)
            {
                return null;
            }

            record = this.valueGather.GetRecord(textId);
            if (record == null)
            {
                return null;
            }

            return new Column
            {
                Id = id,
                Value = Encoding.UTF8.GetString(record.GetBytes())
            };
        }

        public void Dispose()
        {
            var time = DateTime.Now;
            this.keyGather.Dispose();
            this.valueGather.Dispose();
            this.rwls.Dispose();
            Log.Information($"{this.Database}/{this.Collection}/{this.name}.gather took {DateTime.Now - time} to dispose");
        }

        public void Destroy()
        {
            this.keyGather.Destroy();
            this.valueGather.Destroy();
            this.rwls.Dispose();
        }
    }
}
