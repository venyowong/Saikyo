using Saikyo.Core.Extensions;
using Serilog;
using System;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal class TextGather : IInserter<string>, IDisposable, IColumnGetter, IBlockDeleter, IDestroyer, IUpdater<string>
    {
        public string Database { get; private set; }

        public string Collection { get; private set; }

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

        public long AddData(string t)
        {
            return this.rwls.WriteLock(() =>
            {
                var valueId = 0L;
                if (!string.IsNullOrWhiteSpace(t))
                {
                    valueId = this.valueGather.AddData(Encoding.UTF8.GetBytes(t));
                }
                return this.keyGather.AddData(BitConverter.GetBytes(valueId));
            });
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

        public void Update(long id, string str)
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

            this.valueGather.Update(id, Encoding.UTF8.GetBytes(str));
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
