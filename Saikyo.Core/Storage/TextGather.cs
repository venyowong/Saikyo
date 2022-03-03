using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Saikyo.Core.Storage
{
    internal class TextGather : IInserter<string>, IDisposable, IColumnGetter, IDeleter, IDestroyer
    {
        public string Database { get; private set; }

        public string Collection { get; private set; }

        private DataGather keyGather;
        private DataGather valueGather;
        private ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

        public TextGather(string database, string collection, string name)
        {
            this.Database = database;
            this.Collection = collection;
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
            this.keyGather.Dispose();
            this.valueGather.Dispose();
            this.rwls.Dispose();
        }

        public void Destroy()
        {
            this.keyGather.Destroy();
            this.valueGather.Destroy();
            this.rwls.Dispose();
        }
    }
}
