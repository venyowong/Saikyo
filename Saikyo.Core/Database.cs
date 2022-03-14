using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public class Database
    {
        public string Name { get; private set; }

        public Database(string name)
        {
            this.Name = name;
        }

        public Collection<T> GetCollection<T>(string name, string dataPath = "data") where T : new()
            => new Collection<T>(this.Name, name, dataPath);

        public Collection GetCollection(string name, string dataPath = "data") => new Collection(this.Name, name, dataPath);
    }
}
