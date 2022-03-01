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

        public Collection<T> GetCollection<T>(string name) where T : new()
            => Instance.Kernel.GetCollection<T>(this.Name, name);
    }
}
