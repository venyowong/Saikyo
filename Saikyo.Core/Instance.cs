using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public static class Instance
    {
        public static Configuration Config { get; set; }

        internal static Kernel Kernel { get; set; }

        public static void Init()
        {
            Kernel = new Kernel();
            if (Config == null)
            {
                Config = new Configuration();
            }
        }

        public static Database Use(string db) => Kernel.GetDatabase(db);
    }
}
