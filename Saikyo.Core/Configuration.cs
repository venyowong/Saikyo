using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core
{
    public class Configuration
    {
        /// <summary>
        /// The path for storing data
        /// </summary>
        public string DataPath { get; set; } = "data";

        public int ReaderWriterLockTimeout { get; set; } = 100;

        public int MaxTextBlockSize { get; set; } = 4096;
    }
}
