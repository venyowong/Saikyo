using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Exceptions
{
    public class ConfigurationException : Exception
    {
        public ConfigurationException(string msg) : base(msg) { }
    }
}
