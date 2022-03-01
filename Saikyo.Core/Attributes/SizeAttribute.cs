using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Attributes
{
    /// <summary>
    /// You should set the size from the beginning and don't modify it
    /// <para>If you modify this size, the data file will doesn't work</para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class SizeAttribute : Attribute
    {
        public int Size { get; set; }

        public SizeAttribute(int size)
        {
            Size = size;
        }
    }
}
