using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Attributes
{
    /// <summary>
    /// The key property in object
    /// <para>When query all data, items in result list will be sorted by this property</para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }
}
