using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Attributes
{
    /// <summary>
    /// used to mark which property need to be ignored
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreAttribute : Attribute
    {
    }
}
