using Saikyo.Core.Storage;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Extensions
{
    internal static class NodeExtension
    {
        public static byte GetDepth<T>(this IAVLNode<T> node) where T : IComparable<T> 
            => (byte)(node.LeftDepth > node.RightDepth ? node.LeftDepth + 1 : node.RightDepth + 1);

        public static Column ToColumn<T>(this IAVLNode<T> node) where T : IComparable<T>
            => new Column
            {
                Id = node.Id,
                Value = node.Value
            };
    }
}
