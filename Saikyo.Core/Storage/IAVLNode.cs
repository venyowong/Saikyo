using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Storage
{
    internal interface IAVLNode<T> where T : IComparable<T>
    {
        long Id { get; }

        long Parent { get; set; }

        long Left { get; set; }

        byte LeftDepth { get; set; }

        long Right { get; set; }

        byte RightDepth { get; set; }

        IAVLTree<T> Tree { get; }

        T Value { get; }

        void AddNode(IAVLNode<T> node);

        void Delete();

        List<IAVLNode<T>> GetTree();

        IAVLNode<T> GetMinNode();

        List<IAVLNode<T>> Gt(T t);

        List<IAVLNode<T>> Gte(T t);

        List<IAVLNode<T>> Lt(T t);

        List<IAVLNode<T>> Lte(T t);

        List<IAVLNode<T>> Eq(T t);

        void UpdateDepth(long id, byte depth, long path = 0);

        void ChangeBlock(long oldId, long newId, byte depth, bool updateParentDepth = true);

        void RotateLL(bool updateParentDepth = true);

        void RotateRR(bool updateParentDepth = true);

        long GetRightLongestPath(long path);

        long GetLeftLongestPath(long path);
    }
}
