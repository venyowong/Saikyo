using Saikyo.Core.Extensions;
using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Saikyo.Core.Storage.Blocks
{
    internal class AVLBlock<T> : IValueBlock<T>, ISizeBlock, IAVLNode<T> where T : IComparable<T>
    {
        public readonly int HeaderSize = 30;

        public T Value { get; private set; }

        public long Id { get; private set; }

        public int Cap { get; private set; }

        public long Offset { get; private set; }

        public FixedSizeStreamUnit<long> Next => this.right;

        public StreamUnit Data { get; private set; }

        public Stream Stream { get; private set; }

        public long Parent
        {
            get => this.parent.Value;
            set => this.parent.Update(value);
        }

        public long Left
        {
            get => this.left.Value;
            set => this.left.Update(value);
        }

        public byte LeftDepth
        {
            get => this.leftDepth.Value;
            set => this.leftDepth.Update(value);
        }

        public long Right
        {
            get => this.right.Value;
            set => this.right.Update(value);
        }

        public byte RightDepth
        {
            get => this.rightDepth.Value;
            set => this.rightDepth.Update(value);
        }

        public IAVLTree<T> Tree { get; private set; }

        public FixedSizeStreamUnit<int> DataSize { get; private set; }

        private FixedSizeStreamUnit<long> parent;
        private FixedSizeStreamUnit<long> left;
        private FixedSizeStreamUnit<byte> leftDepth;
        private FixedSizeStreamUnit<long> right;
        private FixedSizeStreamUnit<byte> rightDepth;

        public AVLBlock(Stream stream, long id, int offset, IAVLTree<T> tree, int cap = 0)
        {
            this.Stream = stream;
            this.Id = id;
            var type = typeof(T);
            if (Type.GetTypeCode(type) == TypeCode.String)
            {
                this.Cap = cap;
            }
            else
            {
                var dataSize = TypeHelper.GetTypeSize(type);
                this.Cap = dataSize + this.HeaderSize;
            }
            this.Offset = offset + id * this.Cap;
            this.parent = new FixedSizeStreamUnit<long>(stream, this.Offset);
            this.left = new FixedSizeStreamUnit<long>(stream, this.parent.Offset + this.parent.Cap);
            this.leftDepth = new FixedSizeStreamUnit<byte>(stream, this.left.Offset + this.left.Cap);
            this.right = new FixedSizeStreamUnit<long>(stream, this.leftDepth.Offset + this.leftDepth.Cap);
            this.rightDepth = new FixedSizeStreamUnit<byte>(stream, this.right.Offset + this.right.Cap);
            this.DataSize = new FixedSizeStreamUnit<int>(stream, this.rightDepth.Offset + this.rightDepth.Cap);
            this.Data = new StreamUnit(stream, this.DataSize.Offset + this.DataSize.Cap, this.Cap - this.HeaderSize, this.DataSize.Value);
            this.Value = this.Data.Data.FromBytes<T>();
            this.Tree = tree;
        }

        public AVLBlock(Stream stream, long id, int offset, T t, IAVLTree<T> tree, int cap = 0)
        {
            this.Stream = stream;
            this.Id = id;
            var type = typeof(T);
            if (Type.GetTypeCode(type) == TypeCode.String)
            {
                this.Cap = cap;
            }
            else
            {
                var dataSize = TypeHelper.GetTypeSize(type);
                this.Cap = dataSize + this.HeaderSize;
            }
            this.Offset = offset + id * this.Cap;
            this.parent = new FixedSizeStreamUnit<long>(stream, this.Offset);
            this.left = new FixedSizeStreamUnit<long>(stream, this.parent.Offset + this.parent.Cap);
            this.leftDepth = new FixedSizeStreamUnit<byte>(stream, this.left.Offset + this.left.Cap);
            this.right = new FixedSizeStreamUnit<long>(stream, this.leftDepth.Offset + this.leftDepth.Cap);
            this.rightDepth = new FixedSizeStreamUnit<byte>(stream, this.right.Offset + this.right.Cap);
            var data = t.ToBytes();
            this.DataSize = new FixedSizeStreamUnit<int>(stream, this.rightDepth.Offset + this.rightDepth.Cap, data.Length);
            this.Data = new StreamUnit(stream, this.DataSize.Offset + this.DataSize.Cap, this.Cap - this.HeaderSize, data);
            this.Value = t;
            this.Tree = tree;
        }

        public void Dispose()
        {
            this.parent?.Dispose();
            this.left?.Dispose();
            this.leftDepth?.Dispose();
            this.right?.Dispose();
            this.rightDepth?.Dispose();
            this.DataSize?.Dispose();
            this.Data?.Dispose();
        }

        public void Update(T value)
        {
            this.Value = value;
            this.Data.Update(value.ToBytes());
        }

        public void AddNode(IAVLNode<T> node)
        {
            lock (this)
            {
                if (this.Value.CompareTo(node.Value) > 0) // left
                {
                    if (this.Left <= 0) // left leef is empty, so set the node as left leef
                    {
                        node.Parent = this.Id;
                        this.Left = node.Id;
                        this.UpdateDepth(node.Id, 1);
                    }
                    else
                    {
                        this.Tree.GetNode(this.Left).AddNode(node);
                    }
                }
                else // right
                {
                    if (this.Right <= 0) // right leef is empty, so set the node as left leef
                    {
                        node.Parent = this.Id;
                        this.Right = node.Id;
                        this.UpdateDepth(node.Id, 1);
                    }
                    else
                    {
                        this.Tree.GetNode(this.Right).AddNode(node);
                    }
                }
            }
        }

        public void Delete()
        {
            if (this.Left <= 0)
            {
                if (this.Right <= 0) // leaf
                {
                    this.Tree.GetNode(this.Parent).UpdateDepth(this.Id, 0);
                    this.Parent = 0;
                }
                else // only right
                {
                    var parent = this.Tree.GetNode(this.Parent);
                    if (parent == null)
                    {
                        this.Tree.Root = this.Right;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, this.Right, this.Tree.GetNode(this.Right).GetDepth());
                    }
                }
            }
            else
            {
                if (this.Right <= 0) // only left
                {
                    var parent = this.Tree.GetNode(this.Parent);
                    if (parent == null)
                    {
                        this.Tree.Root = this.Left;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, this.Left, this.Tree.GetNode(this.Left).GetDepth());
                    }
                }
                else // both
                {
                    var replace = this.Tree.GetNode(this.Right).GetMinNode();
                    replace.Delete();

                    replace.Left = this.Left;
                    replace.LeftDepth = this.LeftDepth;
                    this.Left = 0;
                    this.LeftDepth = 0;
                    var left = this.Tree.GetNode(this.Left);
                    if (left != null)
                    {
                        left.Parent = replace.Id;
                    }

                    replace.Right = this.Right;
                    replace.RightDepth = this.RightDepth;
                    this.Right = 0;
                    this.RightDepth = 0;
                    var right = this.Tree.GetNode(this.Right);
                    if (right != null)
                    {
                        right.Parent = replace.Id;
                    }

                    var parent = this.Tree.GetNode(this.Parent);
                    if (parent == null)
                    {
                        this.Tree.Root = replace.Id;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, replace.Id, replace.GetDepth());
                    }
                }
            }
        }

        public List<IAVLNode<T>> GetTree()
        {
            List<IAVLNode<T>> list;
            if (this.Left > 0)
            {
                list = this.Tree.GetNode(this.Left).GetTree();
            }
            else
            {
                list = new List<IAVLNode<T>>();
            }
            list.Add(this);
            if (this.Right > 0)
            {
                list.AddRange(this.Tree.GetNode(this.Right).GetTree());
            }
            return list;
        }

        public IAVLNode<T> GetMinNode()
        {
            if (this.Left > 0)
            {
                return this.Tree.GetNode(this.Left).GetMinNode();
            }

            return this;
        }

        public List<IAVLNode<T>> Gt(T t)
        {
            if (this.Value.CompareTo(t) > 0)
            {
                List<IAVLNode<T>> list;
                if (this.Left > 0)
                {
                    list = this.Tree.GetNode(this.Left).Gt(t);
                }
                else
                {
                    list = new List<IAVLNode<T>>();
                }
                list.Add(this);
                if (this.Right > 0)
                {
                    list.AddRange(this.Tree.GetNode(this.Right).GetTree());
                }
                return list;
            }
            else
            {
                if (this.Right > 0)
                {
                    return this.Tree.GetNode(this.Right).Gt(t);
                }
            }

            return new List<IAVLNode<T>>();
        }

        public List<IAVLNode<T>> Gte(T t)
        {
            if (this.Value.CompareTo(t) >= 0)
            {
                List<IAVLNode<T>> list;
                if (this.Left > 0)
                {
                    list = this.Tree.GetNode(this.Left).Gte(t);
                }
                else
                {
                    list = new List<IAVLNode<T>>();
                }
                list.Add(this);
                if (this.Right > 0)
                {
                    list.AddRange(this.Tree.GetNode(this.Right).GetTree());
                }
                return list;
            }
            else
            {
                if (this.Right > 0)
                {
                    return this.Tree.GetNode(this.Right).Gte(t);
                }
            }

            return new List<IAVLNode<T>>();
        }

        public List<IAVLNode<T>> Lt(T t)
        {
            if (this.Value.CompareTo(t) < 0)
            {
                List<IAVLNode<T>> list;
                if (this.Left > 0)
                {
                    list = this.Tree.GetNode(this.Left).GetTree();
                }
                else
                {
                    list = new List<IAVLNode<T>>();
                }
                list.Add(this);
                if (this.Right > 0)
                {
                    list.AddRange(this.Tree.GetNode(this.Right).Lt(t));
                }
                return list;
            }
            else
            {
                if (this.Left > 0)
                {
                    return this.Tree.GetNode(this.Left).Lt(t);
                }
            }

            return new List<IAVLNode<T>>();
        }

        public List<IAVLNode<T>> Lte(T t)
        {
            if (this.Value.CompareTo(t) <= 0)
            {
                List<IAVLNode<T>> list;
                if (this.Left > 0)
                {
                    list = this.Tree.GetNode(this.Left).GetTree();
                }
                else
                {
                    list = new List<IAVLNode<T>>();
                }
                list.Add(this);
                if (this.Right > 0)
                {
                    list.AddRange(this.Tree.GetNode(this.Right).Lte(t));
                }
                return list;
            }
            else
            {
                if (this.Left > 0)
                {
                    return this.Tree.GetNode(this.Left).Lte(t);
                }
            }

            return new List<IAVLNode<T>>();
        }

        public List<IAVLNode<T>> Eq(T t)
        {
            if (this.Value.CompareTo(t) > 0)
            {
                if (this.Left > 0)
                {
                    return this.Tree.GetNode(this.Left).Eq(t);
                }
            }
            else
            {
                var list = new List<IAVLNode<T>>();
                if (this.Value.CompareTo(t) == 0)
                {
                    list.Add(this);
                }
                if (this.Right > 0)
                {
                    list.AddRange(this.Tree.GetNode(this.Right).Eq(t));
                }
                return list;
            }

            return new List<IAVLNode<T>>();
        }

        public void UpdateDepth(long id, byte depth, long path = 0)
        {
            if (this.Left == id)
            {
                if (this.LeftDepth == depth)
                {
                    return;
                }

                if (depth > this.LeftDepth) // add
                {
                    this.LeftDepth = depth;
                    path *= 2; // << 1 + 0

                    if (this.LeftDepth - this.RightDepth > 1)
                    {
                        var mode = path % 4;
                        if (mode == 0) // 00 LL
                        {
                            this.RotateLL();
                        }
                        else if (mode == 2) // 10 LR
                        {
                            this.Tree.GetNode(this.Left).RotateRR(false);
                            this.RotateLL();
                        }
                    }
                }
                else // delete
                {
                    this.LeftDepth = depth;
                    if (depth == 0)
                    {
                        this.Left = 0;
                    }

                    if (this.RightDepth - this.LeftDepth > 1)
                    {
                        var right = this.Tree.GetNode(this.Right);
                        var p = right.GetRightLongestPath(1);
                        while (p >= 4)
                        {
                            p /= 2;
                        }
                        if (p == 3) // 11 RR
                        {
                            this.RotateRR();
                        }
                        else if (p == 2) // 10 RL
                        {
                            right.RotateLL(false);
                            this.RotateRR();
                        }
                    }
                }
            }
            else if (this.Right == id)
            {
                if (this.RightDepth == depth)
                {
                    return;
                }

                if (depth > this.RightDepth) // add
                {
                    this.RightDepth = depth;
                    path = path * 2 + 1; // << 1 + 1

                    if (this.RightDepth - this.LeftDepth > 1)
                    {
                        var mode = path % 4;
                        if (mode == 1) // 01 RL
                        {
                            this.Tree.GetNode(this.Right).RotateLL(false);
                            this.RotateRR();
                        }
                        else if (mode == 3) // 11 RR
                        {
                            this.RotateRR();
                        }
                    }
                }
                else // delete
                {
                    this.RightDepth = depth;
                    if (depth == 0)
                    {
                        this.Right = 0;
                    }

                    if (this.LeftDepth - this.RightDepth > 1)
                    {
                        var left = this.Tree.GetNode(this.Left);
                        var p = left.GetRightLongestPath(0);
                        while (p >= 4)
                        {
                            p /= 2;
                        }
                        if (p == 1) // 01 LR
                        {
                            left.RotateRR(false);
                            this.RotateLL();
                        }
                        else if (p == 0) // 00 LL
                        {
                            this.RotateLL();
                        }
                    }
                }
            }
            if (this.Parent > 0)
            {
                this.Tree.GetNode(this.Parent).UpdateDepth(this.Id, this.GetDepth(), path);
            }
        }

        public void ChangeBlock(long oldId, long newId, byte depth, bool updateParentDepth = true)
        {
            if (this.Left == oldId)
            {
                this.Left = newId;
                this.LeftDepth = depth;
            }
            else if (this.Right == oldId)
            {
                this.Right = newId;
                this.RightDepth = depth;
            }
            if (updateParentDepth)
            {
                this.Tree.GetNode(this.Parent)?.UpdateDepth(this.Id, this.GetDepth());
            }
        }

        public void RotateLL(bool updateParentDepth = true)
        {
            var parent = this.Tree.GetNode(this.Parent);
            var left = this.Tree.GetNode(this.Left);
            var lr = this.Tree.GetNode(left.Right);
            if (lr != null)
            {
                lr.Parent = this.Id;
                this.Left = lr.Id;
                this.LeftDepth = lr.GetDepth();
            }
            else
            {
                this.Left = 0;
                this.LeftDepth = 0;
            }
            this.Parent = left.Id;
            left.Right = this.Id;
            left.RightDepth = this.GetDepth();

            if (parent != null)
            {
                left.Parent = parent.Id;
                parent.ChangeBlock(this.Id, left.Id, left.GetDepth(), updateParentDepth);
            }
            else
            {
                left.Parent = 0;
                this.Tree.Root = left.Id;
            }
        }

        public void RotateRR(bool updateParentDepth = true)
        {
            var parent = this.Tree.GetNode(this.Parent);
            var right = this.Tree.GetNode(this.Right);
            var rl = this.Tree.GetNode(right.Left);
            if (rl != null)
            {
                rl.Parent = this.Id;
                this.Right = rl.Id;
                this.RightDepth = rl.GetDepth();
            }
            else
            {
                this.Right = 0;
                this.RightDepth = 0;
            }
            this.Parent = right.Id;
            right.Left = this.Id;
            right.LeftDepth = this.GetDepth();

            if (parent != null)
            {
                right.Parent = parent.Id;
                parent.ChangeBlock(this.Id, right.Id, right.GetDepth(), updateParentDepth);
            }
            else
            {
                right.Parent = 0;
                this.Tree.Root = right.Id;
            }
        }

        public long GetRightLongestPath(long path)
        {
            if (this.Right > 0)
            {
                return this.Tree.GetNode(this.Right).GetRightLongestPath(path * 2 + 1);
            }
            if (this.Left > 0)
            {
                return this.Tree.GetNode(this.Left).GetRightLongestPath(path * 2);
            }
            return path;
        }

        public long GetLeftLongestPath(long path)
        {
            if (this.Left > 0)
            {
                return this.Tree.GetNode(this.Left).GetLeftLongestPath(path * 2 + 1);
            }
            if (this.Right > 0)
            {
                return this.Tree.GetNode(this.Right).GetLeftLongestPath(path * 2);
            }
            return path;
        }
    }
}
