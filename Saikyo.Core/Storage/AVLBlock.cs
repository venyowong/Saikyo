using Saikyo.Core.Extensions;
using System;
using System.Collections.Generic;

namespace Saikyo.Core.Storage
{
    internal class AVLBlock<T> : BaseBlock where T : IComparable<T>
    {
        public T Value { get; private set; }
        
        public byte Depth
        {
            get
            {
                return (byte)(this.leftDepth > this.rightDepth ? this.leftDepth + 1 : this.rightDepth + 1);
            }
        }

        public override long Next
        {
            get { return this.right; }
            set 
            {
                this.rwls.WriteLock(() =>
                {
                    this.right = value;
                });
            }
        }

        private long parent;
        private long left;
        private byte leftDepth;
        private long right;
        private byte rightDepth;
        private BinaryGather<T> gather;

        public AVLBlock(long id, BinaryGather<T> gather) 
            : base(gather.Stream, gather.HeaderSize, id, gather.BlockSize)
        {
            this.gather = gather;

            if (this.State != 1)
            {
                this.Value = this.Data.FromBytes<T>();
            }
        }

        public AVLBlock(long id, T t, BinaryGather<T> gather) 
            : base(gather.Stream, gather.HeaderSize, id, gather.BlockSize, t.ToBytes())
        {
            this.Value = t;
            this.gather = gather;
            this.HeaderSize = Const.AVLBlockHeaderSize;
        }

        public void AddBlock(AVLBlock<T> block)
        {
            this.rwls.WriteLock(() =>
            {
                if (this.Value.CompareTo(block.Value) > 0) // left
                {
                    if (this.left <= 0) // left leef is empty, so set the node as left leef
                    {
                        block.parent = this.Id;
                        this.left = block.Id;
                        this.UpdateDepth(block.Id, 1);
                        this.changed = true;
                    }
                    else
                    {
                        this.gather.GetBlock(this.left).AddBlock(block);
                    }
                }
                else // right
                {
                    if (this.right <= 0) // right leef is empty, so set the node as left leef
                    {
                        block.parent = this.Id;
                        this.right = block.Id;
                        this.UpdateDepth(block.Id, 1);
                        this.changed = true;
                    }
                    else
                    {
                        this.gather.GetBlock(this.right).AddBlock(block);
                    }
                }
            });
        }

        public void Delete()
        {
            if (this.left <= 0)
            {
                if (this.right <= 0) // leaf
                {
                    this.gather.GetBlock(this.parent).UpdateDepth(this.Id, 0);
                    this.parent = 0;
                }
                else // only right
                {
                    var parent = this.gather.GetBlock(this.parent);
                    if (parent == null)
                    {
                        this.gather.Root = this.right;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, this.right, this.gather.GetBlock(this.right).Depth);
                    }
                }
            }
            else
            {
                if (this.right <= 0) // only left
                {
                    var parent = this.gather.GetBlock(this.parent);
                    if (parent == null)
                    {
                        this.gather.Root = this.left;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, this.left, this.gather.GetBlock(this.left).Depth);
                    }
                }
                else // both
                {
                    this.changed = true;

                    var replace = this.gather.GetBlock(this.right).GetMinBlock();
                    replace.Delete();
                    replace.changed = true;

                    replace.left = this.left;
                    replace.leftDepth = this.leftDepth;
                    this.left = 0;
                    this.leftDepth = 0;
                    var left = this.gather.GetBlock(this.left);
                    if (left != null)
                    {
                        left.parent = replace.Id;
                        left.changed = true;
                    }

                    replace.right = this.right;
                    replace.rightDepth = this.rightDepth;
                    this.right = 0;
                    this.rightDepth = 0;
                    var right = this.gather.GetBlock(this.right);
                    if (right != null)
                    {
                        right.parent = replace.Id;
                        right.changed = true;
                    }

                    var parent = this.gather.GetBlock(this.parent);
                    if (parent == null)
                    {
                        this.gather.Root =replace.Id;
                    }
                    else
                    {
                        parent.ChangeBlock(this.Id, replace.Id, replace.Depth);
                    }
                }
            }
        }

        public override void Update(object data)
        {
            if (data == null)
            {
                return;
            }

            if (data is T t)
            {
                if (this.Value.CompareTo(t) == 0)
                {
                    return;
                }

                this.Delete();
                this.Value = t;
                this.Data = t.ToBytes();
                this.DataSize = this.Data.Length;
                this.changed = true;
                this.gather.GetBlock(this.gather.Root).AddBlock(this);
            }
        }

        public List<Column> GetTree()
        {
            return this.rwls.ReadLock(() =>
            {
                List<Column> list;
                if (this.left > 0)
                {
                    list = this.gather.GetBlock(this.left).GetTree();
                }
                else
                {
                    list = new List<Column>();
                }
                list.Add(new Column
                {
                    Id = this.Id,
                    Value = this.Value
                });
                if (this.right > 0)
                {
                    list.AddRange(this.gather.GetBlock(this.right).GetTree());
                }
                return list;
            });
        }

        public AVLBlock<T> GetMinBlock()
        {
            if (this.left > 0)
            {
                return this.gather.GetBlock(this.left).GetMinBlock();
            }

            return this;
        }

        public List<Column> Gt(T t)
        {
            return this.rwls.ReadLock(() =>
            {
                if (this.Value.CompareTo(t) > 0)
                {
                    List<Column> list;
                    if (this.left > 0)
                    {
                        list = this.gather.GetBlock(this.left).Gt(t);
                    }
                    else
                    {
                        list = new List<Column>();
                    }
                    list.Add(new Column
                    {
                        Id = this.Id,
                        Value = this.Value
                    });
                    if (this.right > 0)
                    {
                        list.AddRange(this.gather.GetBlock(this.right).GetTree());
                    }
                    return list;
                }
                else
                {
                    if (this.right > 0)
                    {
                        return this.gather.GetBlock(this.right).Gt(t);
                    }
                }

                return new List<Column>();
            });
        }

        public List<Column> Gte(T t)
        {
            return this.rwls.ReadLock(() =>
            {
                if (this.Value.CompareTo(t) >= 0)
                {
                    List<Column> list;
                    if (this.left > 0)
                    {
                        list = this.gather.GetBlock(this.left).Gte(t);
                    }
                    else
                    {
                        list = new List<Column>();
                    }
                    list.Add(new Column
                    {
                        Id = this.Id,
                        Value = this.Value
                    });
                    if (this.right > 0)
                    {
                        list.AddRange(this.gather.GetBlock(this.right).GetTree());
                    }
                    return list;
                }
                else
                {
                    if (this.right > 0)
                    {
                        return this.gather.GetBlock(this.right).Gte(t);
                    }
                }

                return new List<Column>();
            });
        }

        public List<Column> Lt(T t)
        {
            return this.rwls.ReadLock(() =>
            {
                if (this.Value.CompareTo(t) < 0)
                {
                    List<Column> list;
                    if (this.left > 0)
                    {
                        list = this.gather.GetBlock(this.left).GetTree();
                    }
                    else
                    {
                        list = new List<Column>();
                    }
                    list.Add(new Column
                    {
                        Id = this.Id,
                        Value = this.Value
                    });
                    if (this.right > 0)
                    {
                        list.AddRange(this.gather.GetBlock(this.right).Lt(t));
                    }
                    return list;
                }
                else
                {
                    if (this.left > 0)
                    {
                        return this.gather.GetBlock(this.left).Lt(t);
                    }
                }

                return new List<Column>();
            });
        }

        public List<Column> Lte(T t)
        {
            return this.rwls.ReadLock(() =>
            {
                if (this.Value.CompareTo(t) <= 0)
                {
                    List<Column> list;
                    if (this.left > 0)
                    {
                        list = this.gather.GetBlock(this.left).GetTree();
                    }
                    else
                    {
                        list = new List<Column>();
                    }
                    list.Add(new Column
                    {
                        Id = this.Id,
                        Value = this.Value
                    });
                    if (this.right > 0)
                    {
                        list.AddRange(this.gather.GetBlock(this.right).Lte(t));
                    }
                    return list;
                }
                else
                {
                    if (this.left > 0)
                    {
                        return this.gather.GetBlock(this.left).Lte(t);
                    }
                }

                return new List<Column>();
            });
        }

        public List<Column> Eq(T t)
        {
            return this.rwls.ReadLock(() =>
            {
                if (this.Value.CompareTo(t) > 0)
                {
                    if (this.left > 0)
                    {
                        return this.gather.GetBlock(this.left).Eq(t);
                    }
                }
                else
                {
                    var list = new List<Column>();
                    if (this.Value.CompareTo(t) == 0)
                    {
                        list.Add(new Column
                        {
                            Id = this.Id,
                            Value = this.Value
                        });
                    }
                    if (this.right > 0)
                    {
                        list.AddRange(this.gather.GetBlock(this.right).Eq(t));
                    }
                    return list;
                }

                return new List<Column>();
            });
        }

        public override void Dispose()
        {
            if (!this.changed)
            {
                return;
            }

            base.Dispose();

            var startPosition = this.Id * this.blockSize + this.gatherHeaderSize + Const.BaseBlockHeaderSize;
            this.stream.Write(startPosition, BitConverter.GetBytes(this.parent));
            this.stream.Write(startPosition + 8, BitConverter.GetBytes(this.left));
            this.stream.Write(startPosition + 16, new byte[1] { this.leftDepth });
            this.stream.Write(startPosition + 17, new byte[1] { this.rightDepth });
        }

        public override string ToString()
        {
            var str = this.Value.ToString();
            if (this.left > 0)
            {
                str = $"({this.gather.GetBlock(this.left)})-{str}";
            }
            if (this.right > 0)
            {
                str = $"{str}-({this.gather.GetBlock(this.right)})";
            }
            return str;
        }

        protected override void InitHeader()
        {
            if (this.State != 1)
            {
                var startPosition = this.Id * blockSize + gatherHeaderSize + Const.BaseBlockHeaderSize;
                this.parent = this.stream.ReadAsLong(startPosition);
                this.left = this.stream.ReadAsLong(startPosition + 8);
                this.leftDepth = this.stream.ReadAsByte(startPosition + 16);
                this.rightDepth = this.stream.ReadAsByte(startPosition + 17);
            }
            this.HeaderSize = Const.AVLBlockHeaderSize;
        }

        private void UpdateDepth(long id, byte depth, long path = 0)
        {
            if (this.left == id)
            {
                if (this.leftDepth == depth)
                {
                    return;
                }

                if (depth > this.leftDepth) // add
                {
                    this.leftDepth = depth;
                    path *= 2; // << 1 + 0
                    this.changed = true;

                    if (this.leftDepth - this.rightDepth > 1)
                    {
                        var mode = path % 4;
                        if (mode == 0) // 00 LL
                        {
                            this.RotateLL();
                        }
                        else if (mode == 2) // 10 LR
                        {
                            this.gather.GetBlock(this.left).RotateRR(false);
                            this.RotateLL();
                        }
                    }
                }
                else // delete
                {
                    this.leftDepth = depth;
                    if (depth == 0)
                    {
                        this.left = 0;
                    }
                    this.changed = true;

                    if (this.rightDepth - this.leftDepth > 1)
                    {
                        var right = this.gather.GetBlock(this.right);
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
            else if (this.right == id)
            {
                if (this.rightDepth == depth)
                {
                    return;
                }

                if (depth > this.rightDepth) // add
                {
                    this.rightDepth = depth;
                    path = path * 2 + 1; // << 1 + 1
                    this.changed = true;

                    if (this.rightDepth - this.leftDepth > 1)
                    {
                        var mode = path % 4;
                        if (mode == 1) // 01 RL
                        {
                            this.gather.GetBlock(this.right).RotateLL(false);
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
                    this.rightDepth = depth;
                    if (depth == 0)
                    {
                        this.right = 0;
                    }
                    this.changed = true;

                    if (this.leftDepth - this.rightDepth > 1)
                    {
                        var left = this.gather.GetBlock(this.left);
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
            if (this.parent > 0)
            {
                this.gather.GetBlock(this.parent).UpdateDepth(this.Id, this.Depth, path);
            }
        }

        private void ChangeBlock(long oldId, long newId, byte depth, bool updateParentDepth = true)
        {
            if (this.left == oldId)
            {
                this.left = newId;
                this.leftDepth = depth;
                this.changed = true;
            }
            else if (this.right == oldId)
            {
                this.right = newId;
                this.rightDepth = depth;
                this.changed = true;
            }
            if (updateParentDepth)
            {
                this.gather.GetBlock(this.parent)?.UpdateDepth(this.Id, this.Depth);
            }
        }

        private void RotateLL(bool updateParentDepth = true)
        {
            this.changed = true;
            var parent = this.gather.GetBlock(this.parent);
            var left = this.gather.GetBlock(this.left);
            var lr = this.gather.GetBlock(left.right);
            if (lr != null)
            {
                lr.parent = this.Id;
                lr.changed = true;
                this.left = lr.Id;
                this.leftDepth = lr.Depth;
            }
            else
            {
                this.left = 0;
                this.leftDepth = 0;
            }
            this.parent = left.Id;
            left.right = this.Id;
            left.rightDepth = this.Depth;
            left.changed = true;

            if (parent != null)
            {
                left.parent = parent.Id;
                parent.ChangeBlock(this.Id, left.Id, left.Depth, updateParentDepth);
            }
            else
            {
                left.parent = 0;
                this.gather.Root = left.Id;
            }
        }

        private void RotateRR(bool updateParentDepth = true)
        {
            this.changed = true;
            var parent = this.gather.GetBlock(this.parent);
            var right = this.gather.GetBlock(this.right);
            var rl = this.gather.GetBlock(right.left);
            if (rl != null)
            {
                rl.parent = this.Id;
                rl.changed = true;
                this.right = rl.Id;
                this.rightDepth = rl.Depth;
            }
            else
            {
                this.right = 0;
                this.rightDepth = 0;
            }
            this.parent = right.Id;
            right.left = this.Id;
            right.leftDepth = this.Depth;
            right.changed = true;

            if (parent != null)
            {
                right.parent = parent.Id;
                parent.ChangeBlock(this.Id, right.Id, right.Depth, updateParentDepth);
            }
            else
            {
                right.parent = 0;
                this.gather.Root = right.Id;
            }
        }

        private long GetRightLongestPath(long path)
        {
            if (this.right > 0)
            {
                return this.gather.GetBlock(this.right).GetRightLongestPath(path * 2 + 1);
            }
            if (this.left > 0)
            {
                return this.gather.GetBlock(this.left).GetRightLongestPath(path * 2);
            }
            return path;
        }

        private long GetLeftLongestPath(long path)
        {
            if (this.left > 0)
            {
                return this.gather.GetBlock(this.left).GetLeftLongestPath(path * 2 + 1);
            }
            if (this.right > 0)
            {
                return this.gather.GetBlock(this.right).GetLeftLongestPath(path * 2);
            }
            return path;
        }
    }
}
