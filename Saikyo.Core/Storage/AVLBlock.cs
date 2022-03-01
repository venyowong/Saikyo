using Saikyo.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Saikyo.Core.Storage
{
    internal class AVLBlock<T> : BaseBlock where T : IComparable<T>
    {
        public T Value { get; private set; }

        public long Parent { get; set; }

        public long Left { get; set; }

        public byte LeftDepth { get; set; }

        public long Right { get; set; }

        public byte RightDepth { get; set; }

        public byte Depth
        {
            get
            {
                return (byte)(this.LeftDepth > this.RightDepth ? this.LeftDepth + 1 : this.RightDepth + 1);
            }
        }

        private BinaryGather<T> gather;

        public AVLBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, BinaryGather<T> gather) 
            : base(stream, gatherHeaderSize, id, blockSize)
        {
            this.gather = gather;

            if (this.State != 1)
            {
                this.Value = this.Data.FromBytes<T>();
            }
        }

        public AVLBlock(Stream stream, int gatherHeaderSize, long id, int blockSize, T t, BinaryGather<T> gather) 
            : base(stream, gatherHeaderSize, id, blockSize, t.ToBytes())
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
                    if (this.Left <= 0) // left leef is empty, so set the node as left leef
                    {
                        block.Parent = this.Id;
                        this.Left = block.Id;
                        this.UpdateDepth(block.Id, 1);
                        this.changed = true;
                    }
                    else
                    {
                        this.gather.GetBlock(this.Left).AddBlock(block);
                    }
                }
                else // right
                {
                    if (this.Right <= 0) // right leef is empty, so set the node as left leef
                    {
                        block.Parent = this.Id;
                        this.Right = block.Id;
                        this.UpdateDepth(block.Id, 1);
                        this.changed = true;
                    }
                    else
                    {
                        this.gather.GetBlock(this.Right).AddBlock(block);
                    }
                }
            });
        }

        public List<Column> GetTree()
        {
            List<Column> list;
            if (this.Left > 0)
            {
                list = this.gather.GetBlock(this.Left).GetTree();
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
            if (this.Right > 0)
            {
                list.AddRange(this.gather.GetBlock(this.Right).GetTree());
            }
            return list;
        }

        public override void Dispose()
        {
            if (!this.changed)
            {
                return;
            }

            base.Dispose();

            var startPosition = this.Id * this.blockSize + this.gatherHeaderSize + Const.BaseBlockHeaderSize;
            this.stream.Write(startPosition, BitConverter.GetBytes(this.Parent));
            this.stream.Write(startPosition + 8, BitConverter.GetBytes(this.Left));
            this.stream.Write(startPosition + 16, new byte[1] { this.LeftDepth });
            this.stream.Write(startPosition + 17, BitConverter.GetBytes(this.Right));
            this.stream.Write(startPosition + 25, new byte[1] { this.RightDepth });
        }

        public override string ToString()
        {
            var str = this.Value.ToString();
            if (this.Left > 0)
            {
                str = $"{this.gather.GetBlock(this.Left)} {str}";
            }
            if (this.Right > 0)
            {
                str = $"{str} {this.gather.GetBlock(this.Right)}";
            }
            return str;
        }

        protected override void InitData()
        {
            if (this.State != 1)
            {
                var startPosition = this.Id * blockSize + gatherHeaderSize + Const.BaseBlockHeaderSize;
                this.Parent = this.stream.ReadAsLong(startPosition);
                this.Left = this.stream.ReadAsLong(startPosition + 8);
                this.LeftDepth = this.stream.ReadAsByte(startPosition + 16);
                this.Right = this.stream.ReadAsLong(startPosition + 17);
                this.RightDepth = this.stream.ReadAsByte(startPosition + 25);
            }
            this.HeaderSize = Const.AVLBlockHeaderSize;
        }

        private void UpdateDepth(long id, byte depth, int path = 0)
        {
            if (this.Left == id)
            {
                if (this.LeftDepth == depth)
                {
                    return;
                }

                this.LeftDepth = depth;
                path *= 2; // << 1 + 0
                this.changed = true;

                if (this.LeftDepth - this.RightDepth > 1)
                {
                    var mode = path % 4;
                    if (mode == 0) // 00 LL
                    {
                        this.RotateLL();
                    }
                    else if (mode == 2) // 10 LR
                    {
                        this.gather.GetBlock(this.Left).RotateRR();
                        this.RotateLL();
                    }
                }
            }
            else if (this.Right == id)
            {
                if (this.RightDepth == depth)
                {
                    return;
                }

                this.RightDepth = depth;
                path = path * 2 + 1; // << 1 + 1
                this.changed = true;

                if (this.RightDepth - this.LeftDepth > 1)
                {
                    var mode = path % 4;
                    if (mode == 1) // 01 RL
                    {
                        this.gather.GetBlock(this.Right).RotateLL();
                        this.RotateRR();
                    }
                    else if (mode == 3) // 11 RR
                    {
                        this.RotateRR();
                    }
                }
            }
            if (this.Parent > 0)
            {
                this.gather.GetBlock(this.Parent).UpdateDepth(this.Id, this.Depth, path);
            }
        }

        private void ChangeBlock(long oldId, long newId, byte depth)
        {
            if (this.Left == oldId)
            {
                this.Left = newId;
                this.LeftDepth = depth;
                this.changed = true;
            }
            else if (this.Right == oldId)
            {
                this.Right = newId;
                this.RightDepth = depth;
                this.changed = true;
            }
            this.gather.GetBlock(this.Parent)?.UpdateDepth(this.Id, this.Depth);
        }

        private void RotateLL()
        {
            this.changed = true;
            var parent = this.gather.GetBlock(this.Parent);
            var left = this.gather.GetBlock(this.Left);
            var lr = this.gather.GetBlock(left.Right);
            if (lr != null)
            {
                lr.Parent = this.Id;
                this.Left = lr.Id;
                this.LeftDepth = lr.Depth;
            }
            else
            {
                this.Left = 0;
                this.LeftDepth = 0;
            }
            this.Parent = left.Id;
            left.Right = this.Id;
            left.RightDepth = this.Depth;

            if (parent != null)
            {
                left.Parent = parent.Id;
                parent.ChangeBlock(this.Id, left.Id, this.Depth);
            }
            else
            {
                left.Parent = 0;
                this.gather.Root = left.Id;
            }
        }

        private void RotateRR()
        {
            this.changed = true;
            var parent = this.gather.GetBlock(this.Parent);
            var right = this.gather.GetBlock(this.Right);
            var rl = this.gather.GetBlock(right.Left);
            if (rl != null)
            {
                rl.Parent = this.Id;
                this.Right = rl.Id;
                this.RightDepth = rl.Depth;
            }
            else
            {
                this.Right = 0;
                this.RightDepth = 0;
            }
            this.Parent = right.Id;
            right.Left = this.Id;
            right.LeftDepth = this.Depth;

            if (parent != null)
            {
                right.Parent = parent.Id;
                parent.ChangeBlock(this.Id, right.Id, this.Depth);
            }
            else
            {
                right.Parent = 0;
                this.gather.Root = right.Id;
            }
        }
    }
}
