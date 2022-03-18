using Saikyo.Core.Attributes;
using Saikyo.Core.Helpers;
using Saikyo.Core.Storage.Blocks;
using Saikyo.Core.Storage.Gathers;
using Saikyo.Core.Storage.Records;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Saikyo.Core.Extensions
{
    internal static class GatherExtension
    {
        public static bool TryUseBlockId(this IGather gather, long id)
        {
            if (id <= gather.LatestBlockId.Value) // old id
            {
                if (!gather.UnusedBlocks.Blocks.Any(b => b.Id == id))
                {
                    return false; // this id is in use
                }
                var block = gather.UnusedBlocks.PopBlock(id);
                if (block == null)
                {
                    return false;
                }

                return true;
            }
            else
            {
                gather.LatestBlockId.Update(gather.LatestBlockId.Value + 1);
                while (gather.LatestBlockId.Value < id)
                {
                    var block = new Block(gather.Stream, gather.LatestBlockId.Value, gather.BlockCap.Value, gather.HeaderSize, true);
                    gather.UnusedBlocks.PushBlock(block);
                    gather.LatestBlockId.Update(gather.LatestBlockId.Value + 1);
                }
                return true;
            }
        }

        public static long GetFreeBlockId(this IGather gather)
        {
            var block = gather.UnusedBlocks.PopBlock();
            if (block != null)
            {
                return block.Id;
            }

            gather.LatestBlockId.Update(gather.LatestBlockId.Value + 1);
            return gather.LatestBlockId.Value;
        }

        private static ConcurrentDictionary<string, ConstructorInfo> _gatherConstractors = new ConcurrentDictionary<string, ConstructorInfo>();
        public static dynamic CreateGather(this Type type, string path, string name, int blockCap = 0)
        {
            if (Type.GetTypeCode(type) == TypeCode.String)
            {
                if (blockCap <= 0)
                {
                    return new TextGather(path, name);
                }
                else if (blockCap > 200)
                {
                    return new TextGather(path, name, blockCap + 12);
                }
                else
                {
                    return new AVLGather<string>(path, name, blockCap + 30);
                }
            }

            blockCap = TypeHelper.GetTypeSize(type) + 30;
            if (_gatherConstractors.ContainsKey(type.Name))
            {
                return _gatherConstractors[type.Name].Invoke(new object[] { path, name, blockCap + 30 });
            }

            lock (_gatherConstractors)
            {
                if (_gatherConstractors.ContainsKey(type.Name))
                {
                    return _gatherConstractors[type.Name].Invoke(new object[] { path, name, blockCap + 30 });
                }

                var constructor = typeof(AVLGather<>).MakeGenericType(type).GetConstructors().First();
                _gatherConstractors.TryAdd(type.Name, constructor);
                return constructor.Invoke(new object[] { path, name, blockCap + 30 });
            }
        }

        public static dynamic CreateGather(this PropertyInfo property, string path)
        {
            if (Type.GetTypeCode(property.PropertyType) == TypeCode.String)
            {
                var size = property.GetCustomAttribute<SizeAttribute>();
                if (size.Size <= 0)
                {
                    return new TextGather(path, property.Name);
                }
                else if (size.Size > 200)
                {
                    return new TextGather(path, property.Name, size.Size);
                }
                else
                {
                    return new AVLGather<string>(path, property.Name, size.Size);
                }
            }

            var blockCap = TypeHelper.GetTypeSize(property.PropertyType) + 30;

            if (_gatherConstractors.ContainsKey(property.PropertyType.Name))
            {
                return _gatherConstractors[property.PropertyType.Name].Invoke(new object[] { path, property.Name, blockCap });
            }

            lock (_gatherConstractors)
            {
                if (_gatherConstractors.ContainsKey(property.PropertyType.Name))
                {
                    return _gatherConstractors[property.PropertyType.Name].Invoke(new object[] { path, property.Name, blockCap });
                }

                var constructor = typeof(AVLGather<>).MakeGenericType(property.PropertyType).GetConstructors().First();
                _gatherConstractors.TryAdd(property.PropertyType.Name, constructor);
                return (IGather)constructor.Invoke(new object[] { path, property.Name, blockCap });
            }
        }

        public static Task StartTimingFlush(this IGather gather, CancellationToken token)
        {
            var task = Task.Run(() =>
            {
                while (true)
                {
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    Thread.Sleep(3000);
                    var time = DateTime.Now;
                    gather.Flush();
                    var took = DateTime.Now - time;
                    if (took.TotalSeconds >= 1)
                    {
                        Log.Information($"{gather.File.FullName} has been flushed, took {took}");
                    }
                }
            });
            return task;
        }
    }
}
