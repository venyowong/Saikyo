using Saikyo.Core.Storage.Blocks;
using Saikyo.Core.Storage.Records;
using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Extensions
{
    internal static class RecordExtension
    {
        public static IBlock PopBlock(this IChainRecord record, long id = 0)
        {
            lock (record.Blocks)
            {
                if (record.Blocks.Count <= 1)
                {
                    return null;
                }

                if (id == 0)
                {
                    record.Blocks[record.Blocks.Count - 2].Next.Update(0); // blocks[-2] will be tail
                    var block = record.Blocks[record.Blocks.Count - 1];
                    record.Blocks.Remove(block);
                    return block;
                }
                else
                {
                    for (int i = 1; i < record.Blocks.Count; i++)
                    {
                        var block = record.Blocks[i];
                        if (block.Id != id)
                        {
                            continue;
                        }

                        if (i + 1 < record.Blocks.Count) // if this block is not tail
                        {
                            record.Blocks[i - 1].Next.Update(record.Blocks[i + 1].Id); // [i - 1] -> [i + 1], remove this block from the linked list
                        }
                        else
                        {
                            record.Blocks[i - 1].Next.Update(0); // [i - 1] will be tail
                        }
                        record.Blocks.Remove(block);
                        return block;
                    }
                    return null;
                }
            }
        }

        public static void PushBlock(this IChainRecord record, IBlock block)
        {
            lock (record.Blocks)
            {
                var lastBlock = record.Blocks[record.Blocks.Count - 1];
                lastBlock.Next.Update(block.Id);
                record.Blocks.Add(block);
            }
        }

        public static T GetValue<T>(this IRecord record)
        {
            var bytes = record?.Head.Data.Data;
            return bytes.FromBytes<T>();
        }
    }
}
