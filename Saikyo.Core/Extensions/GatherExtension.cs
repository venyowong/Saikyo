using Saikyo.Core.Storage.Blocks;
using Saikyo.Core.Storage.Gathers;
using Saikyo.Core.Storage.Records;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

        public static bool Delete(this IGather gather, long id)
        {
            if (!gather.Records.TryRemove(id, out var record))
            {
                record = new Record(gather.Stream, id, gather.HeaderSize, gather.BlockCap.Value);
            }

            gather.UnusedBlocks.PushBlock(record.Head);
            return true;
        }
    }
}
