using System;
using System.Collections.Generic;
using System.Text;

namespace PutBlockSample
{
    public class Block
    {
        public string BlockID { get; set; }
        public long Start { get; set; }
        public long End { get; set; }

        public int ChunkNumber { get; set; }

        public Block()
        {

        }

        public Block(string blockid, int chunkNumber, long start, long end)
        {
            BlockID = blockid;
            ChunkNumber = chunkNumber;
            Start = start;
            End = end;
        }
    }
}
