/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

abstract class Constants
{

// Hash entries

    // offset of LRU replacement strategy next pointer
    static final long ENTRY_OFF_LRU_NEXT = 0;
    // offset of LRU replacement strategy previous pointer
    static final long ENTRY_OFF_LRU_PREV = 8;
    // offset of next hash entry in a hash bucket
    static final long ENTRY_OFF_NEXT = 16;
    // offset of entry reference counter
    static final long ENTRY_OFF_REFCOUNT = 24;
    // offset of serialized hash value
    static final long ENTRY_OFF_HASH = 32;
    // offset of serialized hash key length
    static final long ENTRY_OFF_KEY_LENGTH = 40;
    // offset of serialized value length
    static final long ENTRY_OFF_VALUE_LENGTH = 48;
    // offset of data in first block
    static final long ENTRY_OFF_DATA = 56;

    // Note: keep ENTRY_OFF_HASH, ENTRY_OFF_KEY_LENGTH, ENTRY_OFF_VALUE_LENGTH in exact that order
    // and together and at the end of the header because
    // org.caffinitas.ohc.SegmentedCacheImpl.(de)serializeEntry relies on it!

// Hash bucket-table

    // total memory required for a hash-partition
    static final long BUCKET_ENTRY_LEN = 8;

// Compressed entries header

    // 'OHRC'
    static final int HEADER_COMPRESSED = 0x4f485243;
    // 'OHRC' reversed
    static final int HEADER_COMPRESSED_WRONG = 0x4352484f;
    // 'OHRU'
    static final int HEADER_UNCOMPRESSED = 0x4f485255;
    // 'OHRU' reversed
    static final int HEADER_UNCOMPRESSED_WRONG = 0x5552484f;

    static long roundUpTo8(long val)
    {
        long rem = val & 7;
        if (rem != 0)
            val += 8L - rem;
        return val;
    }

    static long allocLen(long keyLen, long valueLen)
    {
        return ENTRY_OFF_DATA + roundUpTo8(keyLen) + valueLen;
    }

    static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException
    {
        while (buffer.remaining() > 0)
            channel.write(buffer);
    }

    static boolean readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException
    {
        boolean any = false;
        while (buffer.remaining() > 0)
        {
            int rd = channel.read(buffer);
            if (rd == -1)
                return any;
            if (rd > 0)
                any = true;
        }
        return true;
    }
}
