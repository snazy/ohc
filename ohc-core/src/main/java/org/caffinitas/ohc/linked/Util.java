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
package org.caffinitas.ohc.linked;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

final class Util
{

// Hash entries

    // offset of LRU replacement strategy next pointer (8 bytes, long)
    static final long ENTRY_OFF_LRU_NEXT = 0;
    // offset of LRU replacement strategy previous pointer (8 bytes, long)
    static final long ENTRY_OFF_LRU_PREV = 8;
    // offset of next hash entry in a hash bucket (8 bytes, long)
    static final long ENTRY_OFF_NEXT = 16;
    // offset of entry reference counter (4 bytes, int)
    static final long ENTRY_OFF_REFCOUNT = 24;
    // offset of entry sentinel (4 bytes, int)
    static final long ENTRY_OFF_SENTINEL = 28;
    // slot in which the entry resides (8 bytes, long)
    static final long ENTRY_OFF_EXPIRE_AT = 32;
    // LRU generation (4 bytes, int, only 2 distinct values)
    static final long ENTRY_OFF_GENERATION = 40;
    // bytes 44..47 unused
    // offset of serialized hash value (8 bytes, long)
    static final long ENTRY_OFF_HASH = 48;
    // offset of serialized value length (4 bytes, int)
    static final long ENTRY_OFF_VALUE_LENGTH = 56;
    // offset of serialized hash key length (4 bytes, int)
    static final long ENTRY_OFF_KEY_LENGTH = 60;
    // offset of data in first block
    static final long ENTRY_OFF_DATA = 64;

    static final int SERIALIZED_ENTRY_SIZE = (int) (ENTRY_OFF_DATA - ENTRY_OFF_HASH);
    static final int SERIALIZED_KEY_LEN_SIZE = (int) (ENTRY_OFF_DATA - ENTRY_OFF_KEY_LENGTH);

    // Note: keep ENTRY_OFF_HASH, ENTRY_OFF_VALUE_LENGTH, ENTRY_OFF_KEY_LENGTH in exact that order
    // and together and at the end of the header because
    // org.caffinitas.ohc.linked.OHCacheImpl.(de)serializeEntry relies on it!

// Window Tiny-LFU generations

    static final int GEN_EDEN = 0;
    static final int GEN_MAIN = 1;

// Hash bucket-table

    // total memory required for a hash-partition
    static final long BUCKET_ENTRY_LEN = 8;

// Compressed entries header

    // 'OHCC'
    static final int HEADER_COMPRESSED = 0x4f484343;
    // 'OHCC' reversed
    static final int HEADER_COMPRESSED_WRONG = 0x4343484f;
    // 'OHCE'
    static final int HEADER_ENTRIES = 0x4f484345;
    // 'OHCE' reversed
    static final int HEADER_ENTRIES_WRONG = 0x4543484f;
    // 'OHCK'
    static final int HEADER_KEYS = 0x4f48434b;
    // 'OHCK' reversed
    static final int HEADER_KEYS_WRONG = 0x4b43484f;

// sentinel values

    static final int SENTINEL_NOT_PRESENT = 0;
    static final int SENTINEL_LOADING = 1;
    static final int SENTINEL_SUCCESS = 2;
    static final int SENTINEL_TEMPORARY_FAILURE = 3;
    static final int SENTINEL_PERMANENT_FAILURE = 4;

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
        while (buffer.remaining() > 0)
        {
            int rd = channel.read(buffer);
            if (rd == -1)
                return false;
        }
        return true;
    }

    static int bitNum(long val)
    {
        int bit = 0;
        for (; val != 0L; bit++)
            val >>>= 1;
        return bit;
    }

    static long roundUpToPowerOf2(long number, long max)
    {
        return number >= max
               ? max
               : (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }
}
