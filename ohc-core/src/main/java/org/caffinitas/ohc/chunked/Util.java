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
package org.caffinitas.ohc.chunked;

final class Util
{

// Hash entries

    // timestamp when chunk has been last accessed for read
    static final int CHUNK_OFF_TIMESTAMP = 0;
    // number of entries in the chunk
    static final int CHUNK_OFF_ENTRIES = 8;
    // number of bytes used in the chunk
    static final int CHUNK_OFF_BYTES = 12;
    // offset of first entry
    static final int CHUNK_OFF_DATA = 16;

    // offset of serialized hash value
    static final int ENTRY_OFF_HASH = 0;
    // offset of next hash entry in a hash bucket
    static final int ENTRY_OFF_NEXT = 8;

    // fixed-size: flag whether entry is removed
    static final int ENTRY_OFF_REMOVED = 12;
    // fixed-size: offset of data in first block
    static final int ENTRY_OFF_DATA_FIXED = 16;

    // variable-size: offset of serialized value length (-1 for removed entries)
    static final int ENTRY_OFF_VALUE_LENGTH = 12;
    // variable-size: offset of serialized hash key length
    static final int ENTRY_OFF_KEY_LENGTH = 16;
    // variable-size: offset of allocated value length
    static final int ENTRY_OFF_RESERVED_LENGTH = 20;
    // variable-size: offset of data in first block
    static final int ENTRY_OFF_DATA_VARIABLE = 24;

    static int allocLen(int keyLen, int valueLen, boolean fixedEntrySize)
    {
        return entryOffData(fixedEntrySize) + keyLen + valueLen;
    }

    static int entryOffData(boolean fixedEntrySize)
    {
        return fixedEntrySize ? ENTRY_OFF_DATA_FIXED : ENTRY_OFF_DATA_VARIABLE;
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
