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

abstract class Constants
{

// Hash entries

    // offset of LRU replacement strategy next pointer
    static final long ENTRY_OFF_LRU_NEXT = 0;
    // offset of LRU replacement strategy previous pointer
    static final long ENTRY_OFF_LRU_PREV = 8;
    // offset of entry lock
    static final long ENTRY_OFF_REFCOUNT = 16;
    // offset of next hash entry in a hash bucket
    static final long ENTRY_OFF_NEXT = 24;
    // offset of previous hash entry in a hash bucket
    static final long ENTRY_OFF_PREVIOUS = 32;
    // offset of serialized hash value
    static final long ENTRY_OFF_HASH = 40;
    // offset of serialized hash key length
    static final long ENTRY_OFF_KEY_LENGTH = 48;
    // offset of serialized value length
    static final long ENTRY_OFF_VALUE_LENGTH = 56;
    // offset of data in first block
    static final long ENTRY_OFF_DATA = 72;

// Hash bucket-table

    // reference to the first entry of segment
    static final long BUCKET_FIRST = 0L;
    // total memory required for a hash-partition
    static final long BUCKET_ENTRY_LEN = 8;

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
}
