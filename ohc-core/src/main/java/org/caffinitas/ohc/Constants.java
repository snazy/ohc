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

public interface Constants
{

// Hash entries

    // offset of next block address in a "next block"
    static final int OFF_NEXT_BLOCK = 0;
    // offset of total length
    static final int OFF_DATA_LENGTH = 8;
    // offset of serialized hash value
    static final int OFF_HASH = 16;
    // offset of previous hash entry in LRU list for this hash partition
    static final int OFF_LRU_PREVIOUS = 24;
    // offset of next hash entry in LRU list for this hash partition
    static final int OFF_LRU_NEXT = 32;
    // offset of serialized hash key length
    static final int OFF_HASH_KEY_LENGTH = 40;
    // offset of serialized value length
    static final int OFF_VALUE_LENGTH = 48;
    // offset of entry lock
    static final int OFF_ENTRY_LOCK = 56;
    // offset of data in first block
    static final int OFF_DATA_IN_FIRST = 64;

    // offset of data in "next block"
    static final int OFF_DATA_IN_NEXT = 8;

// Hash partitions (segments)

    // reference to the last-referenced hash entry (for LRU)
    static final long PART_OFF_LRU_HEAD = 0L;
    // offset of CAS style lock field
    static final long PART_OFF_LOCK = 8L;
    // total memory required for a hash-partition
    static final int PARTITION_ENTRY_LEN = 16;

}
