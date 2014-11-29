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
package org.caffinitas.ohc.segment;

public interface Constants
{

// Hash entries

    // offset of value for replacement strategy
    static final long ENTRY_OFF_REPLACEMENT0 = 0;
    // offset of previous hash entry in LRU list for this hash partition
    static final long ENTRY_OFF_REPLACEMENT1 = 8;
    // offset of total length
    static final long ENTRY_OFF_DATA_LENGTH = 16;
    // offset of serialized hash value
    static final long ENTRY_OFF_HASH = 24;
    // offset of next hash entry in LRU list for this hash partition
    static final long ENTRY_OFF_NEXT = 32;
    // offset of serialized hash key length
    static final long ENTRY_OFF_KEY_LENGTH = 40;
    // offset of serialized value length
    static final long ENTRY_OFF_VALUE_LENGTH = 48;
    // offset of entry lock
    static final long ENTRY_OFF_REFCOUNT = 56;
    // offset of data in first block
    static final long ENTRY_OFF_DATA = 64;

// Hash partitions

    // reference to the LRU-head
    static final long PART_OFF_PARTITION_FIRST = 0L;
    // total memory required for a hash-partition
    static final long PARTITION_ENTRY_LEN = 8;
}
