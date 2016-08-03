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

/**
 * Chunked memory allocation off-heap implementation.
 * <p>
 * Purpose of this implementation is to reduce the overhead for relatively small cache entries
 * compared to the linked implementation since the memory for the whole segment is pre-allocated.
 * This implementation is suitable for small entries with fast (de)serialization.
 * </p>
 * <p>
 * Segmentation is the same as in the <i>linked</i> implementation.
 * The number of segments is configured via {@link org.caffinitas.ohc.OHCacheBuilder},
 * defaults to {@code # of cpus * 2} and must be a power of 2.
 * Entries are distribtued over the segments using the most significant bits of the 64 bit hash code.
 * Accesses on each segment are synchronized.
 * </p>
 * <p>
 * Each segment is divided into multiple chunks. Each segment is responsible for a portion of the total
 * capacity ({@code capacity / segmentCount}). This amount of memory is allocated once up-front during
 * initialization and logically divided into a configurable number of chunks. The size of each chunk
 * is configured using the {@code chunkSize}
 * option in {@link org.caffinitas.ohc.OHCacheBuilder},
 * </p>
 * <p>
 * Like the <i>linked</i> implementation, hash entries are serialized into a temporary, thread-local
 * buffer first, before the actual put into a segment occurs (segement operations are synchronized).
 * </p>
 * <p>
 * New entries are placed into the current <i>write</i> chunk. When that chunk is full, the next empty
 * chunk will become the new <i>write</i> chunk. When all chunks are full, the least recently used
 * chunk, including all the entries it contains, is evicted.
 * </p>
 * <p>
 * Specifying the {@code fixedKeyLength} and {@code fixedValueLength} builder properties
 * reduces the memory footprint by 8 bytes per entry.
 * </p>
 * <p>
 * Serialization, direct access and get-with-loader functions are not supported in this implementation.
 * </p>
 * <p>
 * <em>NOTE</em> The CRC hash algorithm requires JRE 8 or newer.
 * </p>
 */
package org.caffinitas.ohc.chunked;
