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
 * Linked memory entry off-heap implementation.
 * <p>
 * The number of segments is configured via {@link org.caffinitas.ohc.OHCacheBuilder},
 * defaults to {@code # of cpus * 2} and must be a power of 2.
 * Entries are distribtued over the segments using the most significant bits of the 64 bit hash code.
 * Accesses on each segment are synchronized.
 * </p>
 * <p>
 * Each hash-map entry is allocated individually. Entries are free'd (deallocated), when they are
 * no longer referenced by the off-heap map itself or any external reference like
 * {@link org.caffinitas.ohc.DirectValueAccess} or a {@link org.caffinitas.ohc.CacheSerializer}.
 * </p>
 * <p>
 * The design of this implementation reduces the locked time of a segment to a very short time.
 * Put/replace operations allocate memory first, call the {@link org.caffinitas.ohc.CacheSerializer}s
 * to serialize the key and value and then put the fully prepared entry into the segment.
 * </p>
 * <p>
 * Eviction is performed using an LRU algorithm. A linked list through all cached elements per segment
 * is used to keep track of the eldest entries.
 * </p>
 * <p>
 * Since this implementation performs alloc/free operations for each individual entry, take care
 * of memory fragmentation. We recommend using jemalloc to keep fragmentation low. On Unix operating
 * systems, preload jemalloc.
 * </p>
 */
package org.caffinitas.ohc.linked;
