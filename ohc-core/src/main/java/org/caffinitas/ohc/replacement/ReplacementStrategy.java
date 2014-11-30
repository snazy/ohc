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
package org.caffinitas.ohc.replacement;

/**
 * Implementation of various kinds of replacement strategies.
 * <p/>
 * Note that all methods are called within a critical section - i.e. while holding an exclusive
 * lock on the {@link org.caffinitas.ohc.OffHeapMap}. That means that you must not do any expensive operation
 * in your implementation.
 * <p/>
 * Each instance of {@link org.caffinitas.ohc.OffHeapMap} has its dedicated instance of this class -
 * this means that you can hold a global state in your implementation. For example
 * {@link org.caffinitas.ohc.replacement.LRUReplacementStrategy} holds the LRU linked list {@code head} and
 * {@code tail} as instance fields.
 * <p/>
 * Per entry state can be maintained using {@link org.caffinitas.ohc.HashEntries#getReplacement0(long)}
 * and {@link org.caffinitas.ohc.HashEntries#getReplacement0(long)} (and its corresponding setters).
 */
public interface ReplacementStrategy
{
    /**
     * Called when the given hash entry has been accessed using a {@code get} operation.
     */
    void entryUsed(long hashEntryAdr);

    /**
     * Called when the given hash entry has been accessed using a {@code put} operation.
     * @param oldHashEntryAdr old entry that has been replaced or {@code 0L}
     */
    void entryReplaced(long oldHashEntryAdr, long hashEntryAdr);

    /**
     * Called when the given hash entry has been removed.
     */
    void entryRemoved(long hashEntryAdr);

    /**
     * Perform a cleanup/eviction/replacement (however you like to call it) on a shard.
     * @param recycleGoal number of bytes to free
     * @param cb callback to actually remove an entry from the map
     * @return number of removed entries
     */
    long cleanUp(long recycleGoal, ReplacementCallback cb);
}
