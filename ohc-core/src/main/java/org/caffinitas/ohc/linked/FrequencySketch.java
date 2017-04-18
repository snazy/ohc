/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified for OHC
 */
package org.caffinitas.ohc.linked;

import java.util.Random;

import com.google.common.primitives.Ints;

/**
 * A probabilistic multiset for estimating the popularity of an element within a time window. The
 * maximum frequency of an element is limited to 15 (4-bits) and an aging process periodically
 * halves the popularity of all elements.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @author snazy@snazy.de (Robert Stupp) - modified for OHC
 */
final class FrequencySketch
{

  /*
   * This class maintains a 4-bit CountMinSketch [1] with periodic aging to provide the popularity
   * history for the TinyLfu admission policy [2]. The time and space efficiency of the sketch
   * allows it to cheaply estimate the frequency of an entry in a stream of cache access events.
   *
   * The counter matrix is represented as a single dimensional array holding 16 counters per slot. A
   * fixed depth of four balances the accuracy and cost, resulting in a width of four times the
   * length of the array. To retain an accurate estimation the array's length equals the maximum
   * number of entries in the cache, increased to the closest power-of-two to exploit more efficient
   * bit masking. This configuration results in a confidence of 93.75% and error bound of e / width.
   *
   * The frequency of all entries is aged periodically using a sampling window based on the maximum
   * number of entries in the cache. This is referred to as the reset operation by TinyLfu and keeps
   * the sketch fresh by dividing all counters by two and subtracting based on the number of odd
   * counters found. The O(n) cost of aging is amortized, ideal for hardware prefetching, and uses
   * inexpensive bit manipulations per array location.
   *
   * A per instance smear is used to help protect against hash flooding [3], which would result
   * in the admission policy always rejecting new candidates. The use of a pseudo random hashing
   * function resolves the concern of a denial of service attack by exploiting the hash codes.
   *
   * [1] An Improved Data Stream Summary: The Count-Min Sketch and its Applications
   * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
   * [2] TinyLFU: A Highly Efficient Cache Admission Policy
   * http://arxiv.org/pdf/1512.00727.pdf
   * [3] Denial of Service via Algorithmic Complexity Attack
   * https://www.usenix.org/legacy/events/sec03/tech/full_papers/crosby/crosby.pdf
   */

    private static final long[] SEED = new long[]
                                       { // A mixture of seeds from FNV-1a, CityHash, and Murmur3
                                         0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L };
    private static final long RESET_MASK = 0x7777777777777777L;
    private static final long ONE_MASK = 0x1111111111111111L;

    final int sampleSize;
    final int tableMask;
    long tableOffset;
    final int tableLength;
    int size;

    /**
     * Creates an initialized frequency sketch.
     *
     * Initializes and increases the capacity of this <tt>FrequencySketch</tt> instance, if necessary,
     * to ensure that it can accurately estimate the popularity of elements given the maximum size of
     * the cache.
     *
     * @param maximumSize the maximum size of the cache
     */
    FrequencySketch(long maximumSize)
    {
        if (maximumSize <= 0)
            throw new IllegalArgumentException("maximumSize must be greater than 0");
        int maximum = (int) Math.min(maximumSize, Integer.MAX_VALUE >>> 1);

        tableLength = (maximum == 0) ? 1 : Ints.checkedCast(Util.roundUpToPowerOf2(maximum, 1 << 30));
        tableOffset = Uns.allocate(8 * tableLength, true);
        Uns.setMemory(tableOffset, 0, 8 * tableLength, (byte) 0);
        tableMask = Math.max(0, tableLength - 1);
        sampleSize = maximum <= 0 ? Integer.MAX_VALUE : 10 * maximum;
        size = 0;
    }

    void release()
    {
        if (tableOffset != 0L)
            Uns.free(tableOffset);
        tableOffset = 0L;
    }

    private long tableAt(int i)
    {
        return Uns.getLong(tableOffset, i * 8);
    }

    private void tableAt(int i, long val)
    {
        Uns.putLong(tableOffset, i * 8, val);
    }

    /**
     * Returns the estimated number of occurrences of an element, up to the maximum (15).
     *
     * @param hash the hash code of the element to count occurrences of
     * @return the estimated number of occurrences of the element; possibly zero but never negative
     */
    int frequency(long hash)
    {
        long start = (hash & 3) << 2;

        int frequency =                 countOf(start, 0, indexOf(hash, 0));
        frequency = Math.min(frequency, countOf(start, 1, indexOf(hash, 1)));
        frequency = Math.min(frequency, countOf(start, 2, indexOf(hash, 2)));
        return      Math.min(frequency, countOf(start, 3, indexOf(hash, 3)));
    }

    private int countOf(long start, int i, int index)
    {
        return (int) ((tableAt(index) >>> ((start + i) << 2)) & 0xfL);
    }

    /**
     * Returns the table index for the counter at the specified depth.
     *
     * @param item the element's hash
     * @param i    the counter depth
     * @return the table index
     */
    int indexOf(long item, int i)
    {
        return ((int) (SEED[i] * item)) & tableMask;
    }

    /**
     * Increments the popularity of the element if it does not exceed the maximum (15). The popularity
     * of all elements will be periodically down sampled when the observed events exceeds a threshold.
     * This process provides a frequency aging to allow expired long term entries to fade away.
     *
     * @param hash the hash code of the element to add
     */
    void increment(long hash)
    {
        // Loop unrolling improves throughput by 5m ops/s

        int start = (int) ((hash & 3) << 2);

        boolean added = incrementAt(indexOf(hash, 0), start)
                      | incrementAt(indexOf(hash, 1), start + 1)
                      | incrementAt(indexOf(hash, 2), start + 2)
                      | incrementAt(indexOf(hash, 3), start + 3);

        if (added && (++size == sampleSize))
        {
            reset();
        }
    }

    /**
     * Increments the specified counter by 1 if it is not already at the maximum value (15).
     *
     * @param i the table index (16 counters)
     * @param j the counter to increment
     * @return if incremented
     */
    private boolean incrementAt(int i, int j)
    {
        int offset = j << 2;
        long mask = (0xfL << offset);
        long t = tableAt(i);
        if ((t & mask) != mask)
        {
            tableAt(i, t + (1L << offset));
            return true;
        }
        return false;
    }

    /**
     * Reduces every counter by half of its original value.
     */
    private void reset()
    {
        int count = 0;
        for (int i = 0; i < tableLength; i++)
        {
            long t = tableAt(i);
            count += Long.bitCount(t & ONE_MASK);
            tableAt(i, (t >>> 1) & RESET_MASK);
        }
        size = (size >>> 1) - (count >>> 2);
    }



    // Following is a faster than using j.u.Random all the time.

    private Random random = new Random();
    private long seed = random.nextLong();
    private int reseed;

    boolean tieAdmit()
    {
        // for RNG see org.caffinitas.ohc.benchmark.distribution.FasterRandom (in ohc-benchmark)

        if (++this.reseed == 32)
            rollover();

        long seed = this.seed;
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        this.seed = seed;
        long random = seed * 2685821657736338717L;

        return (random & 127) == 0;
    }

    private void rollover()
    {
        this.reseed = 0;
        random.setSeed(seed);
        seed = random.nextLong();
    }
}
