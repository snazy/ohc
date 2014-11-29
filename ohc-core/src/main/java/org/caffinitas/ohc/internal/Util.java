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
package org.caffinitas.ohc.internal;

public final class Util
{
    Util()
    {
    }

    public static int maxOf(int[] arr)
    {
        int r = 0;
        for (int l : arr)
            if (l > r)
                r = l;
        return r;
    }

    public static int minOf(int[] arr)
    {
        int r = Integer.MAX_VALUE;
        for (int l : arr)
            if (l < r)
                r = l;
        return r;
    }

    public static long minOf(long[] arr)
    {
        long r = Long.MAX_VALUE;
        for (long l : arr)
            if (l < r)
                r = l;
        return r;
    }

    public static double avgOf(int[] arr)
    {
        double r = 0d;
        for (int l : arr)
            r += l;
        return r / arr.length;
    }

    // maximum hash table size
    static final int MAX_TABLE_SIZE = 1 << 27;

    public static int roundUpToPowerOf2(int number)
    {
        return number >= MAX_TABLE_SIZE
               ? MAX_TABLE_SIZE
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    public static long roundUpTo8(long val)
    {
        long rem = val & 7;
        if (rem != 0)
            val += 8L - rem;
        return val;
    }

    public static int bitNum(long val)
    {
        int bit = 0;
        for (; val != 0L; bit++)
            val >>>= 1;
        return bit;
    }
}
