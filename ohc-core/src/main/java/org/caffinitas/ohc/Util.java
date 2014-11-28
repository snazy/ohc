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

final class Util
{
    static int maxOf(int[] arr)
    {
        int r = 0;
        for (int l : arr)
            if (l > r)
                r = l;
        return r;
    }

    static int minOf(int[] arr)
    {
        int r = Integer.MAX_VALUE;
        for (int l : arr)
            if (l < r)
                r = l;
        return r;
    }

    static long minOf(long[] arr)
    {
        long r = Long.MAX_VALUE;
        for (long l : arr)
            if (l < r)
                r = l;
        return r;
    }

    static double avgOf(int[] arr)
    {
        double r = 0d;
        for (int l : arr)
            r += l;
        return r / arr.length;
    }
}
