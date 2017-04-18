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
package org.caffinitas.ohc.linked;

import java.util.Arrays;

final class HeapKeyBuffer
{
    private final byte[] array;
    private long hash;

    HeapKeyBuffer(byte[] bytes)
    {
        array = bytes;
    }

    byte[] array()
    {
        return array;
    }

    int size()
    {
        return array.length;
    }

    long hash()
    {
        return hash;
    }

    HeapKeyBuffer finish(Hasher hasher)
    {
        hash = hasher.hash(array);

        return this;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HeapKeyBuffer keyBuffer = (HeapKeyBuffer) o;

        return Arrays.equals(array, keyBuffer.array);
    }

    public int hashCode()
    {
        return (int) hash;
    }

    static String padToEight(int val)
    {
        String str = Integer.toBinaryString(val & 0xff);
        while (str.length() < 8)
            str = '0' + str;
        return str;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < array.length; ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(padToEight(array[ii]));
            sb.append(' ');
        }
        return sb.toString();
    }
}
