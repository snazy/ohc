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

final class LongArrayList
{
    private long[] array;
    private int size;

    public LongArrayList()
    {
        array = new long[10];
    }

    public long getLong(int i)
    {
        if (i < 0 || i >= size)
            throw new ArrayIndexOutOfBoundsException();
        return array[i];
    }

    public void clear()
    {
        size = 0;
    }

    public int size()
    {
        return size;
    }

    public void add(long value)
    {
        if (size == array.length)
            array = Arrays.copyOf(array, array.length + 10);
        array[size++] = value;
    }
}
