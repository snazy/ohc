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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

final class KeyBuffer extends AbstractDataOutput
{
    private final Hasher hasher = Hashing.murmur3_128().newHasher();
    private final byte[] array;
    private int p;
    private long hash;

    KeyBuffer(int size)
    {
        array = new byte[size];
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

    KeyBuffer finish()
    {
        hash = hasher.hash().asLong();
        return this;
    }

    public void write(int b)
    {
        hasher.putByte((byte) b);
        array[p++] = (byte) b;
    }

    public void write(byte[] b, int off, int len)
    {
        hasher.putBytes(b, off, len);
        System.arraycopy(b, off, array, p, len);
        p += len;
    }
}
