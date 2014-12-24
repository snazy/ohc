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
package org.caffinitas.ohc.impl;

import java.util.Arrays;

final class KeyBuffer extends AbstractDataOutput
{
    private final byte[] array;
    private int p;
    private long hash;

    // TODO maybe move 'array' to off-heap - depends on actual use.
    // pro: reduces heap pressure
    // pro: harmonize code for key + value (de)serialization in DataIn/Output implementations
    // con: puts pressure on jemalloc

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
        int o = 0;
        int r = size();

        long h1 = 0L;
        long h2 = 0L;
        long k1, k2;

        for (; r >= 16; r -= 16)
        {
            k1 = getLong(o);
            o += 8;
            k2 = getLong(o);
            o += 8;

            // bmix64()

            h1 ^= Murmur3.mixK1(k1);

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= Murmur3.mixK2(k2);

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        k1 = 0;
        k2 = 0;
        switch (r)
        {
            case 15:
                k2 ^= Murmur3.toLong(array[o + 14]) << 48; // fall through
            case 14:
                k2 ^= Murmur3.toLong(array[o + 13]) << 40; // fall through
            case 13:
                k2 ^= Murmur3.toLong(array[o + 12]) << 32; // fall through
            case 12:
                k2 ^= Murmur3.toLong(array[o + 11]) << 24; // fall through
            case 11:
                k2 ^= Murmur3.toLong(array[o + 10]) << 16; // fall through
            case 10:
                k2 ^= Murmur3.toLong(array[o + 9]) << 8; // fall through
            case 9:
                k2 ^= Murmur3.toLong(array[o + 8]); // fall through
            case 8:
                k1 ^= getLong(o);
                break;
            case 7:
                k1 ^= Murmur3.toLong(array[o + 6]) << 48; // fall through
            case 6:
                k1 ^= Murmur3.toLong(array[o + 5]) << 40; // fall through
            case 5:
                k1 ^= Murmur3.toLong(array[o + 4]) << 32; // fall through
            case 4:
                k1 ^= Murmur3.toLong(array[o + 3]) << 24; // fall through
            case 3:
                k1 ^= Murmur3.toLong(array[o + 2]) << 16; // fall through
            case 2:
                k1 ^= Murmur3.toLong(array[o + 1]) << 8; // fall through
            case 1:
                k1 ^= Murmur3.toLong(array[o]);
                break;
            default:
                throw new AssertionError("Should never get here.");
        }

        h1 ^= Murmur3.mixK1(k1);
        h2 ^= Murmur3.mixK2(k2);

        // makeHash()

        h1 ^= size();
        h2 ^= size();

        h1 += h2;
        h2 += h1;

        h1 = Murmur3.fmix64(h1);
        h2 = Murmur3.fmix64(h2);

        h1 += h2;
        //h2 += h1;

        // padToLong()

        hash = h1;

        return this;
    }

    private long getLong(int o)
    {
        long l = Murmur3.toLong(array[o + 7]) << 56;
        l |= Murmur3.toLong(array[o + 6]) << 48;
        l |= Murmur3.toLong(array[o + 5]) << 40;
        l |= Murmur3.toLong(array[o + 4]) << 32;
        l |= Murmur3.toLong(array[o + 3]) << 24;
        l |= Murmur3.toLong(array[o + 2]) << 16;
        l |= Murmur3.toLong(array[o + 1]) << 8;
        l |= Murmur3.toLong(array[o]);
        return l;
    }

    public void write(int b)
    {
        array[p++] = (byte) b;
    }

    public void write(byte[] b, int off, int len)
    {
        System.arraycopy(b, off, array, p, len);
        p += len;
    }

    public void writeShort(int v)
    {
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeChar(int v)
    {
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeInt(int v)
    {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeLong(long v)
    {
        write((int) ((v >>> 56) & 0xFF));
        write((int) ((v >>> 48) & 0xFF));
        write((int) ((v >>> 40) & 0xFF));
        write((int) ((v >>> 32) & 0xFF));
        write((int) ((v >>> 24) & 0xFF));
        write((int) ((v >>> 16) & 0xFF));
        write((int) ((v >>> 8) & 0xFF));
        write((int) (v & 0xFF));
    }

    public void writeFloat(float v)
    {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v)
    {
        writeLong(Double.doubleToLongBits(v));
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return Arrays.equals(array, keyBuffer.array);
    }

    public int hashCode()
    {
        return (int) hash;
    }
}
