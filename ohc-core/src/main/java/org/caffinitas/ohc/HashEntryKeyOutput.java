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

import java.io.IOException;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Used during put operations to serialize the key directly to off-heap bypassing {@link org.caffinitas.ohc.KeyBuffer}.
 */
final class HashEntryKeyOutput extends AbstractOffHeapDataOutput
{
    private final Hasher hasher = Hashing.murmur3_128().newHasher();
    private long hash;

    HashEntryKeyOutput(long hashEntryAdr, long keyLen)
    {
        super(hashEntryAdr, Util.ENTRY_OFF_DATA, keyLen);
    }

    long hash()
    {
        return hash;
    }

    void finish()
    {
        hash = hasher.hash().asLong();
    }

    public void write(int b) throws IOException
    {
        super.write(b);
        hasher.putByte((byte) b);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        super.write(b, off, len);
        hasher.putBytes(b, off, len);
    }

    public void writeShort(int v) throws IOException
    {
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeChar(int v) throws IOException
    {
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeInt(int v) throws IOException
    {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write(v & 0xFF);
    }

    public void writeLong(long v) throws IOException
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

    public void writeFloat(float v) throws IOException
    {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException
    {
        writeLong(Double.doubleToLongBits(v));
    }
}
