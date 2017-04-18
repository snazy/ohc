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

import java.nio.ByteBuffer;
import java.util.Arrays;

final class KeyBuffer
{
    final byte[] buffer;
    private long hash;

    KeyBuffer(int size)
    {
        buffer = new byte[size];
    }

    long hash()
    {
        return hash;
    }

    KeyBuffer finish(Hasher hasher)
    {
        hash = hasher.hash(buffer);

        return this;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return buffer.length == keyBuffer.buffer.length && Arrays.equals(keyBuffer.buffer, buffer);
    }

    public int hashCode()
    {
        return (int) hash;
    }

    private static String pad(int val)
    {
        String str = Integer.toHexString(val & 0xff);
        while (str.length() == 1)
            str = '0' + str;
        return str;
    }

    @Override
    public String toString()
    {
        byte[] b = buffer;
        StringBuilder sb = new StringBuilder(b.length * 3);
        for (int ii = 0; ii < b.length; ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(pad(b[ii]));
            sb.append(' ');
        }
        return sb.toString();
    }

    ByteBuffer byteBuffer()
    {
        return ByteBuffer.wrap(buffer);
    }

    boolean sameKey(long hashEntryAdr)
    {
        if (HashEntries.getHash(hashEntryAdr) != hash())
            return false;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen == buffer.length && compareKey(hashEntryAdr);
    }

    private boolean compareKey(long hashEntryAdr)
    {
        int blkOff = (int) Util.ENTRY_OFF_DATA;
        int p = 0;
        int endIdx = buffer.length - 1;
        for (; p <= endIdx - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLongFromByteArray(buffer, p))
                return false;
        for (; p <= endIdx - 4; p += 4, blkOff += 4)
            if (Uns.getInt(hashEntryAdr, blkOff) != Uns.getIntFromByteArray(buffer, p))
                return false;
        for (; p <= endIdx - 2; p += 2, blkOff += 2)
            if (Uns.getShort(hashEntryAdr, blkOff) != Uns.getShortFromByteArray(buffer, p))
                return false;
        for (; p < endIdx; p++, blkOff++)
            if (Uns.getByte(hashEntryAdr, blkOff) != buffer[p])
                return false;

        return true;
    }
}
