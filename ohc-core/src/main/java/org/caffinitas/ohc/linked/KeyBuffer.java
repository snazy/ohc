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
import java.nio.ByteOrder;
import java.util.Arrays;

final class KeyBuffer
{
    final ByteBuffer buffer;
    private final long hash;

    KeyBuffer(ByteBuffer buffer, Hasher hasher)
    {
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        int position = buffer.position();
        hash = hasher.hash(buffer);
        buffer.position(position);
    }

    long hash()
    {
        return hash;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return buffer.equals(keyBuffer.buffer);
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
        StringBuilder sb = new StringBuilder((buffer.limit() - buffer.position()) * 3);
        for (int ii = buffer.position(); ii < buffer.limit(); ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(pad(buffer.get(ii)));
            sb.append(' ');
        }
        return sb.toString();
    }

    ByteBuffer byteBuffer()
    {
        return buffer.asReadOnlyBuffer();
    }

    boolean sameKey(long hashEntryAdr)
    {
        if (HashEntries.getHash(hashEntryAdr) != hash())
            return false;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen == (buffer.limit() - buffer.position()) && compareKey(hashEntryAdr);
    }

    private boolean compareKey(long hashEntryAdr)
    {
        int blkOff = (int) Util.ENTRY_OFF_DATA;
        int p = buffer.position();
        int endIdx = buffer.limit() - 1;
        for (; p <= endIdx - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != buffer.getLong(p))
                return false;
        for (; p <= endIdx - 4; p += 4, blkOff += 4)
            if (Uns.getInt(hashEntryAdr, blkOff) != buffer.getInt(p))
                return false;
        for (; p <= endIdx - 2; p += 2, blkOff += 2)
            if (Uns.getShort(hashEntryAdr, blkOff) != buffer.getShort(p))
                return false;
        for (; p < endIdx; p++, blkOff++)
            if (Uns.getByte(hashEntryAdr, blkOff) != buffer.get(p))
                return false;

        return true;
    }
}
