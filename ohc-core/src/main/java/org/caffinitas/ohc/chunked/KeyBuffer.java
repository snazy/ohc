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
package org.caffinitas.ohc.chunked;

import java.nio.ByteBuffer;

final class KeyBuffer
{
    private final ByteBuffer bytes;
    private long hash;

    KeyBuffer(ByteBuffer bytes)
    {
        this.bytes = bytes;
    }

    ByteBuffer buffer()
    {
        return bytes;
    }

    int size()
    {
        return bytes.limit() - bytes.position();
    }

    long hash()
    {
        return hash;
    }

    KeyBuffer finish(Hasher hasher)
    {
        // duplicate the buffer as the hasher implementation may change position
        hash = hasher.hash(bytes.duplicate());

        return this;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return bytes.equals(keyBuffer.bytes);
    }

    public int hashCode()
    {
        return (int) hash;
    }

    private static String padToEight(int val)
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
        for (int ii = 0; ii < bytes.limit(); ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(padToEight(bytes.get(ii)));
            sb.append(' ');
        }
        return sb.toString();
    }
}
