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

import java.io.IOException;

/**
 * Instances of this class are passed to {@link org.caffinitas.ohc.CacheSerializer#deserialize(java.io.DataInput)}.
 */
final class HashEntryKeyInput extends AbstractDataInput
{

    HashEntryKeyInput(long hashEntryAdr)
    {
        super(hashEntryAdr, Util.ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr));
    }

    //
    // Note: it is a very bad idea to use Unsafe methods for these writeInt/Short/Long etc because
    // the corresponding sun.misc.Unsafe methods use CPU endian which usually differs from endian used by Java.

    public boolean readBoolean() throws IOException
    {
        return readByte()!=0;
    }

    public int readUnsignedByte() throws IOException
    {
        return readByte()&0xff;
    }

    public short readShort() throws IOException
    {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (short)((ch1 << 8) + ch2);
    }

    public int readUnsignedShort() throws IOException
    {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (ch1 << 8) + ch2;
    }

    public char readChar() throws IOException
    {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (char)((ch1 << 8) + ch2);
    }

    public int readInt() throws IOException
    {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        int ch3 = readUnsignedByte();
        int ch4 = readUnsignedByte();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    public long readLong() throws IOException
    {
        return (((long) readUnsignedByte() << 56) +
                ((long)(readUnsignedByte() & 255) << 48) +
                ((long)(readUnsignedByte() & 255) << 40) +
                ((long)(readUnsignedByte() & 255) << 32) +
                ((long)(readUnsignedByte() & 255) << 24) +
                ((readUnsignedByte() & 255) << 16) +
                ((readUnsignedByte() & 255) <<  8) +
                ((readUnsignedByte() & 255)));
    }

    public float readFloat() throws IOException
    {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException
    {
        return Double.longBitsToDouble(readLong());
    }
}
