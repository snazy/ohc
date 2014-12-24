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
 * Instances of this class are passed to {@link org.caffinitas.ohc.CacheSerializer#serialize(Object, java.io.DataOutput)}.
 */
final class HashEntryValueOutput extends AbstractOffHeapDataOutput
{
    HashEntryValueOutput(long hashEntryAdr, long keyLen, long valueLen)
    {
        super(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(keyLen), valueLen);
    }

    HashEntryValueOutput(long hashEntryAdr, long valueLen)
    {
        super(hashEntryAdr, 0, valueLen);
    }

    // Note: it is a very bad idea to override writeInt/Short/Long etc because the corresponding
    // sun.misc.Unsafe methods use CPU endian which usually differs from endian used by Java


    public void writeBoolean(boolean v) throws IOException
    {
        assertAvail(1);
        Uns.putBoolean(blkAdr, blkOff++, v);
    }

    public void writeByte(int v) throws IOException
    {
        assertAvail(1);
        Uns.putByte(blkAdr, blkOff++, (byte) v);
    }

    public void writeShort(int v) throws IOException
    {
        assertAvail(2);
        Uns.putShort(blkAdr, blkOff, (short) v);
        blkOff += 2;
    }

    public void writeChar(int v) throws IOException
    {
        assertAvail(2);
        Uns.putChar(blkAdr, blkOff, (char) v);
        blkOff += 2;
    }

    public void writeInt(int v) throws IOException
    {
        assertAvail(4);
        Uns.putInt(blkAdr, blkOff, v);
        blkOff += 4;
    }

    public void writeLong(long v) throws IOException
    {
        assertAvail(8);
        Uns.putLong(blkAdr, blkOff, v);
        blkOff += 8;
    }

    public void writeFloat(float v) throws IOException
    {
        assertAvail(4);
        Uns.putFloat(blkAdr, blkOff, v);
        blkOff += 4;
    }

    public void writeDouble(double v) throws IOException
    {
        assertAvail(8);
        Uns.putDouble(blkAdr, blkOff, v);
        blkOff += 8;
    }
}
