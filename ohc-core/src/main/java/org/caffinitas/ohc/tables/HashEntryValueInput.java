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
package org.caffinitas.ohc.tables;

import java.io.IOException;

/**
 * Instances of this class are passed to {@link org.caffinitas.ohc.CacheSerializer#deserialize(java.io.DataInput)}.
 */
final class HashEntryValueInput extends AbstractDataInput
{
    HashEntryValueInput(long hashEntryAdr)
    {
        super(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(HashEntries.getKeyLen(hashEntryAdr)), HashEntries.getValueLen(hashEntryAdr));
    }

    public boolean readBoolean() throws IOException
    {
        assertAvail(1);
        return Uns.getBoolean(blkAdr, blkOff++);
    }

    public int readUnsignedByte() throws IOException
    {
        assertAvail(1);
        return (int) Uns.getByte(blkAdr, blkOff++) & 0xff;
    }

    public short readShort() throws IOException
    {
        assertAvail(2);
        short r = Uns.getShort(blkAdr, blkOff);
        blkOff += 2;
        return r;
    }

    public int readUnsignedShort() throws IOException
    {
        assertAvail(2);
        int r = Uns.getShort(blkAdr, blkOff);
        blkOff += 2;
        return r & 0xffff;
    }

    public char readChar() throws IOException
    {
        assertAvail(2);
        char r = Uns.getChar(blkAdr, blkOff);
        blkOff += 2;
        return r;
    }

    public int readInt() throws IOException
    {
        assertAvail(4);
        int r = Uns.getInt(blkAdr, blkOff);
        blkOff += 4;
        return r;
    }

    public long readLong() throws IOException
    {
        assertAvail(8);
        long r = Uns.getLong(blkAdr, blkOff);
        blkOff += 8;
        return r;
    }

    public float readFloat() throws IOException
    {
        assertAvail(4);
        float r = Uns.getFloat(blkAdr, blkOff);
        blkOff += 4;
        return r;
    }

    public double readDouble() throws IOException
    {
        assertAvail(8);
        double r = Uns.getDouble(blkAdr, blkOff);
        blkOff += 8;
        return r;
    }
}
