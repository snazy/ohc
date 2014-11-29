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
package org.caffinitas.ohc.mono;

import java.io.EOFException;
import java.io.IOException;

import org.caffinitas.ohc.api.AbstractDataInput;
import org.caffinitas.ohc.internal.Util;

public final class HashEntryInput extends AbstractDataInput
{
    private long blkAdr;
    private long blkOff;
    private final long blkEnd;

    public HashEntryInput(long hashEntryAdr, boolean value, long serKeyLen, long valueLen)
    {
        if (hashEntryAdr == 0L)
            throw new NullPointerException();
        if (serKeyLen < 0L)
            throw new InternalError();
        if (valueLen < 0L)
            throw new InternalError();
        long blkOff = Constants.ENTRY_OFF_DATA;

        if (value)
            blkOff += Util.roundUpTo8(serKeyLen);

        if (valueLen > Integer.MAX_VALUE)
            throw new IllegalStateException("integer overflow");

        this.blkAdr = hashEntryAdr;
        this.blkOff = blkOff;
        this.blkEnd = blkOff + (value ? valueLen : serKeyLen);
    }

    private long avail()
    {
        return blkEnd - blkOff;
    }

    private void assertAvail(int req) throws IOException
    {
        if (avail() < req || req < 0)
            throw new EOFException();
    }

    public int available()
    {
        long av = avail();
        return av > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) av;
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        if (b == null)
            throw new NullPointerException();
        if (off < 0 || off + len > b.length || len < 0)
            throw new ArrayIndexOutOfBoundsException();

        assertAvail(len);

        Uns.copyMemory(blkAdr + blkOff, b, off, len);
    }

    public byte readByte() throws IOException
    {
        assertAvail(1);

        return Uns.getByte(blkAdr + blkOff++);
    }
}
