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

import java.io.EOFException;
import java.io.IOException;

abstract class AbstractOffHeapDataOutput extends AbstractDataOutput
{
    long blkAdr;
    protected long blkOff;
    protected final long blkEnd;

    AbstractOffHeapDataOutput(long hashEntryAdr, long offset, long len)
    {
        if (hashEntryAdr == 0L || offset < 0L || len < 0L)
            throw new IllegalArgumentException();

        this.blkAdr = hashEntryAdr;
        this.blkOff = offset;
        this.blkEnd = this.blkOff + len;
    }

    protected void assertAvail(int req) throws IOException
    {
        if (avail() < req || req < 0)
            throw new EOFException();
    }

    long avail()
    {
        return blkEnd - blkOff;
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        if (b == null || off < 0 || off + len > b.length || len < 0)
            throw new IllegalArgumentException();

        assertAvail(len);

        Uns.copyMemory(b, off, blkAdr, blkOff, len);
        blkOff += len;
    }

    public void write(int b) throws IOException
    {
        assertAvail(1);

        Uns.putByte(blkAdr, blkOff++, (byte) b);
    }

}
