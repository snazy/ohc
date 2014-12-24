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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

abstract class AbstractDataInput implements DataInput
{
    long blkAdr;
    long blkOff;
    private final long blkEnd;

    AbstractDataInput(long hashEntryAdr, long offset, long len)
    {
        this.blkAdr = hashEntryAdr;
        this.blkOff = offset;
        this.blkEnd = offset + len;
    }

    long avail()
    {
        return blkEnd - blkOff;
    }

    void assertAvail(int req) throws IOException
    {
        if (available() < req || req < 0)
            throw new EOFException();
    }

    public int available()
    {
        long av = avail();
        return av > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) av;
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        if (b == null || off < 0 || off + len > b.length || len < 0)
            throw new IllegalArgumentException();

        assertAvail(len);

        Uns.copyMemory(blkAdr, blkOff, b, off, len);
        blkOff += len;
    }

    public byte readByte() throws IOException
    {
        assertAvail(1);

        return Uns.getByte(blkAdr, blkOff++);
    }

    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }

    public int skipBytes(int n) throws IOException
    {
        assertAvail(n);
        blkOff += n;
        return n;
    }

    public String readLine() throws IOException
    {
        // method intentionally not supported (no length prefix)
        throw new UnsupportedOperationException();
    }

    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }
}
