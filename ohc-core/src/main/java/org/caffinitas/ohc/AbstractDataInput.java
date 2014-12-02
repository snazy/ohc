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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

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
        if (b == null || off < 0 || off + len > b.length || len < 0)
            throw new IllegalArgumentException();

        assertAvail(len);

        Uns.copyMemory(blkAdr, blkOff, b, off, len);
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
        throw new UnsupportedOperationException();
    }

    public String readUTF() throws IOException
    {
        int utflen = readUnsignedShort();
        char[] chararr = new char[utflen];

        int c = 0, char2, char3;
        int count = 0;
        int chararr_count = 0;

        while (count < utflen)
        {
            c = (int) readByte() & 0xff;
            if (c > 127) break;
            count++;
            chararr[chararr_count++] = (char) c;
        }

        if (count < utflen)
            while (true)
            {
                switch (c >> 4)
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        /* 0xxxxxxx*/
                        count++;
                        chararr[chararr_count++] = (char) c;
                        break;
                    case 12:
                    case 13:
                        /* 110x xxxx   10xx xxxx*/
                        count += 2;
                        if (count > utflen)
                            throw new UTFDataFormatException(
                                                            "malformed input: partial character at end");
                        char2 = (int) readByte();
                        if ((char2 & 0xC0) != 0x80)
                            throw new UTFDataFormatException(
                                                            "malformed input around byte " + count);
                        chararr[chararr_count++] = (char) (((c & 0x1F) << 6) |
                                                           (char2 & 0x3F));
                        break;
                    case 14:
                        /* 1110 xxxx  10xx xxxx  10xx xxxx */
                        count += 3;
                        if (count > utflen)
                            throw new UTFDataFormatException(
                                                            "malformed input: partial character at end");
                        char2 = (int) readByte();
                        char3 = (int) readByte();
                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                            throw new UTFDataFormatException(
                                                            "malformed input around byte " + (count - 1));
                        chararr[chararr_count++] = (char) (((c & 0x0F) << 12) |
                                                           ((char2 & 0x3F) << 6) |
                                                           (char3 & 0x3F));
                        break;
                    default:
                        /* 10xx xxxx,  1111 xxxx */
                        throw new UTFDataFormatException(
                                                        "malformed input around byte " + count);
                }
                if (count >= utflen)
                    break;
                c = (int) readByte() & 0xff;
            }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
}
