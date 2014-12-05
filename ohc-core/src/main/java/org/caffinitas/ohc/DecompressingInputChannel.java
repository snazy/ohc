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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import org.xerial.snappy.Snappy;

import static org.caffinitas.ohc.Constants.*;

final class DecompressingInputChannel implements SeekableByteChannel
{
    private final SeekableByteChannel delegate;

    private final long readAddress;
    private ByteBuffer readBuffer;

    private final long compressedAddress;
    private ByteBuffer compressedBuffer;

    private final long decompressedAddress;
    private ByteBuffer decompressedBuffer;

    private long position;

    /**
     * @param delegate channel to read from
     * @param readBufferSize buffer size for reads from {@code delegate}
     */
    DecompressingInputChannel(SeekableByteChannel delegate, int readBufferSize) throws IOException
    {
        long headerAdr = Uns.allocate(16);
        int bufferSize;
        int maxCLen;
        try
        {
            ByteBuffer header = Uns.directBufferFor(headerAdr, 0, 16);
            readFully(delegate, header);
            header.flip();
            if (header.getInt() != HEADER)
                throw new IOException("Illegal file header");
            if (header.getInt() != 1)
                throw new IOException("Illegal file version");
            bufferSize = header.getInt();
            maxCLen = header.getInt();
        }
        finally
        {
            Uns.free(headerAdr);
        }

        if (readBufferSize < 4096)
            readBufferSize = 4096;
        this.readAddress = Uns.allocate(readBufferSize);
        if (readAddress == 0L)
            throw new IOException("Unable to allocate " + readBufferSize + " bytes in off-heap");
        this.readBuffer = Uns.directBufferFor(readAddress, 0L, readBufferSize);
        this.readBuffer.position(readBuffer.limit());

        this.delegate = delegate;
        this.compressedAddress = Uns.allocate(maxCLen);
        if (compressedAddress == 0L)
        {
            Uns.free(readAddress);
            throw new IOException("Unable to allocate " + maxCLen + " bytes in off-heap");
        }
        this.compressedBuffer = Uns.directBufferFor(compressedAddress, 0L, maxCLen);
        this.compressedBuffer.position(compressedBuffer.limit());

        this.decompressedAddress = Uns.allocate(bufferSize);
        if (decompressedAddress == 0L)
        {
            Uns.free(readAddress);
            Uns.free(compressedAddress);
            throw new IOException("Unable to allocate " + bufferSize + " bytes in off-heap");
        }
        this.decompressedBuffer = Uns.directBufferFor(decompressedAddress, 0L, bufferSize);
        this.decompressedBuffer.position(decompressedBuffer.limit());
    }

    public void close() throws IOException
    {
        if (compressedBuffer == null)
            return;

        this.readBuffer = null;
        this.compressedBuffer = null;
        this.decompressedBuffer = null;
        Uns.free(readAddress);
        Uns.free(compressedAddress);
        Uns.free(decompressedAddress);
    }

    public boolean eof() throws IOException
    {
        return readBuffer.remaining() == 0
               && decompressedBuffer.remaining() == 0
               && delegate.position() >= delegate.size();
    }

    public int read(ByteBuffer dst) throws IOException
    {
        int r = decompressedBuffer.remaining();
        if (r == 0)
        {
            // read length of compressed buffer
            readBytes(4);
            int cLen = compressedBuffer.getInt(0);

            // read compressed block
            readBytes(cLen);

            // decompress
            decompressedBuffer.clear();
            if (!Snappy.isValidCompressedBuffer(compressedBuffer))
                throw new IOException("Invalid compressed data");
            r = Snappy.uncompress(compressedBuffer, decompressedBuffer);
        }

        int dstRem = dst.remaining();
        if (dstRem < r)
        {
            ByteBuffer dDup = decompressedBuffer.duplicate();
            dDup.limit(dDup.position() + dstRem);
            dst.put(dDup);
            decompressedBuffer.position(decompressedBuffer.position() + dstRem);
            return dstRem;
        }
        else
            dst.put(decompressedBuffer);

        return r - decompressedBuffer.remaining();
    }

    private void readBytes(int len) throws IOException
    {
        // read compressed buffer
        compressedBuffer.clear();
        compressedBuffer.limit(len);
        while (compressedBuffer.remaining() > 0)
        {
            if (readBuffer.remaining() == 0)
            {
                readBuffer.clear();
                long av = delegate.size() - delegate.position();
                if (av < readBuffer.capacity())
                    readBuffer.limit((int) av);
                readFully(delegate, readBuffer);
                readBuffer.flip();
            }

            int cbRem = compressedBuffer.remaining();
            if (readBuffer.remaining() > cbRem)
            {
                int rbLimit = readBuffer.limit();
                readBuffer.limit(readBuffer.position() + cbRem);
                compressedBuffer.put(readBuffer);
                readBuffer.limit(rbLimit);
            }
            else
                compressedBuffer.put(readBuffer);
        }
        compressedBuffer.position(0);
    }

    public SeekableByteChannel position(long newPosition) throws IOException
    {
        long diff = newPosition - position;
        if (diff < 0L)
            throw new IllegalStateException("can only seek forward");
        if (diff > 0L)
        {
            // TODO improve and test this one...
            long tmpAdr = Uns.allocate(4096);
            if (tmpAdr == 0L)
                throw new IOException("Unable to allocate 4096 bytes in off-heap");
            try
            {
                ByteBuffer tmpBuf = Uns.directBufferFor(tmpAdr, 0L, 4096);
                while (diff > 0L)
                {
                    tmpBuf.clear();
                    int n;
                    if (diff >= 4096)
                        n = read(tmpBuf);
                    else
                    {
                        ByteBuffer b = tmpBuf.duplicate();
                        b.limit((int) diff);
                        n = read(tmpBuf);
                    }
                    position += n;
                    diff -= n;
                }
            }
            finally
            {
                Uns.free(tmpAdr);
            }
        }
        return this;
    }

    public long position() throws IOException
    {
        return position;
    }

    // following stuff is not needed by SegmentedCacheImpl

    public int write(ByteBuffer src) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public long size() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public SeekableByteChannel truncate(long size) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen()
    {
        return compressedBuffer != null;
    }
}
