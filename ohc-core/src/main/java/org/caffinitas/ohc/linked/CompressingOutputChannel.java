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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.xerial.snappy.Snappy;

import static org.caffinitas.ohc.linked.Util.HEADER_COMPRESSED;
import static org.caffinitas.ohc.linked.Util.writeFully;
import static org.caffinitas.ohc.util.ByteBufferCompat.*;

final class CompressingOutputChannel implements WritableByteChannel
{
    private final WritableByteChannel delegate;
    private final long bufferAddress;
    private final int uncompressedChunkSize;
    private ByteBuffer buffer;
    private boolean closed;

    /**
     * @param delegate              channel to write to
     * @param uncompressedChunkSize chunk size of uncompressed that data that can be compressed in one iteration
     */
    CompressingOutputChannel(WritableByteChannel delegate, int uncompressedChunkSize) throws IOException
    {
        this.delegate = delegate;
        int maxCLen = Snappy.maxCompressedLength(uncompressedChunkSize);
        int bufferCapacity = 4 + maxCLen;
        this.bufferAddress = Uns.allocateIOException(bufferCapacity);
        this.buffer = Uns.directBufferFor(bufferAddress, 0L, bufferCapacity, false);
        this.uncompressedChunkSize = uncompressedChunkSize - 4;

        buffer.putInt(HEADER_COMPRESSED);
        buffer.putInt(1);
        buffer.putInt(uncompressedChunkSize);
        buffer.putInt(maxCLen);
        byteBufferFlip(buffer);
        delegate.write(buffer);
        byteBufferClear(buffer);
    }

    public void close()
    {
        if (buffer == null)
            return;

        this.buffer = null;
        if (!closed)
            Uns.free(bufferAddress);
        closed = true;
    }

    protected void finalize() throws Throwable
    {
        if (!closed)
            Uns.free(bufferAddress);
        super.finalize();
    }

    public int write(ByteBuffer src) throws IOException
    {
        int sz = src.remaining();

        ByteBuffer s = src.duplicate();
        byteBufferPosition(src, src.position() + sz);

        while (sz > 0)
        {
            int chunkSize = sz > uncompressedChunkSize ? uncompressedChunkSize : sz;

            // TODO add output buffering ?

            // write a block of compressed data prefixed by an int indicating the length of the compressed buffer
            byteBufferClear(buffer);
            byteBufferPosition(buffer, 4);
            byteBufferLimit(s, s.position() + chunkSize);
            int cLen = Snappy.compress(s, buffer);
            buffer.putInt(0, cLen);

            byteBufferPosition(buffer, 0);
            byteBufferLimit(buffer, 4 + cLen);
            writeFully(delegate, buffer);

            byteBufferPosition(s, s.position() + chunkSize);
            sz -= chunkSize;
        }

        return sz;
    }

    public boolean isOpen()
    {
        return buffer != null;
    }
}
