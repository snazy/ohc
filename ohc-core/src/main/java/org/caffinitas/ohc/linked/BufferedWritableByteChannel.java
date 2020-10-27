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

import static org.caffinitas.ohc.linked.Util.writeFully;
import static org.caffinitas.ohc.util.ByteBufferCompat.*;

final class BufferedWritableByteChannel implements WritableByteChannel
{
    private final WritableByteChannel delegate;
    private final long bufferAddress;
    private ByteBuffer buffer;
    private boolean closed;

    BufferedWritableByteChannel(WritableByteChannel delegate, int bufferSize) throws IOException
    {
        this.delegate = delegate;
        this.bufferAddress = Uns.allocateIOException(bufferSize);
        this.buffer = Uns.directBufferFor(bufferAddress, 0L, bufferSize, false);
    }

    public int write(ByteBuffer src) throws IOException
    {
        int wr = 0;
        while (true)
        {
            int sr = src.remaining();
            if (sr == 0)
                return wr;
            int br = buffer.remaining();
            if (br == 0)
            {
                byteBufferFlip(buffer);
                writeFully(delegate, buffer);
                byteBufferClear(buffer);
            }
            if (sr > br)
            {
                int lim = src.limit();
                byteBufferLimit(src, src.position() + br);
                buffer.put(src);
                byteBufferPosition(src, src.limit());
                byteBufferLimit(src, lim);
                wr += br;
            }
            else
            {
                buffer.put(src);
                wr += sr;
            }
        }
    }

    public boolean isOpen()
    {
        return buffer != null;
    }

    public void close() throws IOException
    {
        byteBufferFlip(buffer);
        writeFully(delegate, buffer);

        buffer = null;
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
}
