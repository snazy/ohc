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
import java.nio.channels.ReadableByteChannel;

import static org.caffinitas.ohc.util.ByteBufferCompat.*;

final class BufferedReadableByteChannel implements ReadableByteChannel
{
    private final ReadableByteChannel delegate;
    private final long bufferAddress;
    private ByteBuffer buffer;
    private boolean closed;

    BufferedReadableByteChannel(ReadableByteChannel delegate, int bufferSize) throws IOException
    {
        this.delegate = delegate;
        this.bufferAddress = Uns.allocateIOException(bufferSize);
        this.buffer = Uns.directBufferFor(bufferAddress, 0L, bufferSize, false);
        byteBufferPosition(this.buffer, bufferSize);
    }

    public int read(ByteBuffer dst) throws IOException
    {
        int p = dst.position();
        while (true)
        {
            int dr = dst.remaining();
            if (dr == 0)
                return dst.position() - p;

            int br = buffer.remaining();
            if (br == 0)
            {
                byteBufferClear(buffer);
                int rd = delegate.read(buffer);
                if (rd == -1)
                {
                    rd = dst.position() - p;
                    return rd == 0 ? -1 : rd;
                }
                byteBufferFlip(buffer);
                br = buffer.remaining();
            }

            if (dr >= br)
                dst.put(buffer);
            else
            {
                int lim = buffer.limit();
                byteBufferLimit(buffer, buffer.position() + dr);
                dst.put(buffer);
                byteBufferLimit(buffer, lim);
            }
        }
    }

    public boolean isOpen()
    {
        return buffer != null;
    }

    public void close()
    {
        buffer = null;
        if (!closed)
            Uns.free(bufferAddress);
        closed = true;
    }

    protected void finalize() throws Throwable
    {
        close();
        super.finalize();
    }
}
