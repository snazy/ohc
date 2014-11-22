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

public interface BytesSource
{
    int size();

    byte getByte(int pos);

    boolean hasArray();

    byte[] array();

    int arrayOffset();

    public static abstract class AbstractSource implements BytesSource
    {

        public boolean hasArray()
        {
            return false;
        }

        public byte[] array()
        {
            throw new UnsupportedOperationException();
        }

        public int arrayOffset()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static final class EmptySource extends AbstractSource
    {
        public int size()
        {
            return 0;
        }

        public byte getByte(int pos)
        {
            return 0;
        }
    }

    public static final class StringSource extends ByteArraySource
    {
        public StringSource(String s)
        {
            super(s.getBytes());
        }
    }

    public static class ByteArraySource implements BytesSource
    {
        private final byte[] array;
        private final int off;
        private final int len;

        public ByteArraySource(byte[] array)
        {
            this(array, 0, array.length);
        }

        public ByteArraySource(byte[] array, int off, int len)
        {
            if (array == null)
                throw new NullPointerException();
            if (off < 0 || len < 0)
                throw new IllegalArgumentException();
            this.array = array;
            this.off = off;
            this.len = len;
        }

        public int size()
        {
            return len;
        }

        public byte getByte(int pos)
        {
            if (pos < 0 || pos >= len)
                throw new ArrayIndexOutOfBoundsException();
            return array[off + pos];
        }

        public boolean hasArray()
        {
            return true;
        }

        public byte[] array()
        {
            return array;
        }

        public int arrayOffset()
        {
            return off;
        }
    }
}
