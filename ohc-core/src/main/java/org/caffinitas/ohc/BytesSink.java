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

import java.nio.charset.Charset;

public interface BytesSink
{
    void setSize(int size);

    void putByte(int pos, byte value);

    boolean hasArray();

    byte[] array();

    int arrayOffset();

    public abstract class AbstractSink implements BytesSink
    {
        public int arrayOffset()
        {
            throw new UnsupportedOperationException();
        }

        public byte[] array()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasArray()
        {
            return false;
        }
    }

    public static class ByteArraySink implements BytesSink
    {

        private byte[] array;

        public int arrayOffset()
        {
            return 0;
        }

        public byte[] array()
        {
            return array;
        }

        public boolean hasArray()
        {
            return true;
        }

        public void setSize(int size)
        {
            if (size < 0)
                throw new IllegalArgumentException();
            this.array = new byte[size];
        }

        public void putByte(int pos, byte value)
        {
            if (pos < 0 || pos >= array.length)
                throw new IllegalArgumentException();
            array[pos] = value;
        }

        public String toString()
        {
            return array != null ? new String(array, Charset.forName("UTF-8")) : "";
        }
    }
}
