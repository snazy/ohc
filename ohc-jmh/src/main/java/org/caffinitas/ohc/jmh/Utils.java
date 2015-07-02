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
package org.caffinitas.ohc.jmh;

import java.nio.ByteBuffer;

import org.caffinitas.ohc.CacheSerializer;

public final class Utils
{
    public static final CacheSerializer<byte[]> byteArraySerializer = new CacheSerializer<byte[]>()
    {
        public void serialize(byte[] bytes, ByteBuffer buf)
        {
            buf.putInt(bytes.length);
            buf.put(bytes);
        }

        public byte[] deserialize(ByteBuffer buf)
        {
            byte[] arr = new byte[buf.getInt()];
            buf.get(arr);
            return arr;
        }

        public int serializedSize(byte[] bytes)
        {
            return 4 + bytes.length;
        }
    };

    public static final CacheSerializer<Integer> intSerializer = new CacheSerializer<Integer>()
    {
        public void serialize(Integer integer, ByteBuffer buf)
        {
            buf.putInt(integer);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            return buf.getInt();
        }

        public int serializedSize(Integer integer)
        {
            return 4;
        }
    };
}
