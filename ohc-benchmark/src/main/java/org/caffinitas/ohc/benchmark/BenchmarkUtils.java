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
package org.caffinitas.ohc.benchmark;

import java.nio.ByteBuffer;

import org.caffinitas.ohc.CacheSerializer;

public final class BenchmarkUtils {
    public static final CacheSerializer<byte[]> serializer = new CacheSerializer<byte[]>() {
        public void serialize(byte[] bytes, ByteBuffer buf) {
            buf.putInt(bytes.length);
            buf.put(bytes);
        }

        public byte[] deserialize(ByteBuffer buf) {
            byte[] bytes = new byte[buf.getInt()];
            buf.get(bytes);
            return bytes;
        }

        public int serializedSize(byte[] t) {
            return t.length + 4;
        }
    };

    public static final CacheSerializer<Long> longSerializer = new CacheSerializer<Long>()
    {
        public void serialize(Long val, ByteBuffer buf)
        {
            buf.putLong(val);
        }

        public Long deserialize(ByteBuffer buf)
        {
            return buf.getLong();
        }

        public int serializedSize(Long value)
        {
            return 8;
        }
    };

    public static class KeySerializer implements CacheSerializer<Long>
    {
        private final int keyLen;

        public KeySerializer(int keyLen)
        {
            this.keyLen = keyLen;
        }

        public void serialize(Long val, ByteBuffer buf)
        {
            buf.putLong(val);
            for (int i = 0; i < keyLen; i++)
                buf.put((byte)(0 & 0xff));
        }

        public Long deserialize(ByteBuffer buf)
        {
            long v = buf.getLong();
            for (int i = 0; i < keyLen; i++)
                buf.get();
            return v;
        }

        public int serializedSize(Long value)
        {
            return 8 + keyLen;
        }
    }
}
