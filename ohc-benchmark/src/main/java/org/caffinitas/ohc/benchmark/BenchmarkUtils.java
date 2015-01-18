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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.caffinitas.ohc.CacheSerializer;

public final class BenchmarkUtils {
    public static final CacheSerializer<byte[]> serializer = new CacheSerializer<byte[]>() {
        public void serialize(byte[] bytes, DataOutput stream) throws IOException {
            stream.writeInt(bytes.length);
            stream.write(bytes);
        }

        public byte[] deserialize(DataInput stream) throws IOException {
            byte[] bytes = new byte[stream.readInt()];
            stream.readFully(bytes);
            return bytes;
        }

        public int serializedSize(byte[] t) {
            return t.length + 4;
        }
    };

    public static final CacheSerializer<Long> longSerializer = new CacheSerializer<Long>()
    {
        public void serialize(Long val, DataOutput out) throws IOException
        {
            out.writeLong(val);
        }

        public Long deserialize(DataInput in) throws IOException
        {
            return in.readLong();
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

        public void serialize(Long val, DataOutput out) throws IOException
        {
            out.writeLong(val);
            for (int i = 0; i < keyLen; i++)
                out.write(0);
        }

        public Long deserialize(DataInput in) throws IOException
        {
            long v = in.readLong();
            for (int i = 0; i < keyLen; i++)
                in.readByte();
            return v;
        }

        public int serializedSize(Long value)
        {
            return 8 + keyLen;
        }
    }
}
