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
import java.util.List;
import java.util.concurrent.Future;

import com.google.common.base.Charsets;

import org.caffinitas.ohc.CacheSerializer;

public class BenchmarkUtils {
    public static CacheSerializer<String> serializer = new CacheSerializer<String>() {
        public void serialize(String t, DataOutput stream) throws IOException {
            byte[] bytes = t.getBytes(Charsets.UTF_8);
            stream.writeInt(bytes.length);
            stream.write(bytes);
        }

        public String deserialize(DataInput stream) throws IOException {
            byte[] bytes = new byte[stream.readInt()];
            stream.readFully(bytes);
            return new String(bytes, Charsets.UTF_8);
        }

        public long serializedSize(String t) {
            return t.getBytes(Charsets.UTF_8).length + 4;
        }
    };

    public static <T> void waitOnFuture(List<Future<T>> futures) throws Exception {
        for (Future<T> future : futures)
            future.get();
        futures.clear();
    }
}
