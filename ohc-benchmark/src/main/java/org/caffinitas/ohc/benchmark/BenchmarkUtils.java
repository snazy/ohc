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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Future;

import com.google.common.base.Charsets;

import org.caffinitas.ohc.CacheSerializer;

public class BenchmarkUtils {
    public static CacheSerializer<String> serializer = new CacheSerializer<String>() {
        public void serialize(String t, OutputStream stream) throws IOException {
            DataOutputStream out = new DataOutputStream(stream);
            byte[] bytes = t.getBytes(Charsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        public String deserialize(InputStream stream) throws IOException {
            DataInputStream in = new DataInputStream(stream);
            byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
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
