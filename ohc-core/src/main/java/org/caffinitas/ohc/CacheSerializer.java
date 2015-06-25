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

import java.nio.ByteBuffer;

/**
 * Serialize and deserialize cached data using {@link java.io.ByteBuffer}
 */
public interface CacheSerializer<T>
{
    /**
     * Serialize the specified type into the specified {@code ByteBuffer} instance.
     *
     * @param t   type that needs to be serialized
     * @param buf {@code ByteBuffer} into which serialization needs to happen.
     */
    public void serialize(T t, ByteBuffer buf);

    /**
     * Deserialize from the specified {@code DataInput} instance.
     *
     * @param buf {@code ByteBuffer} from which deserialization needs to happen.
     * @return the type that was deserialized
     */
    public T deserialize(ByteBuffer buf);

    /**
     * Calculate the number of bytes that will be produced by {@link #serialize(Object, java.nio.ByteBuffer)}
     * for given object {@code t}.
     *
     * @param t object to calculate serialized size for
     * @return serialized size of {@code t}
     */
    public int serializedSize(T t);
}

