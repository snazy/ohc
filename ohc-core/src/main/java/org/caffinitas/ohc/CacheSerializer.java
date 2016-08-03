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
 * Serialize and deserialize cached data using {@link java.nio.ByteBuffer}
 */
public interface CacheSerializer<T>
{
    /**
     * Serialize the specified type into the specified {@code ByteBuffer} instance.
     *
     * @param value non-{@code null} object that needs to be serialized
     * @param buf   {@code ByteBuffer} into which serialization needs to happen.
     */
    void serialize(T value, ByteBuffer buf);

    /**
     * Deserialize from the specified {@code DataInput} instance.
     * <p>
     * Implementations of this method should never return {@code null}. Although there <em>might</em> be
     * no explicit runtime checks, a violation would break the contract of several API methods in
     * {@link OHCache}. For example users of {@link OHCache#get(Object)} might not be able to distinguish
     * between a non-existing entry or the "value" {@code null}. Instead, consider returning a singleton
     * replacement object.
     * </p>
     *
     * @param buf {@code ByteBuffer} from which deserialization needs to happen.
     * @return the type that was deserialized. Must not return {@code null}.
     */
    T deserialize(ByteBuffer buf);

    /**
     * Calculate the number of bytes that will be produced by {@link #serialize(Object, java.nio.ByteBuffer)}
     * for given object {@code t}.
     *
     * @param value non-{@code null} object to calculate serialized size for
     * @return serialized size of {@code t}
     */
    int serializedSize(T value);
}

