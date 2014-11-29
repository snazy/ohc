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
package org.caffinitas.ohc.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CacheSerializer<T>
{
    /**
     * Serialize the specified type into the specified DataOutput instance.
     *
     * @param t   type that needs to be serialized
     * @param out OutputStream into which serialization needs to happen.
     * @throws java.io.IOException
     */
    public void serialize(T t, DataOutput out) throws IOException;

    /**
     * Deserialize from the specified DataInput instance.
     *
     * @param in InputStream from which deserialization needs to happen.
     * @return the type that was deserialized
     * @throws IOException
     */
    public T deserialize(DataInput in) throws IOException;

    public long serializedSize(T t);
}

