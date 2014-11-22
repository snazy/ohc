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

import java.io.Closeable;
import java.util.Iterator;

import com.google.common.cache.Cache;

public interface OHCache<K, V> extends Cache<K, V>, Closeable
{
    int getBlockSize();

    int getHashTableSize();

    long getCapacity();

    long getMemUsed();

    int calcFreeBlockCount();

    double getFreeSpacePercentage();

    PutResult put(int hash, BytesSource keySource, BytesSource valueSource);

    PutResult put(int hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink);

    boolean get(int hash, BytesSource keySource, BytesSink valueSink);

    boolean remove(int hash, BytesSource keySource);

    Iterator<K> hotN(int n);
}
