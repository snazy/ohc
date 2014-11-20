package org.caffinitas.ohc;

import java.io.Closeable;

public interface OHCache extends Closeable
{
    int getBlockSize();

    int getHashTableSize();

    long getTotalCapacity();

    int calcFreeBlockCount();

    boolean put(int hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink);

    boolean get(int hash, BytesSource keySource, BytesSink valueSink);

    boolean remove(int hash, BytesSource keySource);
}
