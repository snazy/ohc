package org.caffinitas.ohc;

import java.io.Closeable;

public interface OHCache extends Closeable
{
    int getBlockSize();

    int getHashTableSize();

    long getTotalCapacity();

    int calcFreeBlockCount();

    void put(int hash, BytesSource keySource, BytesSource valueSource);

    void get(int hash, BytesSource keySource, BytesSink valueSink);

    void remove(int hash, BytesSource keySource);
}
