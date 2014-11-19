package org.caffinitas.ohc;

public interface BytesSource
{
    int size();

    byte getByte(int pos);
}
