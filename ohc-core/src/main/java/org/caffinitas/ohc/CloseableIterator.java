package org.caffinitas.ohc;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<E> extends Closeable, Iterator<E>
{
}
