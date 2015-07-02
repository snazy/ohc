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
import java.nio.ByteBuffer;

/**
 * Returned by {@link org.caffinitas.ohc.OHCache} for direct/random access to cached values and must be closed after use.
 * <p>
 * You must close the returned {@link DirectValueAccess} instance after use.
 * After closing, you must not call any of the methods of the {@link java.nio.ByteBuffer}
 * returned by {@link #buffer()}.
 * </p>
 */
public interface DirectValueAccess extends Closeable
{
    ByteBuffer buffer();
}
