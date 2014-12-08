/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.caffinitas.ohc.alloc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import com.sun.jna.Function;
import com.sun.jna.InvocationMapper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;

public class JEMallocAllocator implements IAllocator
{
    public interface JEMLibrary extends Library
    {
        long malloc(long size);

        void free(long pointer);
    }

    private final JEMLibrary library;
    
    public JEMallocAllocator()
    {
        Map options = Collections.singletonMap(Library.OPTION_INVOCATION_MAPPER,
                                 // Invocation mapping
                                 new InvocationMapper() {
                                     public InvocationHandler getInvocationHandler(NativeLibrary lib, Method m) {
                                         final Function f = lib.getFunction(m.getName());
                                         if ("malloc".equals(m.getName()))
                                             return new InvocationHandler() {
                                                 public Object invoke(Object proxy, Method method, Object[] args) {
                                                     return f.invokeLong(args);
                                                 }
                                             };
                                         if ("free".equals(m.getName()))
                                             return new InvocationHandler() {
                                                 public Object invoke(Object proxy, Method method, Object[] args) {
                                                     f.invoke(args);
                                                     return null;
                                                 }
                                             };
                                         return null;
                                     }
                                 } );
        library = (JEMLibrary) Native.loadLibrary("jemalloc", JEMLibrary.class, options);
    }

    public long allocate(long size)
    {
        return library.malloc(size);
    }

    public void free(long peer)
    {
        library.free(peer);
    }
}
