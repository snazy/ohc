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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Function;
import com.sun.jna.InvocationMapper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;

public class JEMallocAllocator implements IAllocator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JEMallocAllocator.class);

    public interface JEMLibrary extends Library
    {
        long malloc(long size);

        void free(long pointer);

        // jemalloc's configure script defaults:
        //
        // OSX:
        // JEMALLOC_PREFIX    : je_
        // JEMALLOC_PRIVATE_NAMESPACE : je_
        //
        // Linux:
        // JEMALLOC_PREFIX    :
        // JEMALLOC_PRIVATE_NAMESPACE : je_
        //
        // Means: mallctl is available as "je_mallctl" on OSX and as "mallctl" on Linux - need to find a solution for this

        //int je_mallctl(String name, Pointer oldp, Pointer oldlenp, Pointer newp, long newlen);
    }

    private final JEMLibrary library;
    
    public JEMallocAllocator()
    {
        // use a custom invocation mapper to reduce unnecessary argument mapping and temporary objects
        Map options = Collections.singletonMap(Library.OPTION_INVOCATION_MAPPER,
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

        //
        // see `man jemalloc`
        //
//        if (mallctlBool("opt.zero"))
//            LOGGER.warn("jemalloc is has opt.fill enabled - this leads to performance penalties");
//        if (mallctlBool("opt.redzone"))
//            LOGGER.warn("jemalloc is has opt.redzone enabled - this leads to performance penalties");
//        if (mallctlBool("opt.junk"))
//            LOGGER.warn("jemalloc is has opt.junk enabled - this leads to performance penalties");
//        if (mallctlBool("config.debug"))
//            LOGGER.warn("jemalloc is compiled with debug code - this leads to performance penalties");
//        if (mallctlBool("config.fill"))
//            LOGGER.warn("jemalloc is compiled with fill code - this leads to performance penalties");
    }

    public long getTotalAllocated()
    {
        return -1L;
//        long allocated = mallctlSizeT("stats.allocated");
//        long hugeAllocated = mallctlSizeT("stats.huge.allocated");
//        return allocated + hugeAllocated;
    }

//    private boolean mallctlBool(String name)
//    {
//        Memory oldp = new Memory(1);
//        Memory oldlenp = new Memory(NativeLong.SIZE);
//        oldlenp.setNativeLong(0, new NativeLong(oldp.size()));
//        int r = library.je_mallctl(name, oldp, oldlenp, null, 0);
//        return r == 0 && oldp.getByte(0) != 0;
//    }
//
//    private long mallctlSizeT(String name)
//    {
//        Memory oldp = new Memory(NativeLong.SIZE);
//        Memory oldlenp = new Memory(NativeLong.SIZE);
//        oldlenp.setNativeLong(0, new NativeLong(oldp.size()));
//        int r = library.je_mallctl(name, oldp, oldlenp, null, 0);
//        return r != 0 ? 0L : oldp.getNativeLong(0).longValue();
//    }

    public long allocate(long size)
    {
        return library.malloc(size);
    }

    public void free(long peer)
    {
        library.free(peer);
    }
}
