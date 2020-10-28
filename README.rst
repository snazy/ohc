OHC - An off-heap-cache
=======================

Features
========

- asynchronous cache loader support
- optional per entry or default TTL/expireAt
- entry eviction and expiration without a separate thread
- capable of maintaining huge amounts of cache memory
- suitable for tiny/small entries with low overhead using the chunked implementation
- runs with Java 8 and Java 11 - support for Java 7 and earlier has been dropped with version 0.7.0
- to build OHC from source, Java 11 or newer (tested with Java 11 + 15) is required

Performance
===========

OHC shall provide a good performance on both commodity hardware and big systems using non-uniform-memory-architectures.

No performance test results available yet - you may try the ohc-benchmark tool. See instructions below.
A very basic impression on the speed is in the _Benchmarking_ section.

Requirements
============

Java 8 VM that support 64bit and has ``sun.misc.Unsafe`` (Oracle JVMs on x64 Intel CPUs).

OHC is targeted for Linux and OSX. It *should* work on Windows and other Unix OSs.

Architecture
============

OHC provides two implementations for different cache entry characteristics:
- The _linked_ implementation allocates off-heap memory for each entry individually and works best for medium and big entries.
- The _chunked_ implementation allocates off-heap memory for each hash segment as a whole and is intended for small entries.

Linked implementation
---------------------

The number of segments is configured via ``org.caffinitas.ohc.OHCacheBuilder``, defaults to ``# of cpus * 2`` and must
be a power of 2. Entries are distribtued over the segments using the most significant bits of the 64 bit hash code.
Accesses on each segment are synchronized.

Each hash-map entry is allocated individually. Entries are free'd (deallocated), when they are no longer referenced by
the off-heap map itself or any external reference like ``org.caffinitas.ohc.DirectValueAccess`` or a
``org.caffinitas.ohc.CacheSerializer``.

The design of this implementation reduces the locked time of a segment to a very short time. Put/replace operations
allocate memory first, call the ``org.caffinitas.ohc.CacheSerializer`` to serialize the key and value and then put the
fully prepared entry into the segment.

Eviction is performed using an LRU algorithm. A linked list through all cached elements per segment is used to keep
track of the eldest entries.

Chunked implementation
----------------------

Chunked memory allocation off-heap implementation.

Purpose of this implementation is to reduce the overhead for relatively small cache entries compared to the linked
implementation since the memory for the whole segment is pre-allocated. This implementation is suitable for small
entries with fast (de)serialization implementations of ``org.caffinitas.ohc.CacheSerializer``.

Segmentation is the same as in the linked implementation. The number of segments is configured via
``org.caffinitas.ohc.OHCacheBuilder``, defaults to ``# of cpus * 2`` and must be a power of 2. Entries are distribtued
over the segments using the most significant bits of the 64 bit hash code. Accesses on each segment are synchronized.

Each segment is divided into multiple chunks. Each segment is responsible for a portion of the total capacity
``(capacity / segmentCount)``. This amount of memory is allocated once up-front during initialization and logically
divided into a configurable number of chunks. The size of each chunk is configured using the ``chunkSize`` option in
``org.caffinitas.ohc.OHCacheBuilder``.

Like the linked implementation, hash entries are serialized into a temporary buffer first, before the actual put
into a segment occurs (segement operations are synchronized).

New entries are placed into the current write chunk. When that chunk is full, the next empty chunk will become the new
write chunk. When all chunks are full, the least recently used chunk, including all the entries it contains, is evicted.

Specifying the ``fixedKeyLength`` and ``fixedValueLength`` builder properties reduces the memory footprint by
8 bytes per entry.

Serialization, direct access and get-with-loader functions are not supported in this implementation.

To enable the chunked implementation, specify the ``chunkSize`` in ``org.caffinitas.ohc.OHCacheBuilder``.

Note: the chunked implementation should still be considered experimental.

Eviction algorithms
===================

OHC supports three eviction algorithms:

- *LRU*: The oldest (least recently used) entries are evicted to make room for new entries.
- *Window Tiny-LFU*:
  Entries with lower usage frequency are evicted to make room for new entries.
  The goal of this eviction algorithm is to prevent heavily used entries from being evicted.
  Note that the maximum size of entries is limited to the size of the eden generation, which is currently
  fixed at 20% of the segment size (i.e. overall capacity / number of segments).
  Each OHC cache segment is divided into an eden and a main "generation". New entries start in the eden generation
  to give these time to build up their usage frequencies. When the eden generation becomes full, entries in the
  eden generation have to pass the admission filter, which checks the frequencies of the entries in the eden
  generation against the frequencies of the oldest (least recently used) entries in the main generation.
  See `this article <http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html>`_ for a more thorough
  description.
  (Only supported in the _linked_ implementation, not supported by the chunked implementation)
- *None*: OHC performs no eviction on its own. It is up to the caller to check the return values and monitor
  free capacity.
  (Only supported in the _linked_ implementation, not supported by the chunked implementation)

Configuration
=============

Use the class ``OHCacheBuilder`` to configure all necessary parameter like

- number of segments (must be a power of 2), defaults to number-of-cores * 2
- hash table size (must be a power of 2), defaults to 8192
- load factor, defaults to .75
- capacity for data over the whole cache
- key and value serializers
- default TTL
- optional unlocked mode

Generally you should work with a large hash table. The larger the hash table, the shorter the linked-list in each
hash partition - that means less linked-link walks and increased performance.

The total amount of required off heap memory is the *total capacity* plus *hash table*. Each hash bucket (currently)
requires 8 bytes - so the formula is ``capacity + segment_count * hash_table_size * 8``.

OHC allocates off-heap memory directly bypassing Java's off-heap memory limitation. This means, that all
memory allocated by OHC is not counted towards ``-XX:maxDirectMemorySize``.

Memory & jemalloc
=================

Since especially the linked implementation performs alloc/free operations for each individual entry, consider that
memory fragmentation can happen.

Also leave some head room since some allocations might still be in flight and also "the other stuff"
(operating system, JVM, etc) need memory. It depends on the usage pattern how much head room is necessary.
Note that the linked implementation allocates memory during write operations _before_ it is counted towards the
segments, which will evict older entries. This means: do not dedicate all available memory to OHC.

We recommend using jemalloc to keep fragmentation low. On Unix operating systems, preload jemalloc.

OSX usually does not require jemalloc for performance reasons. Also make sure that you are using a recent version of
jemalloc - some Linux distributions still provide quite old versions.

To preload jemalloc on Linux, use
``export LD_PRELOAD=<path-to-libjemalloc.so``, to preload jemalloc on OSX, use
``export DYLD_INSERT_LIBRARIES=<path-to-libjemalloc.so``. A script template for preloading can be found at the
`Apache Cassandra project <https://github.com/apache/cassandra/blob/bf3255fc93db65b816b016958967003df38a6004/bin/cassandra#L135-L182>`_.

Usage
=====

Quickstart::

 OHCache ohCache = OHCacheBuilder.newBuilder()
                                 .keySerializer(yourKeySerializer)
                                 .valueSerializer(yourValueSerializer)
                                 .build();

This quickstart uses the very least default configuration:

- total cache capacity of 64MB or 16 * number-of-cpus, whichever is smaller
- number of segments is 2 * number of cores
- 8192 buckets per segment
- load factor of .75
- your custom key serializer
- your custom value serializer
- no maximum serialized cache entry size

See javadoc of ``CacheBuilder`` for a complete list of options.

Key and value serializers need to implement the ``CacheSerializer`` interface. This interface has three methods:

- ``int serializedSize(T t)`` to return the serialized size of the given object
- ``void serialize(Object obj, DataOutput out)`` to serialize the given object to the data output
- ``T deserialize(DataInput in)`` to deserialize an object from the data input

Building from source
====================

Clone the git repo to your local machine. Either use the stable master branch or a release tag.

``git clone https://github.com/snazy/ohc.git``

You need OpenJDK 11 or newer to build from source. Just execute

``mvn clean install``

Benchmarking
============

You need to build OHC from source because the big benchmark artifacts are not uploaded to Maven Central.

Execute ``java -jar ohc-benchmark/target/ohc-benchmark-0.7.1-SNAPSHOT.jar -h`` (when building from source)
to get some help information.

Generally the benchmark tool starts a bunch of threads and performs _get_ and _put_ operations concurrently
using configurable key distributions for _get_ and _put_ operations. Value size distribution also needs to be configured.

Available command line options::

 -cap <arg>    size of the cache
 -d <arg>      benchmark duration in seconds
 -h            help, print this command
 -lf <arg>     hash table load factor
 -r <arg>      read-write ration (as a double 0..1 representing the chance for a read)
 -rkd <arg>    hot key use distribution - default: uniform(1..10000)
 -sc <arg>     number of segments (number of individual off-heap-maps)
 -t <arg>      threads for execution
 -vs <arg>     value sizes - default: fixed(512)
 -wkd <arg>    hot key use distribution - default: uniform(1..10000)
 -wu <arg>     warm up - <work-secs>,<sleep-secs>
 -z <arg>      hash table size
 -cs <arg>     chunk size - if specified it will use the "chunked" implementation
 -fks <arg>    fixed key size in bytes
 -fvs <arg>    fixed value size in bytes
 -mes <arg>    max entry size in bytes
 -unl          do not use locking - only appropiate for single-threaded mode
 -hm <arg>     hash algorithm to use - MURMUR3, XX, CRC32
 -bh           show bucket historgram in stats
 -kl <arg>     enable bucket histogram. Default: false

Distributions for read keys, write keys and value sizes can be configured using the following functions::

 EXP(min..max)                        An exponential distribution over the range [min..max]
 EXTREME(min..max,shape)              An extreme value (Weibull) distribution over the range [min..max]
 QEXTREME(min..max,shape,quantas)     An extreme value, split into quantas, within which the chance of selection is uniform
 GAUSSIAN(min..max,stdvrng)           A gaussian/normal distribution, where mean=(min+max)/2, and stdev is (mean-min)/stdvrng
 GAUSSIAN(min..max,mean,stdev)        A gaussian/normal distribution, with explicitly defined mean and stdev
 UNIFORM(min..max)                    A uniform distribution over the range [min, max]
 FIXED(val)                           A fixed distribution, always returning the same value
 Preceding the name with ~ will invert the distribution, e.g. ~exp(1..10) will yield 10 most, instead of least, often
 Aliases: extr, qextr, gauss, normal, norm, weibull

(Note: these are similar to the Apache Cassandra stress tool - if you know one, you know both ;)

Quick example with a read/write ratio of ``.9``, approx 1.5GB max capacity, 16 threads that runs for 30 seconds::

 java -jar ohc-benchmark/target/ohc-benchmark-0.5.1-SNAPSHOT.jar


(Note that the version in the jar file name might differ.)

On a 2.6GHz Core i7 system (OSX) the following numbers are typical running the above benchmark (.9 read/write ratio):

- # of gets per second: 2500000
- # of puts per second:  270000

Why off-heap memory
===================

When using a very huge number of objects in a very large heap, Virtual machines will suffer from increased GC
pressure since it basically has to inspect each and every object whether it can be collected and has to access all
memory pages. A cache shall keep a hot set of objects accessible for fast access (e.g. omit disk or network
roundtrips). The only solution is to use native memory - and there you will end up with the choice either
to use some native code (C/C++) via JNI or use direct memory access.

Native code using C/C++ via JNI has the drawback that you have to naturally write C/C++ code for each and
every platform. Although most Unix OS (Linux, OSX, BSD, Solaris) are quite similar when dealing with things
like compare-and-swap or Posix libraries, you usually also want to support the other platform (Windows).

Both native code and direct memory access have the drawback that they have to "leave" the JVM "context" -
want to say that access to off heap memory is slower than access to data in the Java heap and that each JNI call
has some "escape from JVM context" cost.

But off heap memory is great when you have to deal with a huge amount of several/many GB of cache memory since
that dos not put any pressure on the Java garbage collector. Let the Java GC do its job for the application where
this library does its job for the cached data.

Why *not* use ByteBuffer.allocateDirect()?
==========================================

TL;DR allocating off-heap memory directly and bypassing ``ByteBuffer.allocateDirect`` is very gentle to the
GC and we have explicit control over memory allocation and, more importantly, free. The stock implementation
in Java frees off-heap memory during a garbage collection - also: if no more off-heap memory is available, it
likely triggers a Full-GC, which is problematic if multiple threads run into that situation concurrently since
it means lots of Full-GCs sequentially. Further, the stock implementation uses a global, synchronized linked
list to track off-heap memory allocations.

This is why OHC allocates off-heap memory directly and recommends to preload jemalloc on Linux systems to
improve memory managment performance.

History
=======

OHC was developed in 2014/15 for `Apache Cassandra <http://cassandra.apache.org/>`_ 2.2 and 3.0 to be used as the
`new row-cache backend <https://issues.apache.org/jira/browse/CASSANDRA-7438>`_.

Since there were no suitable fully off-heap cache implementations available, it has been decided to
build a completely new one - and that's OHC. But it turned out that OHC alone might also be usable for
other projects - that's why OHC is a separate library.

Contributors
============

A big 'thank you' has to go to `Benedict Elliott Smith <https://twitter.com/_belliottsmith>`_ and
`Ariel Weisberg <https://twitter.com/ArielWeisberg>`_ from DataStax for their very useful input to OHC!

`Ben Manes <https://twitter.com/benmanes>`_, the author of `Caffeine <https://github.com/ben-manes/caffeine/>`_,
the highly configurable on-heap cache using W-Tiny LFU.

Developer: `Robert Stupp <https://twitter.com/snazy>`_

License
=======

Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
