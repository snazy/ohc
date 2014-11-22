OHC - A off-heap-cache
======================

Status
------

This library should be considered as *experimental* and *not production ready*.

Performance
-----------

(No performance test results available yet - you may try the ohc-benchmark tool)

OHC shall provide a good performance on commodity hardware and on big systems using non-uniform-memory-architectures.

Generally you should work with a large hash table. The larger the hash table, the shorter the LRU list in each
hash partition - that means less LRU link walks and increased performance.

Requirements
------------

Java7 VM that support 64bit field CAS and ``sun.misc.Unsafe`` (Oracle JVMs on x64 Intel CPUs).

Architecture
------------

OHC uses a simple hash table as its primary data structure. Each hash partition basically consists of a pointer
to the last recently used hash entry. And each hash entry has a pointer to its successor and predecessor building a
LRU list. The hash table does not support re-hashing - it must be sized appropriately by the user.

Data memory is organized in fixed size blocks. Multiple free-block-lists are used as a simple memory management.

CAS based locks are used on each free list, each hash partition and each hash entry. Hash entry locks are
required because OHC tries to release each lock on a whole hash partition as soon as possible. OHC has to
ensure that data blocks for a hash entry are not recycled for other data while the application is reading from it.

OHC will provide a purely LRU based eviction mechanism. Eviction shall take place concurrently with a very low
influence to the overall performance.

Put operations do not succeed if there is not enough free space to serialize the data. Reason is that OHC will
not block any operation longer than really necessary. This should be completely fine for caches since it is better
to let a cache-put not succeed than to block the calling application.

Plain ``OHCache`` interface provides low level get/put/remove operations that take a ``BytesSource`` or a
``BytesSink``. Reason for this is that you usually do not need to allocate more objects in the calling code -
with the cost of a bit more verbose coding. For convenience ``OHCache`` extends the Guava ``Cache`` interface
that takes Java objects as keys and values - you have to provide appropriate key and value serializers then.

By default OHC performs cache eviction regularly on its own (default is to keep 25% free space). But be aware
that calculation of entries to evict is based on averages and does its job not very accurately to increase
overall performance.

Note on the ``hotN`` function: The implementation will take N divided by number of hash partitions keys and usually
return more results than expected.

Configuration
-------------

Use the class ``OHCacheBuilder`` to configure all necessary parameter like

- hash table size (must be a power of 2)
- data block size (must be a power of 2)
- total capacity

The total amount of required off heap memory is the *total capacity* plus *hash table*. Each hash partition (currently)
requires 16 bytes - so the formula is ``total_capacity + hash_table_size * 16``.

Configure your data block size to somewhat that a single hash entry needs approximately 1 or 2 data blocks.
The first data block for a hash entry has an overhead of 64 bytes - each other data block has an overhead
of 8 bytes. Means the first block has ``block_size - 64`` bytes available for key+value and each other
``block_size - 8`` bytes.
Why off heap memory
-------------------

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
you do not put pressure on the Java garbage collector. Let the Java GC do its job for the application where
this library does its job for the cached data.

Concurrency
-----------

Caches must be designed for a good performance and good concurrency management where many threads perform
possibly concurrent read and write operations.

License
-------

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
