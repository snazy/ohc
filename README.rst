OHC - A off-heap-cache
======================

Status
------

This library should be considered as *experimental* and **not production ready**.

Performance
-----------

(No performance test results available yet - you may try the ohc-benchmark tool)

OHC shall provide a good performance on both commodity hardware and big systems using non-uniform-memory-architectures.

Requirements
------------

Java7 VM that support 64bit field CAS and ``sun.misc.Unsafe`` (Oracle JVMs on x64 Intel CPUs).

Architecture
------------

OHC uses multiple segments. Each segment contains its own independent off-heap hash map. Synchronization occurs
on critical sections that access a off-heap hash map. Necessary serialization and deserialization is performed
outside of these critical sections.

Concurrent eviction is performed using either LRU (double linked list), timestamp or counter (2Q).
Note that the last two replacement strategies are not yet implemented.

Configuration
-------------

Use the class ``OHCacheBuilder`` to configure all necessary parameter like

- number of segments (must be a power of 2), defaults to number-of-cores * 2
- hash table size (must be a power of 2), defaults to 8192
- load factory, defaults to .75
- capacity for data over the whole cache
- eviction configuration (cleanup-trigger-free-percentage triggers a cleanup, cleanup-target-free-percentage defines the target free capacity)
- key and value serializers

Generally you should work with a large hash table. The larger the hash table, the shorter the linked-list in each
hash partition - that means less linked-link walks and increased performance.

The total amount of required off heap memory is the *total capacity* plus *hash table*. Each hash bucket (currently)
requires 8 bytes - so the formula is ``capacity + segment_count * hash_table_size * 8``.

Why off-heap memory
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
that dos not put any pressure on the Java garbage collector. Let the Java GC do its job for the application where
this library does its job for the cached data.

Alternate implementation "mono"
-------------------------------

OHC uses a simple hash table as its primary data structure. Each hash partition basically consists of a pointer
to one hash entry - each hash entry has a pointer to its successor and predecessor
building a linked-list.

The hash table will be automatically resized if the number of linked-list-iterations is too high (configurable).

CAS based stamped locks are used on each hash partition and each hash entry. Hash entry locks are
required because OHC tries to release each lock on a whole hash partition as soon as possible.

Put operations do not succeed if there is not enough free space to serialize the data. Reason is that OHC will
not block any operation longer than really necessary. This should be completely fine for caches since it is better
to let a cache-put not succeed than to block the calling application. If there's demand for a *put guarantee*
it can be implemented (as long as there's enough free capacity).

OHC provides a pure LRU based eviction mechanism. But be aware that calculation of entries to evict is based on averages
and does its job not very accurately to increase overall performance - it's a trade-off between performance
and accuracy.

Note on the ``hotN`` function: The implementation will take N divided by number of hash partitions keys and usually
return much more results than expected (the results are not ordered - the results just represent some least
recently accessed entries).

Memory allocation for hash entries is either performed using JEMalloc (preferred, if available) or ``Unsafe``.

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
