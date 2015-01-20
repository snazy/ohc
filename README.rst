OHC - A off-heap-cache
======================

Status
------

This library should be considered as **nearly** *production ready*.

Performance
-----------

OHC shall provide a good performance on both commodity hardware and big systems using non-uniform-memory-architectures.

No performance test results available yet - you may try the ohc-benchmark tool. See instructions below.
A very basic impression on the speed is in the _Benchmarking_ section.

Requirements
------------

Java7 VM that support 64bit and has ``sun.misc.Unsafe`` (Oracle JVMs on x64 Intel CPUs).

An extension jar that makes use of new ``sun.misc.Unsafe`` methods in Java 8 exists.

Architecture
------------

OHC uses multiple segments. Each segment contains its own independent off-heap hash map. Synchronization occurs
on critical sections that access a off-heap hash map. Necessary serialization and deserialization is performed
outside of these critical sections.
Eviction is performed using LRU strategy when adding entries.
Rehashing is performed in each individual off-heap map when necessary.

Configuration
-------------

Use the class ``OHCacheBuilder`` to configure all necessary parameter like

- number of segments (must be a power of 2), defaults to number-of-cores * 2
- hash table size (must be a power of 2), defaults to 8192
- load factor, defaults to .75
- capacity for data over the whole cache
- key and value serializers

Generally you should work with a large hash table. The larger the hash table, the shorter the linked-list in each
hash partition - that means less linked-link walks and increased performance.

The total amount of required off heap memory is the *total capacity* plus *hash table*. Each hash bucket (currently)
requires 8 bytes - so the formula is ``capacity + segment_count * hash_table_size * 8``.

Usage
-----

Quickstart::

 OHCache ohCache = OHCacheBuilder.newBuilder()
                                 .keySerializer(yourKeySerializer)
                                 .valueSerializer(yourValueSerializer)
                                 .build();

This quickstart uses the very least default configuration:

- of 64MB capacity,
- number of segments is 2 * number of cores
- 8192 buckets per segment
- load factor of .75
- your custom key serializer
- your custom value serializer
- no maximum serialized cache entry size

Key and value serializers need to implement the ``CacheSerializer`` interface. This interface has three methods:

- ``int serializedSize(T t)`` to return the serialized size of the given object
- ``void serialize(Object obj, DataOutput out)`` to serialize the given object to the data output
- ``T deserialize(DataInput in)`` to deserialize an object from the data input

Building from source
--------------------

Clone the git repo to your local machine. Either use the stable master branch or a release tag.

``git clone https://github.com/snazy/ohc.git``

You need Oracle JDK8 to build the source (Oracle JRE7 is the minimum requirement during runtime).
Just execute

``mvn clean install``

Benchmarking
------------

You need to build OHC from source because the big benchmark artifacts are not uploaded to Maven Central.

Execute ``java -jar ohc-benchmark/target/ohc-benchmark-0.3-SNAPSHOT.jar -h`` to get some help information.

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
 -type <arg>   implementation type - default: linked - option: tables
 -vs <arg>     value sizes - default: fixed(512)
 -wkd <arg>    hot key use distribution - default: uniform(1..10000)
 -wu <arg>     warm up - <work-secs>,<sleep-secs>
 -z <arg>      hash table size
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

 java -jar ohc-benchmark/target/ohc-benchmark-0.3-SNAPSHOT.jar \
   -rkd 'gaussian(1..20000000,2)' \
   -wkd 'gaussian(1..20000000,2)' \
   -vs 'gaussian(1024..32768,2)' \
   -r .9 \
   -cap 1600000000 \
   -d 30 \
   -t 16

(Note that the version in the jar file name might differ.)

On a 2.6GHz Core i7 system (OSX) the following numbers are typical running the above benchmark (.9 read/write ratio):

- # of gets per second: 2500000
- # of puts per second:  270000

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

History
-------

OHC was developed in 2014/15 for Apache Cassandra 3.0 to be used as the new row-cache backend for
https://issues.apache.org/jira/browse/CASSANDRA-7438 .
Since there were no suitable fully off-heap cache implementations available, it has been decided to
build a completely new one - and that's OHC. But it turned out that OHC alone might also be usable for
other projects that's why OHC is a separate library.

Contributors
------------

A big 'thank you' has to go to `Benedict Elliott Smith <https://twitter.com/_belliottsmith>`_ and
`Ariel Weisberg <https://twitter.com/ArielWeisberg>`_ from DataStax for their very useful input to OHC!

Developer: `Robert Stupp <https://twitter.com/snazy>`_

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
