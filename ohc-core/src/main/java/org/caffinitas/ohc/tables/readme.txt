Proposal for better OHC implementation:

- same API as 'linked' implementation
- but use tables instead of links

'linked' implementation:

OHCacheImpl is non-synchronized and contains several segments implemented by OffHeapMap, which has synchronized methods.
OffHeapMap contains a simple table containing one pointer to the first hash entry of a bucket - this keeps the
table pretty dense.
Each hash entry lives in its own memory region and consists of the header, the key and the value.
Key and value are just the serialized representations.
Header consists of 7 long values:
- pointer to next entry in LRU list (over whole OffHeapMap)
- pointer to previous entry in LRU list (over whole OffHeapMap)
- pointer to next entry in bucket
- reference counter (initially 1, incremented on each 'live' access, decremented when no longer used, if decremented to 0, memory can be freed)
- 64 bit hash
- value length
- key length
Additionally each OffHeapMap contains two longs to the head and the tail of the LRU list.

Problems occur especially on NUMA machines insisting in high to very high system CPU usage. It is not caused
by synchronization or locks (which can achieve rates of 1M per second and much more). It is caused by letting multiple
CPUs (in multiple sockets) access the structure.

Each read has to:
- Access the OffHeapMap table
- Access the first hash entry (in the bucket)
- Compare the hash and key length against the key to match
- Compare the key value (if hash and key length match)
- Traverse via 'pointer to next entry' to the next entry in the bucket (and restart with hash + key length comparison)
- Look up LRU prev and LRU next of its own and its LRU predecessor and antecessor
- Modify LRU prev, LRU next of its own and its LRU predecessor and antecessor (or LRU head and LRU tail in OffHeapMap)

Each write has to:
- Access the OffHeapMap table
- Store the first hash entry in its 'next entry' pointer
- Modify the OffHeapMap table
- Set the right LRU next value in its header to OffHeapMap's LRU head
- Eventually set OffHeapMap's LRU tail (if it's the first entry in the table)
- Modify the LRU prev value of the previous LRU head

Both reads and writes have one bad characteristic: they have to access memory that might be gigabytes away from the
last accessed value. This is a bad behaviour since many distinct regions in whole RAM need to be read and written.

'table' implementation:

Assumption: Stress tests showed that each hash bucket has at most 8 entries (maybe nine in very rare cases).

1st change: Blow up per-OffHeapMap table.
The table shall include pointers to _all_ entries of the whole OffHeapMap - i.e.
eliminate the 'pointer to next entry' in 'linked' implementation.
Size estimation on 32 core system:
- 'linked' implementation : 4MB (= 8192 * 8 * 64 with default configuration)
- 'table' implementation  : 32MB (= 8192 * 8 * 8 * 64 with default configuration)

2nd change: Condense LRU information.
Per-OffHeapMap LRU information shall be in multiple smaller tables.
OffHeapMap gets a two-dimensional long-array ('long[][]'). Each single array consists of 504 entries
(= exactly 4kB in Java heap) or 512 entries (= exactly 4kB in off-heap).
The whole two-dimensional long-array is sized to be able to contain 'hash table size * load factor' (at least).
One long-array is the current-write-target receiving pointers all accessed hash entries.
Upon reads it has to locate the address of the hash entry in the two-dimensional long-array,
remove it from the long-array and add it to the current-write-target.
Upon removes it has to locate the address of the hash entry in the two-dimensional long-array and
remove it from the long-array.
Upon puts it has to add it to the current-write-target.
If the current-write-target is full, an empty one is assigned the current-write-target.
If one long-array becomes empty, its has to be recycled for reuse.
If there are no more long-arrays available to become the the current-write-target, the whole structure needs
to be compacted.
