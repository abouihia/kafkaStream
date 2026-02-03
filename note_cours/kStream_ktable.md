A- KStream 
is Kafka Streams’ abstraction for an unbounded stream of events, 
represented as key–value records, where each record is an independent event and never “updates” or replaces another one.


Core characteristics
Record stream of events: A KStream models things like trades, clicks, log lines,
or sensor readings; every event is an append-only record in time.


Source and result type: A KStream is typically created from one or more Kafka topics, 
or from transforming other KStreams/KTables, and can be further transformed, joined, or aggregated.


How it is used
Stateless transforms: You apply operations like map, filter, flatMap, etc.,
processing records one by one without storing past state.


Gateway to stateful ops: A KStream can be grouped and aggregated into a KTable (for counts, sums, etc.),
or joined with other KStreams/KTables, forming the basis of most real-time processing topologies.




B- Ktable 

Is an abstraction in kafka Stream which holds latest value for a given key.

What fundamental concept is at play when a KTable keeps only the most recent value for a key,
effectively discarding older values for the same key?

A KTable maintains a materialized view of the latest value for each key, 
effectively acting as a changelog stream where older values for a key are superseded by newer ones.

difference filtering : Ktable, Kstream:
Filtering a KTable means filtering a stateful, materialized view (the latest value per key), 
Filtering a KStream filters a stateless event stream (every event, including historical ones).

how to create Ktable:
  streamBuilder.table(topics, Consumer.with(Serdes.String(), SerdesFactory.revenueSerde()), 
   Materialized.as('words-store'))

A materialized view is a stored, continuously updated result of a query, kept as a physical table (or state store) so it can be read very quickly.

Materialized.as('words-store') // create a stateStore  (to store the data in additional entity) in case if app crash or restore or deployment

the default state store is RocksDB ( which is a high perf embedded database for( key/value) data)


when to use KTable:
any  business usecase that requires the streaming app to maintain the latest value for key


How it's work:
when does the K table decides to with the records to these operators.

we have two configuration that hold this:

1- cache.max.bytes.buffering:  Ktable use a cache internally for deduplication and the cache service a buffer(default size for buffer =10MB).
                               this buffer basically holds the previous value with the same key.
                                And anytime we have a new value with the same key,then it gets overriden.
But if the buffer size is greater than the value mentioned in this property, then the data will be emitted to the downstream nodes immediately


2: commit.interval.ms:(30 secondes) : So this is one of the other property which is responsible for sending 
                  the data from the key table to the downstream nodes once the  commit interval is exhausted (after waiting 30 secondes)


Whenever you use ktable with the materialized call, this is going to create an internal changelog topic. So 
this is a topic which is mainly used for fault tolerance in the case of application restart.
This is how it gets the latest value for the given key. 


the rolecache.max.bytes.buffering a bit more. It controls the maximum size of the internal cache that a KTable uses to store recent updates for keys. When new records arrive, they are first written to this cache. The cache serves two main purposes:
1.  Deduplication: If multiple updates for the same key arrive within a short period, the cache can ensure that only the latest of these updates is eventually processed and emitted downstream, reducing redundant writes.
2.  Buffering: It holds these updates until either the cache reaches its cache.max.bytes.buffering limit or the commit.interval.ms time has elapsed.

So, to put it together: the KTable buffers updates in its internal cache (up to cache.max.bytes.buffering size). These buffered updates are then emitted downstream (and to the changelog topic) when either the cache is full or the commit.interval.ms timer expires, whichever comes first.



If cache.max.bytes.buffering is set too high, the application can suffer from excessive heap usage and memory pressure, potentially leading to more frequent GC pauses or even OutOfMemoryError, because a large volume of records is held in the in‑memory record cache before being flushed to RocksDB and downstream.
​

If commit.interval.ms is too long, updates sit in the cache and are not flushed to the state store and changelog topic for a long time, so downstream consumers or applications that read from the changelog / materialized KTable will see stale data and higher end‑to‑end latency before new state becomes visible


changelog topic: It's the durable, ordered log of all changes to a KTable's state, and its role in enabling fault tolerance and state recovery is absolutely critical.

Ktable vs GlobalKTable:
Imagine  you have 2 instances for the same application sharing the application.id   instance1 have two task (t1, t2) , Instance1 have task(t3, t4)
Ktable : key will  be distributed between theses  2 instances( I1 , I2) , I1 have access to only the key that are tied to the task T1 and T1 (T for task).
GlobalKTable : will access to all the key for all tasks in each instance.

when to use  each one:

Ktable : if you have large keyspace (million of keys), is better for join where time need to be synchronized.