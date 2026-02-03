StatFul operation:

def:
 Any operation that must remember past event in order to process the current event.
The operation keeps state across records, typically in local state store (RocksDB)

Because state is maintained, Kafka Streams can do things like rolling counts, sums, 
moving averages, and joins between streams/tables, not just simple filters or maps.

That state is backed by changelog topics, so it can be restored after crashes or rebalances,
giving fault‑tolerant, stateful stream processing rather than one‑off stateless transformations.


1-aggregation:  calculating total number of orders made in a retail company  -> implement  : count, reduce aggregate
  to work this aggregation, the key must be not null



2-joining Event: combining data form two independent topics based on key  -> implement : join, leftjoin, outerjoin
3- Windowing: grouping data in a certain time window  -> implement : windowedBy



Materialized views act like a persistent storage for the aggregated state.
This means that COUNT and REDUCE operations can always access the current aggregated value, even if the application restarts or new events come in. 
This is crucial for maintaining accuracy and fault tolerance in streaming applications.
