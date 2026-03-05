
The main difference between a KTable and a GlobalKTable is that a KTable shards data between Kafka Streams instances, 
while a GlobalKTable extends a full copy of the data to each instance. You typically use a GlobalKTable with lookup data. 
There are also some idiosyncrasies regarding joins between a GlobalKTable and a KStream; we’ll cover these later in the course.

The main difference between a KTable and a GlobalKTable is that a KTable shards data between Kafka Streams instances, 
while a GlobalKTable extends a full copy of the data to each instance. You typically use a GlobalKTable with lookup data. 
There are also some idiosyncrasies regarding joins between a GlobalKTable and a KStream; we’ll cover these later in the course.



# Joins
 # Stream-stream joins 
  combine two event streams into a new stream. The streams are joined based on a common key, so keys are necessary. 
You define a time window, and records on either side of the join need to arrive within the defined window. 
Kafka Streams uses a state store under the hood to buffer records, so that when a record arrives,
it can look in the other stream's state store and find records by key that fit into the time window, based on timestamps.
 
# Stream-Table Joins
non windowed.
You can join a KStream with a KTable and a KStream with a GlobalKTable.
the stream is always on the primary side

Inner  :=====>  join only fires if both sides are available.
Outer-Both  :====>  sides always produce an output record
            left-value + right-value
            left-value + null
            Null + Right-value

Left-Outer  :====>  the left side always produces an output record
            left-value + right-value
            left-value + null


GlobalKTable-Stream Join properties:

GlobalKTable: you get the full replication of the underlying topics across all instance

GlobalKTable provides a mechanism whereby, when you perform a join with a KStream, 
the key of the stream doesn't have to match. You get a KeyValueMapper when you define the join,
and you can derive the key to match the GlobalKTable key using the stream key and/or value.

# Table Table  joins