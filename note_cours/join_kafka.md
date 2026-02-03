Join in Kafka
Is base on same key  in difference on merge .

inner join:(like join in RDB)
 join two same key from differente topics.

recap :
KStream:
KTable:
GlobalKTable:

we have differentes types of join in kafka:
 A-KStream - kTable:


B-KStream - GlobalKTable:


C-kTable - kTable:


D-KStream - KStream:
A Kstream is an infinite stream which representes a log of everything that happened.
It's expected  they both share the same key and also it should be within a certain time window

Explain
  with the same kye but time window
 stream 1: assume  the time 5 PM   if an event receive before and after 5 p.m  ex : 4.50 PM
 stream 2: assumre the time 00 PM   if an event receive before and after 00 p.m ex  59 minutes and 56 minutes

 
D-1 innerJoin:
D-2-left-join:
  Join is triggered when a record on the left side of the join is received
  if there is no matching record on the right side, then the join will be triggerred with null value for the right side valuee
D-3-outJoin:
  Join will be triggered if there is a record on either( chaque) side of the join
  so when a record is recieved on either side of the join, and if there is no matching  on other side  ==> the join will populate the null value
to the combined result

join under the hood:
 for any joins case the kafka create an internal  topics which kafka uses behind the scenes.
  same thing like this:
 joins1-KSTREAM-JOINOTHER-0000000007-store-changelog
   so this random value  KSTREAM-JOINOTHER-0000000007  can be change

who :
var joinedParams  = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
.withName("Jointure_gauche")   // here we give the name
.withStoreName("Jointure_gauche"); // here we give the name 

co-partition or prerequisites in Joins:

the source topics used for joins should have the same number of partitions:
    -Each partition is assigned to a task in kafka streams.
    -This guarantees that the related tasks are together and join will work as expected ( we can use selectKey or map operator to meet these requirements)

 If the partitions don't align, Kafka Streams wouldn't be able to guarantee that records with the same key 
from different topics end up in the same processing task, making the join impossible or incorrect.

Because if you all can remember, we learned that each partition gets assign 
the task in kafka streams by having the same number of partitions guarantees that the related tasks are together in the same stream
task and the joints will work as expected.
In some cases, this might not be possible for those kind of scenarios. 





