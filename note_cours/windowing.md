# Windowing
Windowing allows you to bucket stateful operations by time, without which your aggregations would endlessly accumulate. 
A window gives you a snapshot of an aggregate within a given timeframe, and can be set as hopping, tumbling, session, or sliding.

# Hopping window
is bound by time: You define the size of the window, but then it advances in increments smaller than the window size,
so you end up with overlapping windows. 
You might have a window size of 30 seconds with an advance size of five seconds. Data points can belong to more than one window.

``` java
    KStream<String, String> myStream = builder.stream("topic-A");
    Duration windowSize = Duration.ofMinutes(5);
    Duration advanceSize = Duration.ofMinutes(1);
    TimeWindows hoppingWindow = TimeWindows.of(windowSize).advanceBy(advanceSize);
    myStream.groupByKey()
    .windowedBy(hoppingWindow)
    .count()
``` 
# Thumbling windows:

It's a hopping window with an advance size that's the same as its window size. 
So basically you just define a window size of 30 seconds. When 30 seconds are up, you get a new window with a time of 30 seconds.
So you don't get duplicate results like you do with the overlapping in hopping windows.

``` java
    KStream<String, String> myStream = builder.stream("topic-A");
    Duration windowSize = Duration.ofSeconds(30);
    TimeWindows tumblingWindow = TimeWindows.of(windowSize);
    
    myStream.groupByKey()
    .windowedBy(tumblingWindow)
    .count();

```

# Session window

Session windows are different from the previous two types because they aren't defined by time, but rather by user activity:
So with session windows, you define an inactivityGap. Basically, as long as a record comes in within the inactivityGap,
your session keeps growing. 
So theoretically, if you're keeping track of something and it's a very active key, your session will continue to grow.

``` java
    KStream<String, String> myStream = builder.stream("topic-A");
    Duration inactivityGap = Duration.ofMinutes(5);
    
    myStream.groupByKey()
        .windowedBy(SessionWindows.with(inactivityGap))
        .count();
```


# Sliding window:

A sliding window is bound by time, but its endpoints are determined by user activity. 
So you create your stream and set a maximum time difference between two records that will allow them to be included in the first window.

``` java
    KStream<String, String> myStream = builder.stream("topic-A");
    Duration timeDifference = Duration.ofSeconds(2);
    Duration gracePeriod = Duration.ofMillis(500);
    myStream.groupByKey()
    .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(timeDifference, gracePeriod))
    .count();
```