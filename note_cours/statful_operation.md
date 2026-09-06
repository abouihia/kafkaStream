# Stateful Operations

## Definition
A **stateful operation** is:

> Any operation that must remember past events in order to process the current event.

The operation keeps state across records, typically in a local state store (e.g., RocksDB).

Because state is maintained, Kafka Streams can perform:
- Rolling counts
- Sums
- Moving averages
- Joins between streams/tables

➡️ This goes beyond simple stateless operations like filters or maps.

---

## Fault Tolerance

The state is backed by **changelog topics**, which allows it to be:
- Restored after crashes
- Recovered after rebalances

This enables **fault-tolerant, stateful stream processing**, rather than one-off stateless transformations.

---

## Types of Stateful Operations

### 1. Aggregation
Calculating metrics such as:

> Total number of orders in a retail company

**Implementations:**
- `count`
- `reduce`
- `aggregate`

⚠️ Requirement:
- The key **must not be null**

---

### 2. Joining Events
Combining data from two independent topics based on a key.

**Implementations:**
- `join`
- `leftJoin`
- `outerJoin`

---

### 3. Windowing
Grouping data within a specific time window.

**Implementation:**
- `windowedBy`

---

## Materialized Views

Materialized views act as **persistent storage for aggregated state**.

This means:
- `COUNT` and `REDUCE` operations can always access the current value
- State survives application restarts
- New events are continuously incorporated

✅ Benefits:
- Accuracy
- Fault tolerance
- Real-time consistency  