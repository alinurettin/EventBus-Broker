# 🔬 Research Report: Log-Structured Event Streaming & Consumer Group Coordination
**Project:** EventBus-Broker v2.0.0  
**Domain:** Distributed Streaming Systems, Log-Structured Storage & Pub/Sub Brokers  
**Author:** 7-Agent SDLC Autonomous Research Engineer  

---

## 1. Executive Summary & Problem Space
Modern asynchronous event-driven architectures require high-throughput, horizontally partitioned message brokers to decouple producer workloads from consumer pipelines.

Traditional in-memory pub/sub brokers (e.g. Redis Pub/Sub, basic AMQP brokers) maintain ephemeral queues where unread messages are lost upon consumer disconnects. Conversely, heavyweight distributed streaming platforms like Apache Kafka or Redpanda require multi-gigabyte JVM runtimes, complex ZooKeeper/KRaft quorum coordination, and significant infrastructure provisioning.

**EventBus-Broker** formulates a high-speed, zero-dependency log-structured streaming broker in pure Node.js featuring:
1. **Append-Only Commit Logs** with monotonically increasing 64-bit integer offsets per partition.
2. **CRC-32 Deterministic Partition Key Hashing** guaranteeing FIFO sequencing per entity key.
3. **Consumer Group Protocol** with committed offset tracking and real-time lag metrics.
4. **Log Compaction** retaining only the latest state record per key.
5. **Hierarchical Topic Wildcards** (`*` single-level and `#` multi-level matching).

---

## 2. Mathematical & Storage Formulations

### 2.1 Deterministic Key Partitioning
To guarantee strict FIFO message ordering for a given business entity (e.g., all transactions for `customer_123` must execute sequentially in the exact order generated):

$$\text{hash}_{32}(\text{Key}) = \text{MD5}(\text{Key})[0..3]_{32}$$
$$\text{PartitionID} = \text{hash}_{32}(\text{Key}) \pmod N$$

Where $N$ is the topic partition count. Unkeyed messages fall back to uniform round-robin distribution.

### 2.2 Consumer Group Lag Calculation
For consumer group $G$ subscribed to topic $T$ across $N$ partitions:
$$\text{Lag}(G, T, p) = \max(0, \text{HighWatermark}(T, p) - \text{CommittedOffset}(G, T, p))$$
$$\text{TotalLag}(G, T) = \sum_{p=0}^{N-1} \text{Lag}(G, T, p)$$

Where:
- $\text{HighWatermark}(T, p)$ is the next write offset in partition $p$.
- $\text{CommittedOffset}(G, T, p)$ is the latest acknowledged offset processed by group $G$.

### 2.3 Log Compaction Heuristic
For changelog and state topics, log compaction reduces storage from $O(M)$ total historical messages to $O(U)$ unique entity keys:
$$\mathcal{L}_{\text{compacted}} = \{ m_k \mid m_k = \arg\max_{m.key = k} m.\text{offset} \}$$
Preserving unkeyed messages while eliminating superseded intermediate state transitions.

---

## 3. Benchmark Targets
- Ingestion latency: $< 15\mu\text{s}$ per message append.
- Partition fetch throughput: $> 60,000\text{ msgs/sec}$.
- Zero external runtime dependencies (pure Node.js standard libraries).
