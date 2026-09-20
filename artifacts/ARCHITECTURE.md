# 🏛️ Technical Architecture Document: EventBus-Broker
**Version:** 2.0.0  
**Domain:** Distributed Event Streaming & Log-Structured Pub/Sub  
**Architect:** 7-Agent SDLC Principal Software Architect  

---

## 1. System Topology & Architecture

```mermaid
flowchart TD
    subgraph Producers [Producers & Control Plane]
        PubApp["🚀 Application Services (Producers)"]
        WebUI["🖥️ Dark-Mode Dashboard (Port 6000)"]
    end

    subgraph BrokerProcess [EventBus-Broker Engine]
        Dispatcher["⚡ HTTP Route Dispatcher & Parser"]
        Coordinator["🧠 EventBusBroker Coordinator"]
        
        subgraph TopicRegistry [Partitioned Topics]
            Topic1["📦 Topic: orders.transactions (3 Partitions)"]
            Topic2["📦 Topic: telemetry.metrics (4 Partitions)"]
            TopicDLQ["⚠️ Topic: system.dlq (1 Partition)"]
        end

        subgraph StorageLayer [Partition Commit Logs]
            P0["📜 Partition 0 [0..N] (Append-Only)"]
            P1["📜 Partition 1 [0..M] (Append-Only)"]
            P2["📜 Partition 2 [0..K] (Append-Only)"]
        end

        subgraph GroupCoordinator [Consumer Group Coordinator]
            Offsets["💾 Committed Offset Table (group, topic, partition)"]
            LagCalc["📊 Real-Time Lag Calculation Engine"]
        end

        SSE["📡 SSE Live Broadcast Gateway"]
    end

    subgraph Consumers [Consumer Groups & Workers]
        BillingWorker["💳 Billing Consumer Group"]
        AnalyticsWorker["📊 Analytics Consumer Group"]
    end

    PubApp -->|POST /api/publish| Dispatcher
    WebUI --> Dispatcher
    Dispatcher --> Coordinator
    Coordinator --> Topic1 & Topic2 & TopicDLQ
    Topic1 --> P0 & P1 & P2
    BillingWorker -->|GET /api/fetch| Dispatcher
    BillingWorker -->|POST /api/groups/commit| Dispatcher
    AnalyticsWorker -->|GET /api/groups/lag| Dispatcher
    Dispatcher --> GroupCoordinator
    GroupCoordinator --> Offsets & LagCalc
    Coordinator -->|Message Events| SSE
    SSE -->|text/event-stream| WebUI
```

---

## 2. Core Architectural Subsystems

### 2.1 `PartitionLog` (`src/engine.js`)
- **Append-Only Array:** Fast $O(1)$ memory appends assigning sequential `offset` indices.
- **High Watermark:** Exposes upper write boundary for consumer fetch verification.
- **Compaction:** Scans backwards to build map of latest keys, discarding historical superseded transitions.

### 2.2 `Topic` (`src/engine.js`)
- Manages $N$ independent `PartitionLog` instances.
- Computes deterministic partition assignment: `hash32(key) % partitionCount`.
- Unkeyed messages cycle through round-robin counters for balanced distribution.

### 2.3 `ConsumerGroup` (`src/engine.js`)
- Stores committed offsets keyed by `topic#partitionId`.
- Computes real-time lag per partition: $\text{HighWatermark} - \text{CommittedOffset}$.

### 2.4 `TopicMatcher` (`src/engine.js`)
- Evaluates AMQP/MQTT style hierarchical topic tokens (`*` and `#`).
- Allows topic pattern querying without regular expression backtracking vulnerabilities.
