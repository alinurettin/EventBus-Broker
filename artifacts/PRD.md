# 📋 Product Requirements Document (PRD): EventBus-Broker
**Version:** 2.0.0  
**Owner:** Ali Nurettin Demir  
**Product Manager:** 7-Agent SDLC Product Management Lead  

---

## 1. Product Vision
EventBus-Broker is an ultra-fast, zero-dependency partitioned event streaming broker and pub/sub coordinator. Designed for high-throughput asynchronous microservices, it provides durable append-only commit logs, consumer group offset commits, real-time lag tracking, and topic compaction with sub-millisecond execution.

---

## 2. Target Personas
1. **Event-Driven Backend Engineers:** Need an append-only commit log with partition key hashing for sequential processing without setting up full Kafka clusters in development or testing.
2. **Data & Analytics Engineers:** Need consumer group offset tracking and real-time lag monitoring to verify pipeline processing velocity.
3. **IoT & Telemetry Engineers:** Require MQTT/AMQP-style hierarchical wildcard topic routing (`sensors.*.temperature`).

---

## 3. Core Functional Requirements

| ID | Requirement | Acceptance Criteria |
|---|---|---|
| **FR-01** | **Multi-Partition Append Log** | Append messages to partitioned commit logs with monotonic 64-bit integer offsets and high watermark tracking. |
| **FR-02** | **Deterministic Key Hashing** | Route messages with identical keys to the same partition using 32-bit hashing, guaranteeing per-key FIFO ordering. |
| **FR-03** | **Consumer Group Offset Protocol** | Commit and track progress per `(group, topic, partition)` with zero-overhead offset fetch operations. |
| **FR-04** | **Real-Time Consumer Lag Metric** | Compute total and per-partition consumer group lag ($\text{HighWatermark} - \text{CommittedOffset}$). |
| **FR-05** | **Hierarchical Topic Filtering** | Support single-level (`*`) and multi-level (`#`) wildcard topic pattern subscription queries. |
| **FR-06** | **In-Memory Log Compaction** | Compact changelog topics by retaining only the latest record per message key. |
| **FR-07** | **Interactive Dark-Mode Dashboard** | Web console featuring a message publisher sandbox, consumer lag progress bars, and log browser. |
| **FR-08** | **SSE Live Synchronization Stream** | Broadcast message published events over `GET /api/events/stream`. |

---

## 4. Technical Constraints
- **Zero External Dependencies:** Built strictly on Node.js standard libraries (`node:http`, `node:crypto`, `node:fs`, `node:path`).
- **Port:** Configurable via `PORT` environment variable (defaults to `6000`).
