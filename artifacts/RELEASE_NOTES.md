# 🚀 Release Notes: EventBus-Broker v2.0.0
**Release Date:** 2026-09-20  
**Git Tag:** `v2.0.0`  
**Author:** Ali Nurettin Demir & 7-Agent SDLC Autonomous Factory  

---

## 🌟 Major Highlights

### 1. Horizontally Partitioned Append-Only Commit Logs
- Implemented log-structured storage model with monotonic 64-bit offsets and high watermark tracking per partition.

### 2. Deterministic Partition Key Hashing
- Guaranteed sequential FIFO processing per business entity key using 32-bit hash partitioning.

### 3. Consumer Group Protocol & Offset Lag Tracking
- Tracks committed offsets per `(group, topic, partition)` and computes real-time processing lag.

### 4. Topic Log Compaction
- Automatically compacts state/changelog topics by discarding superseded intermediate states and preserving latest values per key.

### 5. Hierarchical AMQP/MQTT Wildcard Matching
- Supports single-level (`*`) and multi-level (`#`) topic pattern queries.

### 6. Interactive Dark-Mode Control Console
- Real-time message publisher studio, consumer group lag progress bars, and partition message stream browser.

### 7. Automated Verification Suite
- 21 passing non-mocked automated unit and HTTP integration assertions.
