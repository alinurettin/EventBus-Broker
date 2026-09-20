// EventBus-Broker v2.0.0 - Distributed Partitioned Pub/Sub Event Streaming Broker
// Log-Structured Commit Logs, Consumer Group Offsets, CRC32 Partitioning & Wildcard Filtering

const crypto = require('crypto');

/**
 * Deterministic 32-bit CRC-like hash for partition distribution
 */
function hashKey(key) {
  if (!key) return 0;
  const hash = crypto.createHash('md5').update(String(key)).digest();
  return hash.readUInt32BE(0);
}

/**
 * Append-Only Commit Log Partition
 */
class PartitionLog {
  constructor(partitionId, topicName) {
    this.partitionId = partitionId;
    this.topicName = topicName;
    this.messages = []; // Array of message frames
    this.nextOffset = 0;
  }

  append(key, value, headers = {}) {
    const offset = this.nextOffset++;
    const frame = {
      offset,
      key: key !== null && key !== undefined ? String(key) : null,
      value,
      timestamp: Date.now(),
      headers
    };
    this.messages.push(frame);
    return frame;
  }

  fetch(fromOffset = 0, limit = 50) {
    if (fromOffset >= this.messages.length) return [];
    const slice = this.messages.slice(fromOffset, fromOffset + limit);
    return slice;
  }

  getHighWatermark() {
    return this.nextOffset;
  }

  compact() {
    const latestByKey = new Map();
    for (const msg of this.messages) {
      if (msg.key) {
        latestByKey.set(msg.key, msg);
      }
    }
    // Keep unkeyed messages + latest keyed messages, sorted by offset
    const unkeyed = this.messages.filter(m => !m.key);
    const compacted = [...unkeyed, ...Array.from(latestByKey.values())];
    compacted.sort((a, b) => a.offset - b.offset);
    this.messages = compacted;
    return this.messages.length;
  }
}

/**
 * Multi-Partition Topic
 */
class Topic {
  constructor(name, partitionCount = 3) {
    this.name = name;
    this.partitionCount = partitionCount;
    this.partitions = [];
    this.rrCounter = 0;

    for (let i = 0; i < partitionCount; i++) {
      this.partitions.push(new PartitionLog(i, name));
    }
  }

  publish(key, value, headers = {}) {
    let targetPartition = 0;
    if (key !== null && key !== undefined) {
      targetPartition = hashKey(key) % this.partitionCount;
    } else {
      targetPartition = this.rrCounter % this.partitionCount;
      this.rrCounter++;
    }

    const partition = this.partitions[targetPartition];
    const frame = partition.append(key, value, headers);
    return {
      topic: this.name,
      partitionId: targetPartition,
      offset: frame.offset,
      timestamp: frame.timestamp,
      message: frame
    };
  }

  fetch(partitionId, fromOffset = 0, limit = 50) {
    if (partitionId < 0 || partitionId >= this.partitionCount) {
      throw new Error(`Invalid partition ID ${partitionId} for topic ${this.name}`);
    }
    return this.partitions[partitionId].fetch(fromOffset, limit);
  }

  getTotalMessages() {
    return this.partitions.reduce((sum, p) => sum + p.messages.length, 0);
  }

  getPartitionSummaries() {
    return this.partitions.map(p => ({
      partitionId: p.partitionId,
      highWatermark: p.getHighWatermark(),
      messageCount: p.messages.length
    }));
  }
}

/**
 * Consumer Group tracking committed offsets and calculating lag
 */
class ConsumerGroup {
  constructor(groupId) {
    this.groupId = groupId;
    this.committedOffsets = new Map(); // "topic#partitionId" -> offset
    this.consumers = new Set(); // active consumer client IDs
    this.lastActive = Date.now();
  }

  commit(topic, partitionId, offset) {
    const key = `${topic}#${partitionId}`;
    this.committedOffsets.set(key, offset);
    this.lastActive = Date.now();
  }

  getCommittedOffset(topic, partitionId) {
    const key = `${topic}#${partitionId}`;
    return this.committedOffsets.get(key) || 0;
  }

  calculateLag(topicObj) {
    let totalLag = 0;
    const partitionLag = [];

    for (const p of topicObj.partitions) {
      const committed = this.getCommittedOffset(topicObj.name, p.partitionId);
      const highWatermark = p.getHighWatermark();
      const lag = Math.max(0, highWatermark - committed);
      totalLag += lag;
      partitionLag.push({
        partitionId: p.partitionId,
        committedOffset: committed,
        highWatermark,
        lag
      });
    }

    return {
      groupId: this.groupId,
      topic: topicObj.name,
      totalLag,
      partitions: partitionLag
    };
  }
}

/**
 * Hierarchical Wildcard Topic Matcher
 * Supports AMQP / MQTT wildcards:
 * '*' matches exactly one segment
 * '#' matches zero or more segments
 */
class TopicMatcher {
  static matches(pattern, topic) {
    if (pattern === topic || pattern === '#') return true;

    const patParts = pattern.split('.');
    const topParts = topic.split('.');

    let p = 0;
    let t = 0;

    while (p < patParts.length && t < topParts.length) {
      if (patParts[p] === '#') {
        // Hash matches everything remaining if at end
        if (p === patParts.length - 1) return true;
        // Search next matching segment
        const nextPat = patParts[p + 1];
        while (t < topParts.length && topParts[t] !== nextPat) {
          t++;
        }
        p++;
        continue;
      }

      if (patParts[p] === '*' || patParts[p] === topParts[t]) {
        p++;
        t++;
      } else {
        return false;
      }
    }

    if (p < patParts.length && patParts[p] === '#') p++;

    return p === patParts.length && t === topParts.length;
  }
}

/**
 * Core EventBus Broker Coordinator
 */
class EventBusBroker {
  constructor() {
    this.topics = new Map(); // topicName -> Topic
    this.groups = new Map(); // groupId -> ConsumerGroup
    this.subscribers = new Set();
    this.totalPublished = 0;
    this.startTime = Date.now();

    this.seedDefaultCluster();
  }

  seedDefaultCluster() {
    // Create baseline topics
    this.createTopic('orders.transactions', 3);
    this.createTopic('telemetry.metrics', 4);
    this.createTopic('user.notifications', 2);
    this.createTopic('system.dlq', 1); // Dead-letter queue

    // Preload sample transactions
    const orderTopic = this.topics.get('orders.transactions');
    for (let i = 1; i <= 15; i++) {
      orderTopic.publish(`customer_${i % 4}`, {
        orderId: `ord_100${i}`,
        amount: (i * 24.50).toFixed(2),
        currency: 'USD',
        status: i % 5 === 0 ? 'PENDING' : 'COMPLETED'
      }, { source: 'checkout-api' });
      this.totalPublished++;
    }

    // Preload telemetry
    const telTopic = this.topics.get('telemetry.metrics');
    for (let i = 1; i <= 20; i++) {
      telTopic.publish(`node_${i % 3}`, {
        cpuUsage: (20 + (i * 3.5) % 60).toFixed(1),
        memoryMB: 512 + i * 32,
        loadAvg: 0.85
      }, { host: `worker-node-${i % 3}` });
      this.totalPublished++;
    }

    // Preload Consumer Group
    const group = this.getOrCreateGroup('analytics_worker_group');
    group.commit('orders.transactions', 0, 3);
    group.commit('orders.transactions', 1, 4);
    group.commit('orders.transactions', 2, 2);
  }

  createTopic(name, partitionCount = 3) {
    if (!name || typeof name !== 'string') throw new Error('Valid topic name required');
    if (this.topics.has(name)) return this.topics.get(name);

    const topic = new Topic(name, Math.max(1, partitionCount));
    this.topics.set(name, topic);
    this.broadcastEvent('topic_created', { name, partitions: topic.partitionCount });
    return topic;
  }

  publish(topicName, key, value, headers = {}) {
    let topic = this.topics.get(topicName);
    if (!topic) {
      topic = this.createTopic(topicName, 3);
    }

    const pubResult = topic.publish(key, value, headers);
    this.totalPublished++;

    this.broadcastEvent('message_published', {
      topic: topicName,
      partitionId: pubResult.partitionId,
      offset: pubResult.offset,
      key
    });

    return pubResult;
  }

  fetch(topicName, partitionId, fromOffset = 0, limit = 50) {
    const topic = this.topics.get(topicName);
    if (!topic) throw new Error(`Topic ${topicName} does not exist`);
    return topic.fetch(partitionId, fromOffset, limit);
  }

  getOrCreateGroup(groupId) {
    if (!this.groups.has(groupId)) {
      this.groups.set(groupId, new ConsumerGroup(groupId));
    }
    return this.groups.get(groupId);
  }

  commitOffset(groupId, topic, partitionId, offset) {
    const group = this.getOrCreateGroup(groupId);
    group.commit(topic, partitionId, offset);
    return { success: true, groupId, topic, partitionId, offset };
  }

  getGroupLag(groupId, topicName) {
    const group = this.getOrCreateGroup(groupId);
    const topic = this.topics.get(topicName);
    if (!topic) throw new Error(`Topic ${topicName} not found`);
    return group.calculateLag(topic);
  }

  queryTopics(pattern) {
    const matched = [];
    for (const [name, topic] of this.topics.entries()) {
      if (TopicMatcher.matches(pattern, name)) {
        matched.push({
          topic: name,
          partitions: topic.partitionCount,
          totalMessages: topic.getTotalMessages()
        });
      }
    }
    return matched;
  }

  subscribe(res) {
    this.subscribers.add(res);
    res.on('close', () => this.subscribers.delete(res));
  }

  broadcastEvent(eventType, payload) {
    const data = `event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const res of this.subscribers) {
      try { res.write(data); } catch (e) { this.subscribers.delete(res); }
    }
  }

  metrics() {
    let totalMessages = 0;
    for (const t of this.topics.values()) {
      totalMessages += t.getTotalMessages();
    }

    return {
      totalTopics: this.topics.size,
      totalPartitions: Array.from(this.topics.values()).reduce((s, t) => s + t.partitionCount, 0),
      totalMessages,
      totalPublished: this.totalPublished,
      activeConsumerGroups: this.groups.size,
      subscribers: this.subscribers.size,
      uptimeSeconds: Math.floor((Date.now() - this.startTime) / 1000)
    };
  }
}

module.exports = {
  hashKey,
  PartitionLog,
  Topic,
  ConsumerGroup,
  TopicMatcher,
  EventBusBroker
};
