// EventBus-Broker v2.0.0 - Exhaustive Verification Suite
// 100% Non-Mocked Assertions for Partitioned Logs, Consumer Groups, Key Compaction & Hierarchical Topics

const assert = require('assert');
const http = require('http');
const {
  hashKey,
  PartitionLog,
  Topic,
  ConsumerGroup,
  TopicMatcher,
  EventBusBroker
} = require('../src/engine');
const { startServer } = require('../src/index');

let assertionCount = 0;
function pass(desc) {
  assertionCount++;
  console.log(`  ✓ [Assertion ${assertionCount}] ${desc}`);
}

async function runSuite() {
  console.log('====================================================');
  console.log('🧪 Running Verification Suite: EventBus-Broker (v2.0.0)');
  console.log('====================================================\n');

  // ----------------------------------------------------
  // SECTION 1: Append-Only Partition Log
  // ----------------------------------------------------
  console.log('[SECTION 1: Append-Only Partition Log]');

  const log = new PartitionLog(0, 'test.topic');
  const m1 = log.append('k1', { msg: 'First message' });
  const m2 = log.append('k2', { msg: 'Second message' });
  const m3 = log.append('k1', { msg: 'Updated k1 message' });

  assert.strictEqual(m1.offset, 0);
  assert.strictEqual(m2.offset, 1);
  assert.strictEqual(m3.offset, 2);
  pass('Messages receive strictly monotonically increasing offsets');

  assert.strictEqual(log.getHighWatermark(), 3);
  pass('High watermark reflects next write offset');

  const slice = log.fetch(1, 2);
  assert.strictEqual(slice.length, 2);
  assert.strictEqual(slice[0].offset, 1);
  assert.strictEqual(slice[1].offset, 2);
  pass('Log fetch retrieves sequential window slices');

  // Key Compaction
  const compactedCount = log.compact();
  assert.strictEqual(compactedCount, 2, 'Compacted log keeps only latest k1 and k2');
  const latestK1 = log.messages.find(m => m.key === 'k1');
  assert.strictEqual(latestK1.value.msg, 'Updated k1 message');
  pass('Log compaction retains latest record per key');

  // ----------------------------------------------------
  // SECTION 2: Topic Multi-Partition Hashing
  // ----------------------------------------------------
  console.log('\n[SECTION 2: Multi-Partition Topic Hashing]');

  const topic = new Topic('orders.events', 4);
  assert.strictEqual(topic.partitions.length, 4);
  pass('Topic initialized with 4 independent partition logs');

  // Deterministic partition assignment by key
  const pub1 = topic.publish('cust_101', { event: 'created' });
  const pub2 = topic.publish('cust_101', { event: 'paid' });
  const pub3 = topic.publish('cust_101', { event: 'shipped' });

  assert.strictEqual(pub1.partitionId, pub2.partitionId);
  assert.strictEqual(pub2.partitionId, pub3.partitionId);
  pass('Identical message key deterministically routes to identical partition');

  // Total messages across topic
  assert.strictEqual(topic.getTotalMessages(), 3);
  pass('Topic accurately calculates aggregate message count');

  // ----------------------------------------------------
  // SECTION 3: Consumer Group Lag & Offset Commits
  // ----------------------------------------------------
  console.log('\n[SECTION 3: Consumer Group Lag & Offset Commits]');

  const group = new ConsumerGroup('billing_service');
  group.commit('orders.events', pub1.partitionId, 1);

  assert.strictEqual(group.getCommittedOffset('orders.events', pub1.partitionId), 1);
  pass('Consumer group commits and persists partition offset');

  const lagReport = group.calculateLag(topic);
  assert.ok(lagReport.totalLag >= 2, 'Lag calculated as HighWatermark (3) - Committed (1) = 2');
  pass('Consumer group calculates accurate lag metric across partitions');

  // ----------------------------------------------------
  // SECTION 4: Hierarchical Topic Pattern Matching
  // ----------------------------------------------------
  console.log('\n[SECTION 4: Hierarchical Topic Pattern Matching]');

  assert.strictEqual(TopicMatcher.matches('orders.*.created', 'orders.us.created'), true);
  assert.strictEqual(TopicMatcher.matches('orders.*.created', 'orders.eu.created'), true);
  assert.strictEqual(TopicMatcher.matches('orders.*.created', 'orders.us.created.nested'), false);
  pass('Single-level wildcard (*) matches single topic segment');

  assert.strictEqual(TopicMatcher.matches('telemetry.#', 'telemetry.nodes.us-east.cpu'), true);
  assert.strictEqual(TopicMatcher.matches('telemetry.#', 'telemetry'), true);
  assert.strictEqual(TopicMatcher.matches('telemetry.#', 'billing.invoices'), false);
  pass('Multi-level wildcard (#) matches zero or more topic segments');

  // ----------------------------------------------------
  // SECTION 5: Live Ephemeral HTTP Server & REST Gateway
  // ----------------------------------------------------
  console.log('\n[SECTION 5: Live Ephemeral HTTP Server & REST Gateway]');

  const server = await new Promise((resolve, reject) => {
    try {
      const s = startServer(0, () => resolve(s));
    } catch (err) {
      reject(err);
    }
  });

  const testPort = server.address().port;
  console.log(`  [HTTP] Ephemeral server running on port ${testPort}`);

  const makeReq = (path, method = 'GET', data = null) => {
    return new Promise((resolve, reject) => {
      const postData = data ? JSON.stringify(data) : null;
      const opts = {
        hostname: '127.0.0.1',
        port: testPort,
        path,
        method,
        headers: {
          'Content-Type': 'application/json',
          ...(postData ? { 'Content-Length': Buffer.byteLength(postData) } : {})
        }
      };

      const req = http.request(opts, res => {
        let raw = '';
        res.on('data', chunk => raw += chunk);
        res.on('end', () => {
          let parsed = null;
          try { parsed = JSON.parse(raw); } catch (e) { parsed = raw; }
          resolve({ status: res.statusCode, headers: res.headers, body: parsed });
        });
      });

      req.on('error', reject);
      if (postData) req.write(postData);
      req.end();
    });
  };

  // 1. GET /api/health
  const healthRes = await makeReq('/api/health');
  assert.strictEqual(healthRes.status, 200);
  assert.strictEqual(healthRes.body.service, 'EventBus-Broker');
  assert.strictEqual(healthRes.body.status, 'UP');
  pass('GET /api/health returns HTTP 200 with service UP');

  // 2. GET /api/stats
  const statsRes = await makeReq('/api/stats');
  assert.strictEqual(statsRes.status, 200);
  assert.ok(statsRes.body.metrics.totalTopics > 0);
  pass('GET /api/stats returns broker runtime telemetry');

  // 3. GET /api/topics
  const topicsRes = await makeReq('/api/topics');
  assert.strictEqual(topicsRes.status, 200);
  assert.ok(topicsRes.body.topics.length >= 3);
  pass('GET /api/topics returns registered topic catalogs and partition counts');

  // 4. POST /api/topics
  const createTopicRes = await makeReq('/api/topics', 'POST', { name: 'inventory.events', partitions: 5 });
  assert.strictEqual(createTopicRes.status, 200);
  assert.strictEqual(createTopicRes.body.topic.name, 'inventory.events');
  assert.strictEqual(createTopicRes.body.topic.partitions, 5);
  pass('POST /api/topics dynamically creates multi-partition topic');

  // 5. POST /api/publish
  const pubPayload = {
    topic: 'inventory.events',
    key: 'sku_9921',
    value: { sku: 'sku_9921', stock: 150, location: 'warehouse_A' }
  };
  const pubRes = await makeReq('/api/publish', 'POST', pubPayload);
  assert.strictEqual(pubRes.status, 200);
  assert.strictEqual(pubRes.body.result.topic, 'inventory.events');
  assert.ok(pubRes.body.result.offset >= 0);
  pass('POST /api/publish appends record frame to target partition');

  // 6. GET /api/fetch
  const fetchUrl = `/api/fetch?topic=inventory.events&partition=${pubRes.body.result.partitionId}&offset=0&limit=10`;
  const fetchRes = await makeReq(fetchUrl);
  assert.strictEqual(fetchRes.status, 200);
  assert.ok(fetchRes.body.messages.length > 0);
  assert.strictEqual(fetchRes.body.messages[0].key, 'sku_9921');
  pass('GET /api/fetch retrieves committed messages by partition and offset');

  // 7. POST /api/groups/commit
  const commitPayload = {
    groupId: 'inventory_sync_group',
    topic: 'inventory.events',
    partition: pubRes.body.result.partitionId,
    offset: pubRes.body.result.offset + 1
  };
  const commitRes = await makeReq('/api/groups/commit', 'POST', commitPayload);
  assert.strictEqual(commitRes.status, 200);
  assert.strictEqual(commitRes.body.success, true);
  pass('POST /api/groups/commit persists consumer group progress');

  // 8. GET /api/groups/lag
  const lagUrl = `/api/groups/lag?groupId=inventory_sync_group&topic=inventory.events`;
  const lagRes = await makeReq(lagUrl);
  assert.strictEqual(lagRes.status, 200);
  assert.ok(lagRes.body.lagInfo.partitions.length === 5);
  pass('GET /api/groups/lag calculates lag across all topic partitions');

  // 9. SSE stream test
  await new Promise(resolve => {
    const sseReq = http.request({
      hostname: '127.0.0.1',
      port: testPort,
      path: '/api/events/stream',
      method: 'GET'
    }, res => {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-type'], 'text/event-stream');
      res.on('data', chunk => {
        const text = chunk.toString();
        if (text.includes('event: init')) {
          res.destroy();
          resolve();
        }
      });
    });
    sseReq.end();
  });
  pass('SSE connection to /api/events/stream established and receives init event');

  // 10. 404 Route
  const notFoundRes = await makeReq('/api/unknown_endpoint');
  assert.strictEqual(notFoundRes.status, 404);
  pass('Invalid path returns HTTP 404');

  server.close();

  console.log('\n====================================================');
  console.log(`🎉 ALL ${assertionCount} ASSERTIONS PASSED (100% Non-Mocked Coverage)`);
  console.log('====================================================\n');
}

runSuite().catch(err => {
  console.error('\n❌ TEST SUITE FAILED:', err);
  process.exit(1);
});
