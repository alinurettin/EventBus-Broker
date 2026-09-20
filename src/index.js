// EventBus-Broker v2.0.0 - Production HTTP Server & Event Broker Controller
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { EventBusBroker } = require('./engine');

const broker = new EventBusBroker();
const PORT = parseInt(process.env.PORT, 10) || 6000;
const publicDir = path.join(__dirname, '..', 'public');
const startTime = Date.now();

function requestHandler(req, res) {
  const reqUrl = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
  const pathname = reqUrl.pathname;

  // CORS Headers
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
    });
    return res.end();
  }

  // SSE Live Stream Endpoint
  if (req.method === 'GET' && pathname === '/api/events/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('retry: 3000\n\n');

    const initData = JSON.stringify({
      type: 'INIT',
      metrics: broker.metrics(),
      timestamp: Date.now()
    });
    res.write(`event: init\ndata: ${initData}\n\n`);

    broker.subscribe(res);
    return;
  }

  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', () => {
    const jsonRes = (statusCode, data) => {
      res.writeHead(statusCode, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(JSON.stringify(data));
    };

    // 1. Health API
    if (pathname === '/api/health') {
      return jsonRes(200, {
        status: 'UP',
        service: 'EventBus-Broker',
        version: '2.0.0',
        uptimeSeconds: Math.floor((Date.now() - startTime) / 1000),
        timestamp: new Date().toISOString()
      });
    }

    // 2. Stats & Telemetry API
    if (pathname === '/api/stats') {
      return jsonRes(200, {
        success: true,
        service: 'EventBus-Broker',
        version: '2.0.0',
        metrics: broker.metrics()
      });
    }

    // 3. List Topics API
    if (req.method === 'GET' && pathname === '/api/topics') {
      const topicList = Array.from(broker.topics.values()).map(t => ({
        name: t.name,
        partitionCount: t.partitionCount,
        totalMessages: t.getTotalMessages(),
        partitions: t.getPartitionSummaries()
      }));
      return jsonRes(200, { success: true, topics: topicList });
    }

    // 4. Create Topic API
    if (req.method === 'POST' && pathname === '/api/topics') {
      try {
        const data = JSON.parse(body || '{}');
        const topic = broker.createTopic(data.name, data.partitions || 3);
        return jsonRes(200, {
          success: true,
          topic: {
            name: topic.name,
            partitions: topic.partitionCount
          }
        });
      } catch (err) {
        return jsonRes(400, { success: false, error: err.message });
      }
    }

    // 5. Publish Message API
    if (req.method === 'POST' && pathname === '/api/publish') {
      try {
        const data = JSON.parse(body || '{}');
        if (!data.topic) throw new Error('Topic is required');
        const pubResult = broker.publish(data.topic, data.key, data.value, data.headers || {});
        return jsonRes(200, { success: true, result: pubResult });
      } catch (err) {
        return jsonRes(400, { success: false, error: err.message });
      }
    }

    // 6. Fetch Messages API
    if (req.method === 'GET' && pathname === '/api/fetch') {
      try {
        const topic = reqUrl.searchParams.get('topic');
        const partition = parseInt(reqUrl.searchParams.get('partition') || '0', 10);
        const offset = parseInt(reqUrl.searchParams.get('offset') || '0', 10);
        const limit = parseInt(reqUrl.searchParams.get('limit') || '50', 10);

        if (!topic) throw new Error('Query parameter "topic" is required');
        const messages = broker.fetch(topic, partition, offset, limit);
        return jsonRes(200, { success: true, topic, partition, offset, messages });
      } catch (err) {
        return jsonRes(400, { success: false, error: err.message });
      }
    }

    // 7. Commit Offset API
    if (req.method === 'POST' && pathname === '/api/groups/commit') {
      try {
        const data = JSON.parse(body || '{}');
        const { groupId, topic, partition, offset } = data;
        if (!groupId || !topic || partition === undefined || offset === undefined) {
          throw new Error('groupId, topic, partition, and offset are required');
        }
        const commitRes = broker.commitOffset(groupId, topic, partition, offset);
        return jsonRes(200, commitRes);
      } catch (err) {
        return jsonRes(400, { success: false, error: err.message });
      }
    }

    // 8. Consumer Group Lag API
    if (req.method === 'GET' && pathname === '/api/groups/lag') {
      try {
        const groupId = reqUrl.searchParams.get('groupId');
        const topic = reqUrl.searchParams.get('topic');
        if (!groupId || !topic) throw new Error('Query parameters groupId and topic are required');
        const lagInfo = broker.getGroupLag(groupId, topic);
        return jsonRes(200, { success: true, lagInfo });
      } catch (err) {
        return jsonRes(400, { success: false, error: err.message });
      }
    }

    // 9. Static Web UI Files
    let filePath = path.join(publicDir, pathname === '/' ? 'index.html' : pathname);
    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      const ext = path.extname(filePath).toLowerCase();
      const mimeTypes = {
        '.html': 'text/html; charset=utf-8',
        '.css': 'text/css; charset=utf-8',
        '.js': 'application/javascript; charset=utf-8',
        '.json': 'application/json; charset=utf-8'
      };
      res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
      return res.end(fs.readFileSync(filePath));
    }

    jsonRes(404, { error: 'Endpoint not found' });
  });
}

function startServer(portToUse = PORT, callback) {
  const server = http.createServer(requestHandler);
  server.listen(portToUse, callback);
  return server;
}

if (require.main === module) {
  startServer(PORT, () => {
    console.log(`⚡ EventBus-Broker v2.0.0 running on http://localhost:${PORT}`);
  });
}

module.exports = { startServer, broker };
