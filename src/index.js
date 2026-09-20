// EventBus-Broker - Production Engine Entrypoint
const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const CoreEngine = require('./engine');

const engine = new CoreEngine();
const PORT = parseInt(process.env.PORT, 10) || 6000;
const publicDir = path.join(__dirname, '..', 'public');
const startTime = Date.now();

function requestHandler(req, res) {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    return res.end();
  }

  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    if (pathname === '/api/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
      return res.end(JSON.stringify({
        status: 'UP',
        service: 'EventBus-Broker',
        uptimeSeconds: Math.floor((Date.now() - startTime) / 1000),
        timestamp: new Date().toISOString()
      }));
    }

    if (pathname === '/api/stats') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
      return res.end(JSON.stringify({
        success: true,
        service: 'EventBus-Broker',
        description: 'High-speed in-memory message broker with durable partition logs, topic filtering and consumer groups.',
        status: 'OPTIMAL',
        activeItems: engine.count()
      }));
    }

    if (req.method === 'POST' && pathname === '/api/process') {
      try {
        const data = JSON.parse(body || '{}');
        const resObj = engine.process(data);
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        return res.end(JSON.stringify({ success: true, result: resObj }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: e.message }));
      }
    }

    let filePath = path.join(publicDir, pathname === '/' ? 'index.html' : pathname);
    fs.stat(filePath, (err, stats) => {
      if (!err && stats.isFile()) {
        const ext = path.extname(filePath);
        const mime = ext === '.html' ? 'text/html; charset=utf-8' : (ext === '.css' ? 'text/css' : 'application/javascript');
        res.writeHead(200, { 'Content-Type': mime, 'Access-Control-Allow-Origin': '*' });
        fs.createReadStream(filePath).pipe(res);
      } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: '404 Not Found', path: pathname }));
      }
    });
  });
}

function startServer(port = PORT, callback) {
  const s = http.createServer(requestHandler);
  s.listen(port, () => {
    if (callback) callback(s);
  });
  return s;
}

if (require.main === module) {
  startServer(PORT, () => {
    console.log('EventBus-Broker running on port ' + PORT);
  });
}

module.exports = { startServer, requestHandler };
