const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');

const port = Number(process.env.PORT) || 8080;
const indexPath = path.join(__dirname, 'index.html');

const httpServer = http.createServer((request, response) => {
  if (request.url === '/health') {
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify({ http: true, websocket: true, port }));
    return;
  }

  if (request.url !== '/' && request.url !== '/index.html') {
    response.writeHead(404, { 'Content-Type': 'text/plain' });
    response.end('Not found');
    return;
  }

  fs.readFile(indexPath, (error, page) => {
    if (error) {
      response.writeHead(500, { 'Content-Type': 'text/plain' });
      response.end('Unable to load index.html');
      return;
    }

    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    response.end(page);
  });
});

const dashboardWebSocketServer = new WebSocketServer({ noServer: true });
const relayWebSocketServer = new WebSocketServer({ noServer: true });

function isAllowedTarget(target) {
  if (!target || typeof target !== 'string') return false;

  try {
    const parsed = new URL(target);
    return (parsed.protocol === 'ws:' || parsed.protocol === 'wss:');
  } catch (error) {
    return false;
  }
}

dashboardWebSocketServer.on('connection', (socket) => {
  socket.send('<strong>Connection ready.</strong><br>Waiting for your next WebSocket message.');

  socket.on('message', (message) => {
    const payload = message.toString();
    for (const client of dashboardWebSocketServer.clients) {
      if (client.readyState === client.OPEN) {
        client.send(payload);
      }
    }
  });
});

relayWebSocketServer.on('connection', (client, request, target) => {
  const upstream = new WebSocket(target, { origin: request.headers.origin });
  const pendingMessages = [];

  client.on('message', (message, isBinary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(message, { binary: isBinary });
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      pendingMessages.push({ message, isBinary });
    }
  });

  upstream.on('open', () => {
    for (const pending of pendingMessages) {
      upstream.send(pending.message, { binary: pending.isBinary });
    }
    pendingMessages.length = 0;
  });

  upstream.on('message', (message, isBinary) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message, { binary: isBinary });
    }
  });

  upstream.on('error', () => {
    if (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING) {
      client.close(1011, 'Relay connection failed');
    }
  });

  upstream.on('close', (code, reason) => {
    const isValidCloseCode = typeof code === 'number' && Number.isInteger(code) && code >= 1000 && code <= 4999 && code !== 1005 && code !== 1006 && code !== 1015;
    const normalizedCode = isValidCloseCode ? code : 1000;
    const normalizedReason = Buffer.isBuffer(reason) ? reason.toString() : (typeof reason === 'string' ? reason : '');

    if (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING) {
      client.close(normalizedCode, normalizedReason);
    }
  });

  client.on('close', () => {
    if (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING) {
      upstream.close();
    }
  });
});

httpServer.on('upgrade', (request, socket, head) => {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);

  if (requestUrl.pathname === '/relay') {
    const target = requestUrl.searchParams.get('target');
    if (!isAllowedTarget(target)) {
      socket.destroy();
      return;
    }
    relayWebSocketServer.handleUpgrade(request, socket, head, (client) => {
      relayWebSocketServer.emit('connection', client, request, target);
    });
    return;
  }

  dashboardWebSocketServer.handleUpgrade(request, socket, head, (client) => {
    dashboardWebSocketServer.emit('connection', client, request);
  });
});

httpServer.listen(port, '0.0.0.0', () => {
  console.log(`WebSocket tester running at http://localhost:${port}`);
  console.log(`WebSocket endpoint: ws://localhost:${port}`);
});

httpServer.on('error', (error) => {
  if (error.code === 'EADDRINUSE') {
    console.error(`Port ${port} is already in use. Stop the existing server or use another port.`);
  } else {
    console.error(error);
  }
  process.exitCode = 1;
});