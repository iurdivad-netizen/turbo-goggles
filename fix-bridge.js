#!/usr/bin/env node
// ── WebSocket-to-FIX Bridge ──
// Accepts WebSocket connections from the browser and forwards
// FIX 4.4 messages to the broker over raw TCP.
//
// Usage: node fix-bridge.js [--port 8089]
//
// Dependencies: npm install ws

const net = require('net');
const tls = require('tls');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.argv.find((_, i, a) => a[i - 1] === '--port') || '8089', 10);
const SOH = '\x01'; // FIX field delimiter

// ── Helpers ──

function log(tag, msg) {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`[${ts}] [${tag}] ${msg}`);
}

function pipeDelimToSOH(msg) {
  return msg.replace(/\|/g, SOH);
}

function sohToPipeDelim(msg) {
  return msg.replace(/\x01/g, '|');
}

function computeChecksum(msg) {
  let sum = 0;
  for (let i = 0; i < msg.length; i++) {
    sum += msg.charCodeAt(i);
  }
  return String(sum % 256).padStart(3, '0');
}

function computeBodyLength(msg) {
  // Body length = everything after "8=FIX.x.x|9=nnn|" up to (but not including) "10=xxx|"
  const afterTag9 = msg.indexOf(SOH + '35=');
  if (afterTag9 < 0) return msg.length;
  const beforeTag10 = msg.lastIndexOf(SOH + '10=');
  if (beforeTag10 < 0) return msg.length - afterTag9 - 1;
  return beforeTag10 - afterTag9;
}

function finalizeFixMessage(rawMsg) {
  // Convert pipe delimiters to SOH
  let msg = pipeDelimToSOH(rawMsg);

  // Strip existing 9= and 10= if present (we'll recompute)
  msg = msg.replace(/9=\d+\x01/, '');
  msg = msg.replace(/10=\d{3}\x01?$/, '');
  if (!msg.endsWith(SOH)) msg += SOH;

  // Split at first SOH to get BeginString
  const firstSOH = msg.indexOf(SOH);
  const beginString = msg.substring(0, firstSOH + 1); // e.g. "8=FIX.4.4\x01"
  const rest = msg.substring(firstSOH + 1);

  // Compute body length (everything after tag 9)
  const bodyLen = Buffer.byteLength(rest, 'ascii');
  const withLen = beginString + '9=' + bodyLen + SOH + rest;

  // Compute checksum over everything
  const checksum = computeChecksum(withLen);
  return withLen + '10=' + checksum + SOH;
}

// ── TCP connection to FIX server ──

function createFixConnection(host, port, useSSL) {
  return new Promise((resolve, reject) => {
    const opts = { host, port: parseInt(port, 10) };
    let socket;

    if (useSSL) {
      socket = tls.connect(opts, () => {
        log('TCP', `TLS connected to ${host}:${port}`);
        resolve(socket);
      });
    } else {
      socket = net.createConnection(opts, () => {
        log('TCP', `Connected to ${host}:${port}`);
        resolve(socket);
      });
    }

    socket.setTimeout(15000);
    socket.on('timeout', () => {
      log('TCP', `Connection timeout to ${host}:${port}`);
      socket.destroy();
      reject(new Error('Connection timeout'));
    });
    socket.on('error', (err) => {
      log('TCP', `Connection error: ${err.message}`);
      reject(err);
    });
  });
}

// ── WebSocket Server ──

const wss = new WebSocketServer({ port: PORT });
log('BRIDGE', `WebSocket-to-FIX bridge listening on ws://localhost:${PORT}`);
log('BRIDGE', 'Waiting for browser connections...');

wss.on('connection', (ws, req) => {
  const clientIP = req.socket.remoteAddress;
  log('WS', `Browser connected from ${clientIP}`);

  // Map of active FIX TCP connections keyed by "host:port"
  const fixSockets = new Map();
  let buffer = '';

  ws.on('message', async (data) => {
    let payload;
    try {
      payload = JSON.parse(data.toString());
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
      return;
    }

    const { action, id, host, port, ssl, fixMessage } = payload;

    if (action === 'connect') {
      // Open a TCP connection to the FIX server
      const key = `${host}:${port}`;
      log('WS', `Connect request: ${key} (SSL: ${ssl || false})`);

      try {
        const socket = await createFixConnection(host, parseInt(port, 10), ssl === 'ssl');
        fixSockets.set(key, socket);

        // Buffer for partial FIX messages
        let recvBuffer = '';

        socket.on('data', (chunk) => {
          recvBuffer += chunk.toString('ascii');

          // FIX messages end with SOH after tag 10
          // Split on complete messages
          const parts = recvBuffer.split(/(?<=10=\d{3}\x01)/);
          recvBuffer = parts.pop() || ''; // last part may be incomplete

          for (const fixMsg of parts) {
            if (fixMsg.trim()) {
              const readable = sohToPipeDelim(fixMsg);
              log('FIX←', readable);
              ws.send(JSON.stringify({
                type: 'fix_response',
                id,
                key,
                message: readable
              }));
            }
          }
        });

        socket.on('close', () => {
          log('TCP', `Connection closed: ${key}`);
          fixSockets.delete(key);
          ws.send(JSON.stringify({ type: 'disconnected', id, key }));
        });

        socket.on('error', (err) => {
          log('TCP', `Socket error on ${key}: ${err.message}`);
          ws.send(JSON.stringify({ type: 'error', id, key, message: err.message }));
        });

        ws.send(JSON.stringify({ type: 'connected', id, key }));
      } catch (err) {
        ws.send(JSON.stringify({ type: 'error', id, key, message: err.message }));
      }

    } else if (action === 'send') {
      // Send a FIX message over an existing TCP connection
      const key = `${host}:${port}`;
      const socket = fixSockets.get(key);

      if (!socket || socket.destroyed) {
        ws.send(JSON.stringify({ type: 'error', id, key, message: 'Not connected. Connect first.' }));
        return;
      }

      try {
        const finalMsg = finalizeFixMessage(fixMessage);
        log('FIX→', sohToPipeDelim(finalMsg));
        socket.write(finalMsg, 'ascii');
        ws.send(JSON.stringify({ type: 'sent', id, key }));
      } catch (err) {
        ws.send(JSON.stringify({ type: 'error', id, key, message: err.message }));
      }

    } else if (action === 'disconnect') {
      const key = `${host}:${port}`;
      const socket = fixSockets.get(key);
      if (socket) {
        socket.destroy();
        fixSockets.delete(key);
      }
      ws.send(JSON.stringify({ type: 'disconnected', id, key }));

    } else {
      ws.send(JSON.stringify({ type: 'error', message: `Unknown action: ${action}` }));
    }
  });

  ws.on('close', () => {
    log('WS', 'Browser disconnected. Closing all FIX connections.');
    for (const [key, socket] of fixSockets) {
      socket.destroy();
    }
    fixSockets.clear();
  });
});
