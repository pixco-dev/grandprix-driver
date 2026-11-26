// 그랑프리 드라이버 - 멀티플레이 릴레이 서버
// 역할: (1) public/ 의 게임 HTML 정적 서빙  (2) WebSocket 으로 방 관리 + 차량 상태 릴레이
// 권위(authoritative) 없음: 각 클라이언트가 자기 차만 물리로 돌리고, 서버는 받은 상태를 같은 방의 다른 사람에게 그대로 전달.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.webmanifest': 'application/manifest+json'
};

// ---- 정적 파일 서빙 ----
const server = http.createServer((req, res) => {
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (url === '/' || url === '') url = '/index.html';
  const fp = path.normalize(path.join(PUBLIC, url));
  if (!fp.startsWith(PUBLIC)) { res.writeHead(403); res.end('forbidden'); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---- 방 / 릴레이 ----
const wss = new WebSocketServer({ server });
const rooms = new Map();
let nextId = 1;
const CODE = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function genCode() { let c; do { c = ''; for (let i = 0; i < 4; i++) c += CODE[Math.floor(Math.random() * CODE.length)]; } while (rooms.has(c)); return c; }

function roster(room) {
  return room.order.map((id, slot) => {
    const p = room.players.get(id);
    return p ? { id, name: p.name, team: p.team, color: p.color, car: p.car, cmp: p.cmp, slot } : null;
  }).filter(Boolean);
}
function broadcast(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const [id, p] of room.players) { if (id === except) continue; if (p.ws.readyState === 1) p.ws.send(s); }
}

wss.on('connection', (ws) => {
  ws.id = nextId++; ws.room = null; ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    if (m.t === 'join') {
      let code = (m.room || '').toUpperCase().trim();
      let room;
      if (!code) { // 방 생성
        code = genCode();
        room = { code, players: new Map(), order: [], hostId: ws.id, config: null };
        rooms.set(code, room);
      } else {
        room = rooms.get(code);
        if (!room) { ws.send(JSON.stringify({ t: 'err', msg: '방을 찾을 수 없어요: ' + code })); return; }
        if (room.players.size >= 11) { ws.send(JSON.stringify({ t: 'err', msg: '방이 가득 찼어요 (최대 11명)' })); return; }
      }
      ws.room = code;
      const p = { ws, name: (m.name || '플레이어').slice(0, 16), team: (m.team | 0), color: m.color || '#ffffff', car: m.car || '', cmp: m.cmp || 'M' };
      room.players.set(ws.id, p); room.order.push(ws.id);
      ws.send(JSON.stringify({ t: 'welcome', id: ws.id, room: code, host: room.hostId === ws.id, players: roster(room), config: room.config }));
      broadcast(room, { t: 'peer', p: { id: ws.id, name: p.name, team: p.team, color: p.color, car: p.car, cmp: p.cmp } }, ws.id);
      return;
    }

    if (!ws.room) return;
    const room = rooms.get(ws.room);
    if (!room) return;

    switch (m.t) {
      case 'meta': {
        const p = room.players.get(ws.id);
        if (p) { if (m.p.name != null) p.name = String(m.p.name).slice(0, 16); if (m.p.team != null) p.team = m.p.team | 0; if (m.p.color != null) p.color = m.p.color; if (m.p.cmp != null) p.cmp = m.p.cmp; }
        broadcast(room, { t: 'meta', id: ws.id, p: m.p }, ws.id);
        break;
      }
      case 'config':
        if (ws.id === room.hostId) { room.config = m.config; broadcast(room, { t: 'config', config: m.config }, ws.id); }
        break;
      case 'start':
        if (ws.id === room.hostId) { room.config = m.config || room.config; broadcast(room, { t: 'start', roster: roster(room), config: room.config }); }
        break;
      case 's': { // 차량 상태 릴레이 (가장 빈번)
        const s = JSON.stringify({ t: 's', id: ws.id, d: m.d });
        for (const [id, p] of room.players) { if (id !== ws.id && p.ws.readyState === 1) p.ws.send(s); }
        break;
      }
      case 'ev':
        broadcast(room, Object.assign({}, m, { id: ws.id, t: 'ev' }), ws.id);
        break;
      case 'ping':
        ws.send(JSON.stringify({ t: 'pong', c: m.c, now: Date.now() }));
        break;
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room); if (!room) return;
    room.players.delete(ws.id); room.order = room.order.filter(i => i !== ws.id);
    if (room.players.size === 0) { rooms.delete(room.code); return; }
    if (room.hostId === ws.id) { room.hostId = room.order[0]; broadcast(room, { t: 'host', id: room.hostId }); }
    broadcast(room, { t: 'leave', id: ws.id });
  });
});

// 끊긴 연결 정리 (Render 프록시 유휴 방지)
const iv = setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch (e) {} });
}, 30000);
wss.on('close', () => clearInterval(iv));

server.listen(PORT, () => console.log('grandprix-mp listening on :' + PORT));
