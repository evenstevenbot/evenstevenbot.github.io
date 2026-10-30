/* Fin Ops · フィンオプス — real-time multiplayer fish feeding game.
 * Networking mirrors /tools/pokerplanning: PeerJS (WebRTC data channels, public PeerJS cloud
 * broker for signalling), host-as-hub star topology. The room creator claims the peer id
 * derived from the room code and runs the authoritative simulation (~30 Hz); everyone else
 * connects to that id, sends steering input, and renders interpolated snapshots (~15 Hz).
 * ?local=1 on localhost swaps PeerJS for a BroadcastChannel loopback (tests only).
 */
(() => {
'use strict';
const { COLORS, SPECIES } = window.FinFish;
const PREFIX = 'evenstevenbot-finops-v1-';
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ';
const MAX_PLAYERS = 20;
const WORLD_W = 2000, WORLD_H = 1250, SAND_H = 70;
const FISH_LEN = 110, FISH_HT = FISH_LEN * 208 / 268;
const MAX_SIZE = 2.6, GROW = 1.1;
const TICK_MS = 33, SNAP_EVERY = 2, INTERP_MS = 100;
const FOOD_R = 9;
const FOOD_RATE = { low: 1.1, med: 2.1, high: 3.6 };   // flakes / second (scaled by player count)
const DEFAULTS = { mode: 'timer', min: 1, goal: 20, food: 'med' };
const NAME_KEY = 'finops-name', FISH_KEY = 'finops-fish';

// ICE: same STUN + Metered TURN config as /tools/pokerplanning (TURN covers UDP-blocking networks).
const STUN_SERVERS = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
const TURN_SERVERS = [
  { urls: 'stun:stun.relay.metered.ca:80' },
  { urls: 'turn:global.relay.metered.ca:80', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turn:global.relay.metered.ca:80?transport=tcp', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turn:global.relay.metered.ca:443', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turns:global.relay.metered.ca:443?transport=tcp', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
];
const IS_LOCAL = /^(localhost|127\.0\.0\.1)$/.test(location.hostname);
const QS = new URLSearchParams(location.search);
const LOCAL_BUS = IS_LOCAL && QS.has('local');
const peerOpts = { debug: 0, config: { iceServers: STUN_SERVERS.concat(TURN_SERVERS) } };
const REDUCED = matchMedia('(prefers-reduced-motion: reduce)').matches;

const $ = (s) => document.querySelector(s);
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const rand = (a, b) => a + Math.random() * (b - a);
const now = () => performance.now();
const newCode = () => Array.from({ length: 5 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join('');
const INPUT_TIMEOUT_MS = 1200; // clients resend input every 500 ms even when unchanged
const cleanCode = (s) => String(s || '').trim().toUpperCase().replace(/[^A-Z]/g, '').slice(0, 5); // 'abcde', ' AbC de ' -> 'ABCDE'
const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 14);
const speciesOf = (id) => SPECIES.find((s) => s.id === id) || SPECIES[0];
const validFish = (sp, c) => SPECIES.some((s) => s.id === sp) && COLORS.some((x) => x.id === c);

/* ---------------- transport ---------------- */
class Emitter { constructor() { this.h = {}; } on(e, f) { (this.h[e] = this.h[e] || []).push(f); } emit(e, ...a) { (this.h[e] || []).forEach((f) => f(...a)); } }
// BroadcastChannel loopback that mimics the bits of the PeerJS API we use (local tests only).
class LocalPeer extends Emitter {
  constructor(id) {
    super(); this.id = id || 'c' + Math.random().toString(36).slice(2, 10); this.conns = {}; this.destroyed = false;
    this.bc = new BroadcastChannel('finops-local');
    this.bc.onmessage = (ev) => this.onMsg(ev.data);
    if (id) { // claiming a host id: ask if anyone already owns it
      this.bc.postMessage({ k: 'who', id, from: this.id + ':probe' });
      this.taken = false; setTimeout(() => { if (this.destroyed) return; if (this.taken) this.emit('error', { type: 'unavailable-id' }); else this.emit('open', this.id); }, 250);
    } else setTimeout(() => this.emit('open', this.id), 10);
  }
  onMsg(m) {
    if (this.destroyed) return;
    if (m.k === 'who') { if (m.id === this.id && this.opened) this.bc.postMessage({ k: 'iam', to: m.from }); return; }
    if (m.k === 'iam' && m.to === this.id + ':probe') { this.taken = true; return; }
    if (m.to !== this.id) return;
    if (m.k === 'open') { const c = this.mk(m.from); this.bc.postMessage({ k: 'ack', to: m.from, from: this.id }); this.emit('connection', c); setTimeout(() => { c.open = true; c.emit('open'); }, 0); }
    else if (m.k === 'ack') { const c = this.conns[m.from]; if (c && !c.open) { c.open = true; clearTimeout(c.t); c.emit('open'); } }
    else if (m.k === 'd') { const c = this.conns[m.from]; if (c) c.emit('data', m.d); }
    else if (m.k === 'x') { const c = this.conns[m.from]; if (c) { delete this.conns[m.from]; c.emit('close'); } }
  }
  mk(other) {
    const c = new Emitter(); c.peer = other; c.open = false;
    c.send = (d) => { if (!this.destroyed) this.bc.postMessage({ k: 'd', to: other, from: this.id, d }); };
    c.close = () => { if (this.conns[other]) { delete this.conns[other]; this.bc.postMessage({ k: 'x', to: other, from: this.id }); } };
    this.conns[other] = c; return c;
  }
  connect(id) { const c = this.mk(id); this.bc.postMessage({ k: 'open', to: id, from: this.id }); c.t = setTimeout(() => this.emit('error', { type: 'peer-unavailable' }), 2500); return c; }
  destroy() { if (this.destroyed) return; Object.values(this.conns).forEach((c) => c.close()); this.destroyed = true; this.bc.close(); }
}
// The probe trick above needs the owner to answer; mark opened once claim succeeds.
const makePeer = (id) => {
  if (LOCAL_BUS) { const p = new LocalPeer(id); p.on('open', () => { p.opened = true; }); return p; }
  return id ? new Peer(id, peerOpts) : new Peer(peerOpts);
};

/* ---------------- state ---------------- */
let peer = null, role = null, room = '', myId = null, hostConn = null, joined = false, leaving = false;
let lastHostMsg = 0;
const H = { // host-only state
  conns: new Map(),   // pid -> {c, lastSeen}
  players: new Map(), // pid -> player
  foods: [], nextFood: 1, nextPid: 1, set: { ...DEFAULTS }, ph: 'lobby', cdEnd: 0, playStart: 0,
  ev: [], tick: 0, spawnAcc: 0, metaVer: 0, result: null, timer: null, lastT: 0,
};
// client view state
const V = { meta: { pl: {}, set: { ...DEFAULTS }, ph: 'lobby', host: 0, n: 0 }, snaps: [], fish: new Map(), eats: [], fx: [], ph: 'lobby', tl: 0, cd: 0, result: null };

/* ---------------- host: sim ---------------- */
function hostPlayerList() { return [...H.players.values()]; }
function spawnPos(bottom) {
  return bottom ? { x: rand(120, WORLD_W - 120), y: WORLD_H - SAND_H - 50 } : { x: rand(200, WORLD_W - 200), y: rand(250, WORLD_H - 250) };
}
function addPlayer(pid, info) {
  const late = H.ph === 'countdown' || H.ph === 'play';
  const p = { id: pid, name: info.name, sp: info.sp, c: info.c, ready: false, score: 0, eats: 0, size: 1, ...spawnPos(late), vx: 0, vy: 0, ax: 0, ay: 0, f: 1, a: 0, lastScore: 0 };
  H.players.set(pid, p); bumpMeta();
  return p;
}
function fishTaken(sp, c, except) { return hostPlayerList().some((p) => p.id !== except && p.sp === sp && p.c === c); }
function bumpMeta() { H.metaVer++; broadcastMeta(); }
function metaMsg() {
  const pl = {};
  for (const p of H.players.values()) pl[p.id] = [p.name, p.sp, p.c, p.ready ? 1 : 0];
  return { t: 'meta', pl, set: H.set, ph: H.ph, host: 0, n: H.players.size, res: H.result };
}
function broadcastMeta() { const m = metaMsg(); sendAll(m); onClientData(m); }
function sendAll(m) { for (const { c } of H.conns.values()) if (c.open) try { c.send(m); } catch {} }

function resetRound() {
  for (const p of H.players.values()) { p.score = 0; p.eats = 0; p.size = 1; p.lastScore = 0; }
  H.foods = []; H.result = null;
}
function hostStart() {
  if (role !== 'host' || H.players.size === 0) return;
  resetRound();
  H.ph = 'countdown'; H.cdEnd = Date.now() + 3000; bumpMeta();
}
function hostToLobby() {
  resetRound();
  for (const p of H.players.values()) p.ready = false;
  H.ph = 'lobby'; bumpMeta();
}
function hostEnd() {
  const ranked = hostPlayerList().sort((a, b) => b.score - a.score || a.lastScore - b.lastScore);
  H.result = ranked.map((p) => [p.id, p.score]);
  H.ph = 'end'; H.foods = []; bumpMeta();
}
function timeLeft() {
  if (H.ph !== 'play' || H.set.mode !== 'timer') return H.set.min * 60000;
  return Math.max(0, H.playStart + H.set.min * 60000 - Date.now());
}
function mouthOf(p) {
  const s = speciesOf(p.sp);
  const w = FISH_LEN * p.size, h = FISH_HT * p.size * fatOf(p.size);
  const lx = ((s.mouth[0] + 14) / 268 - 0.5) * w, ly = ((s.mouth[1] + 14) / 208 - 0.5) * h;
  const ca = Math.cos(p.a), sa = Math.sin(p.a);
  const rx = lx * ca - ly * sa, ry = lx * sa + ly * ca;
  return { x: p.x + rx * p.f, y: p.y + ry };
}
const fatOf = (size) => 1 + (size - 1) * 0.2;
const radiusOf = (size) => 30 * size;
const eatRadius = (size) => FOOD_R + 8 + 4 * size;

function spawnFood() {
  const gold = Math.random() < 0.2;
  const fast = Math.random() < 0.3;
  H.foods.push({
    id: H.nextFood++, k: gold ? 2 : 1, x: rand(60, WORLD_W - 60), y: -10,
    vy: fast ? rand(150, 230) : rand(40, 85), drift: rand(-25, 25),
    wob: rand(18, 55), wf: rand(1.4, 3.2), ph: rand(0, 6.28), dart: rand(0.4, 1.5), dvx: 0, fade: 1, t: 0,
  });
}
function hostTick() {
  const t = now(); const dt = Math.min(0.08, (t - (H.lastT || t)) / 1000) || TICK_MS / 1000; H.lastT = t;
  // phase transitions
  if (H.ph === 'countdown' && Date.now() >= H.cdEnd) { H.ph = 'play'; H.playStart = Date.now(); H.spawnAcc = 2; bumpMeta(); }
  const players = hostPlayerList();
  // fish movement
  const nowMs = Date.now();
  for (const p of players) {
    if (p.id !== 0 && nowMs - (p.inAt || 0) > INPUT_TIMEOUT_MS) { p.ax = 0; p.ay = 0; } // stalled client: stop steering, drag glides it to a stop
    const mag = Math.hypot(p.ax, p.ay); let ax = p.ax, ay = p.ay; if (mag > 1) { ax /= mag; ay /= mag; }
    const maxV = 340 / Math.pow(p.size, 0.3), acc = 1100;
    p.vx += ax * acc * dt; p.vy += ay * acc * dt;
    const drag = Math.exp(-(mag > 0.05 ? 1.6 : 3.2) * dt); p.vx *= drag; p.vy *= drag;
    const sp = Math.hypot(p.vx, p.vy); if (sp > maxV) { p.vx *= maxV / sp; p.vy *= maxV / sp; }
    p.x += p.vx * dt; p.y += p.vy * dt;
    if (p.vx > 25) p.f = 1; else if (p.vx < -25) p.f = -1;
    const tilt = clamp(Math.atan2(p.vy, Math.abs(p.vx) + 60) * 0.7, -0.55, 0.55) * p.f;
    p.a += (tilt - p.a) * Math.min(1, dt * 8);
  }
  // fish-fish collisions: mass ~ size^2, elastic-ish impulses so you can shove others
  for (let i = 0; i < players.length; i++) for (let j = i + 1; j < players.length; j++) {
    const a = players[i], b = players[j];
    let dx = b.x - a.x, dy = (b.y - a.y) * 1.25; const r = radiusOf(a.size) + radiusOf(b.size);
    let d = Math.hypot(dx, dy); if (d >= r) continue;
    if (d < 0.01) { dx = 1; dy = 0; d = 1; }
    const nx = dx / d, ny = dy / d, ma = a.size * a.size, mb = b.size * b.size, over = r - d;
    a.x -= nx * over * mb / (ma + mb); a.y -= ny * over * mb / (ma + mb);
    b.x += nx * over * ma / (ma + mb); b.y += ny * over * ma / (ma + mb);
    const vrel = (b.vx - a.vx) * nx + (b.vy - a.vy) * ny;
    if (vrel < 0) {
      const J = -(1 + 0.9) * vrel / (1 / ma + 1 / mb);
      a.vx -= J / ma * nx; a.vy -= J / ma * ny; b.vx += J / mb * nx; b.vy += J / mb * ny;
    }
  }
  for (const p of players) {
    const r = radiusOf(p.size);
    if (p.x < r) { p.x = r; p.vx = Math.abs(p.vx) * 0.5; } if (p.x > WORLD_W - r) { p.x = WORLD_W - r; p.vx = -Math.abs(p.vx) * 0.5; }
    const top = r * 0.8 + 10, bot = WORLD_H - SAND_H + 10 - r * 0.8;
    if (p.y < top) { p.y = top; p.vy = Math.abs(p.vy) * 0.5; } if (p.y > bot) { p.y = bot; p.vy = -Math.abs(p.vy) * 0.5; }
  }
  // food
  if (H.ph === 'play') {
    const rate = FOOD_RATE[H.set.food] * (1 + 0.12 * (players.length - 1));
    H.spawnAcc += rate * dt; while (H.spawnAcc >= 1 && H.foods.length < 160) { H.spawnAcc -= 1; spawnFood(); }
    const mouths = players.map((p) => [p, mouthOf(p)]);
    for (const f of H.foods) {
      f.t += dt; f.dart -= dt;
      if (f.dart <= 0) { f.dart = rand(0.5, 1.6); f.dvx = rand(-1, 1) * rand(60, 140); } // flutter
      f.dvx *= Math.exp(-2.5 * dt);
      let ev = 0, evy = 0;
      for (const [, m] of mouths) { // shy flakes: slip away from mouths that come close
        const dx = f.x - m.x, dy = f.y - m.y, d = Math.hypot(dx, dy);
        if (d < 95 && d > 0.1) { const k = (1 - d / 95) * 120; ev += dx / d * k; evy += dy / d * k * 0.4; }
      }
      f.x += (f.drift + f.dvx + ev + Math.cos(f.t * f.wf + f.ph) * f.wob * f.wf * 0.5) * dt;
      f.y += (f.vy + evy) * dt;
      f.x = clamp(f.x, 15, WORLD_W - 15);
      if (f.y > WORLD_H - SAND_H + 5) f.fade -= dt * 1.5;
      if (f.fade > 0 && f.y > 0) for (const [p, m] of mouths) {
        if (Math.hypot(f.x - m.x, f.y - m.y) < eatRadius(p.size)) {
          f.fade = -1; p.score += f.k; p.eats++; p.size = Math.min(MAX_SIZE, p.size * GROW); p.lastScore = Date.now();
          H.ev.push([f.id, p.id, Math.round(f.x), Math.round(f.y), f.k]); break;
        }
      }
    }
    H.foods = H.foods.filter((f) => f.fade > 0);
    // win checks
    if (H.set.mode === 'timer' && timeLeft() <= 0) hostEnd();
    else if (H.set.mode === 'goal' && players.some((p) => p.score >= H.set.goal)) hostEnd();
  }
  // drop silent clients
  const cut = Date.now() - 9000;
  for (const [pid, e] of H.conns) if (e.lastSeen < cut) { try { e.c.close(); } catch {} dropClient(pid); }
  // snapshots
  H.tick++;
  const snap = { t: 's', ph: H.ph, tl: timeLeft(), cd: H.ph === 'countdown' ? Math.max(0, H.cdEnd - Date.now()) : 0,
    p: players.map((p) => [p.id, Math.round(p.x), Math.round(p.y), +p.size.toFixed(3), p.score, p.f, +p.a.toFixed(2), Math.round(p.vx), Math.round(p.vy)]),
    fd: H.foods.map((f) => [f.id, Math.round(f.x), Math.round(f.y), f.k, +f.fade.toFixed(2)]), ev: H.ev };
  onClientData(snap);
  if (H.tick % SNAP_EVERY === 0) { sendAll(snap); H.ev = []; }
}

function dropClient(pid) {
  H.conns.delete(pid);
  if (H.players.delete(pid)) bumpMeta();
}
function hostHandle(pid, d) {
  const e = H.conns.get(pid); if (!e || !d || typeof d !== 'object') return;
  e.lastSeen = Date.now();
  const p = H.players.get(pid);
  switch (d.t) {
    case 'hello': {
      if (p) return;
      const name = cleanName(d.name) || 'Fish', sp = String(d.sp), c = String(d.c);
      if (!validFish(sp, c)) return;
      if (H.players.size >= MAX_PLAYERS) { e.c.send({ t: 'full' }); return; }
      if (fishTaken(sp, c)) { e.c.send({ t: 'taken' }); return; }
      addPlayer(pid, { name, sp, c }); e.c.send({ t: 'welcome', id: pid });
      break;
    }
    case 'in': if (p) { p.ax = clamp(+d.x || 0, -1, 1); p.ay = clamp(+d.y || 0, -1, 1); p.inAt = Date.now(); } break;
    case 'ready': if (p) { p.ready = !!d.v; bumpMeta(); } break;
    case 'bye': try { e.c.close(); } catch {} dropClient(pid); break;
  }
}
function onHostConnection(c) {
  const pid = H.nextPid++;
  const ent = { c, lastSeen: Date.now() };
  H.conns.set(pid, ent);
  c.on('open', () => { try { c.send({ t: 'hi', you: pid }); c.send(metaMsg()); } catch {} });
  c.on('data', (d) => hostHandle(pid, d));
  c.on('close', () => { if (H.conns.get(pid) === ent) dropClient(pid); });
  c.on('error', (er) => { if (er && er.type === 'not-open-yet') return; if (H.conns.get(pid) === ent) dropClient(pid); });
}
// local host actions
function hostSet(patch) {
  if (role !== 'host') return;
  const s = H.set;
  if (patch.mode && (patch.mode === 'timer' || patch.mode === 'goal')) s.mode = patch.mode;
  if (patch.dmin) s.min = clamp(s.min + patch.dmin, 1, 10);
  if (patch.dgoal) s.goal = clamp(s.goal + patch.dgoal * 5, 5, 100);
  if (patch.food && FOOD_RATE[patch.food]) s.food = patch.food;
  bumpMeta();
}

/* ---------------- connection setup ---------------- */
function destroyPeer() { if (peer) { const p = peer; peer = null; try { p.destroy(); } catch {} } }
function createRoom(tries = 0) {
  if (!LOCAL_BUS && typeof Peer === 'undefined') { $('#homeError').textContent = 'Could not load the real-time library. Check your connection and reload.'; return; }
  destroyPeer(); role = 'host'; room = newCode();
  $('#homeError').textContent = ''; $('#createBtn').disabled = true; $('#createBtn').textContent = 'Filling the tank…';
  const p = makePeer(PREFIX + room); peer = p;
  p.on('open', () => {
    if (peer !== p) return;
    myId = 0; history.replaceState(null, '', '?room=' + room + (LOCAL_BUS ? '&local=1' : ''));
    H.lastT = now(); H.timer = setInterval(hostTick, TICK_MS);
    showPick();
  });
  p.on('connection', (c) => { if (peer === p) onHostConnection(c); });
  p.on('disconnected', () => { if (peer === p && !p.destroyed) setTimeout(() => { try { p.reconnect(); } catch {} }, 1500); });
  p.on('error', (err) => {
    if (peer !== p) return;
    if (err.type === 'unavailable-id' && tries < 4) { createRoom(tries + 1); return; }
    if (err.type === 'peer-unavailable' || err.type === 'network' || err.type === 'server-error') return; // ignore stray errors once running
    $('#createBtn').disabled = false; $('#createBtn').textContent = 'Create a tank';
    $('#homeError').textContent = "Can't reach the room server. Your network may be blocking it. Try another network.";
  });
}
function joinRoom(code) {
  if (!LOCAL_BUS && typeof Peer === 'undefined') { $('#homeError').textContent = 'Could not load the real-time library. Check your connection and reload.'; return; }
  destroyPeer(); role = 'client'; room = code;
  $('#homeError').textContent = 'Swimming over to ' + code + '…';
  const p = makePeer(); peer = p;
  p.on('open', () => {
    if (peer !== p) return;
    const c = p.connect(PREFIX + code, { reliable: true, serialization: 'json' }); hostConn = c;
    c.on('open', () => { if (hostConn !== c) return; lastHostMsg = Date.now(); history.replaceState(null, '', '?room=' + code + (LOCAL_BUS ? '&local=1' : '')); });
    c.on('data', (d) => { if (hostConn === c) { lastHostMsg = Date.now(); onClientData(d); } });
    c.on('close', () => { if (hostConn === c) hostLeft(); });
    c.on('error', () => { if (hostConn === c) hostLeft(); });
  });
  p.on('error', (err) => {
    if (peer !== p) return;
    if (err.type === 'peer-unavailable' && !lastHostMsg) { destroyPeer(); showHome(); $('#homeError').textContent = `No tank called ${code}. Check the code, or create a new tank.`; return; }
    if (!lastHostMsg) { $('#homeError').textContent = "Can't reach the room server. Your network may be blocking it. Try another network."; }
  });
}
function hostLeft() {
  if (leaving || role !== 'client' || !hostConn) return;
  hostConn = null; destroyPeer();
  showMsg('The host left the tank', 'This game ended because the host closed their page. Start a new tank to keep playing.');
}
function send(m) {
  if (role === 'host') { if (m.t === 'ready') { const p = H.players.get(0); if (p) { p.ready = !!m.v; bumpMeta(); } } else if (m.t === 'in') { const p = H.players.get(0); if (p) { p.ax = m.x; p.ay = m.y; p.inAt = Date.now(); } } else if (m.t === 'hello') hostHello(m); return; }
  if (hostConn) try { hostConn.send(m); } catch {}
}
function hostHello(m) {
  if (H.players.has(0)) return;
  if (fishTaken(m.sp, m.c)) { onClientData({ t: 'taken' }); return; }
  addPlayer(0, { name: cleanName(m.name) || 'Host', sp: m.sp, c: m.c }); onClientData({ t: 'welcome', id: 0 });
}
setInterval(() => { if (role === 'client' && lastHostMsg && hostConn && Date.now() - lastHostMsg > 7000) hostLeft(); }, 1000);
setInterval(() => { if (role === 'client' && hostConn) send({ t: 'p' }); }, 2000);

/* ---------------- client: messages ---------------- */
function onClientData(d) {
  if (!d || typeof d !== 'object') return;
  switch (d.t) {
    case 'hi': myId = null; break;
    case 'bye': if (role === 'client') hostLeft(); break;
    case 'meta': {
      const prevPh = V.meta.ph; V.meta = d; V.result = d.res;
      if (d.ph !== prevPh) onPhase(prevPh, d.ph);
      renderMetaUI(); break;
    }
    case 's': {
      d.at = now(); V.snaps.push(d); if (V.snaps.length > 8) V.snaps.shift();
      V.ph = d.ph; V.tl = d.tl; V.cd = d.cd;
      for (const e of d.ev || []) onEat(e);
      break;
    }
    case 'welcome': myId = d.id; joined = true; showGame(); break;
    case 'taken': $('#pickError').textContent = 'Someone just grabbed that fish. Pick another!'; $('#diveBtn').disabled = false; renderPick(); break;
    case 'full': $('#pickError').textContent = 'This tank is full (20 fish max). Try again later or start your own tank.'; $('#diveBtn').disabled = false; break;
  }
}

/* ---------------- UI: screens ---------------- */
function setScreen(name) { document.body.className = 'screen-' + name + (document.body.classList.contains('urgent') ? ' urgent' : ''); }
function showHome() {
  setScreen('home'); $('#home').hidden = false; $('#pick').hidden = true; $('#hud').hidden = true; $('#board').hidden = true; $('#lobbyBar').hidden = true; $('#endScreen').hidden = true; $('#msg').hidden = true;
  $('#createBtn').disabled = false; $('#createBtn').textContent = 'Create a tank';
}
let pickSp = null, pickC = null;
function showPick() {
  setScreen('pick'); $('#home').hidden = true; $('#pick').hidden = false;
  $('#pickCode').textContent = room; $('#menuCode').textContent = room;
  $('#nameInput').value = localStorage.getItem(NAME_KEY) || '';
  try { const f = JSON.parse(localStorage.getItem(FISH_KEY) || 'null'); if (f && validFish(f[0], f[1])) { pickSp = f[0]; pickC = f[1]; } } catch {}
  renderPick();
}
function takenSet() { const s = new Set(); for (const [id, m] of Object.entries(V.meta.pl || {})) if (+id !== myId) s.add(m[1] + ':' + m[2]); return s; }
function renderPick() {
  if ($('#pick').hidden) return;
  const taken = takenSet();
  if (!pickSp || taken.has(pickSp + ':' + pickC) || !validFish(pickSp, pickC)) { // first free combo
    pickSp = pickSp && validFish(pickSp, COLORS[0].id) ? pickSp : SPECIES[Math.floor(Math.random() * SPECIES.length)].id;
    const free = COLORS.find((c) => !taken.has(pickSp + ':' + c.id));
    if (free) pickC = free.id; else { for (const s of SPECIES) { const c = COLORS.find((x) => !taken.has(s.id + ':' + x.id)); if (c) { pickSp = s.id; pickC = c.id; break; } } }
  }
  const sg = $('#speciesGrid'); sg.textContent = '';
  for (const s of SPECIES) {
    const free = COLORS.filter((c) => !taken.has(s.id + ':' + c.id)).length;
    const b = el('button', 'sp-btn' + (s.id === pickSp ? ' on' : '')); b.setAttribute('role', 'radio'); b.setAttribute('aria-checked', s.id === pickSp);
    b.disabled = free === 0; b.title = s.name;
    const showC = s.id === pickSp ? pickC : (COLORS.find((c) => c.hex.toUpperCase() === s.body.toUpperCase()) || COLORS[0]).id;
    const img = el('img'); img.src = window.FinFish.urlFor(s.id, showC); img.alt = '';
    b.append(img, el('span', 'sp-name', s.name), el('span', 'jp', s.jp));
    b.onclick = () => { pickSp = s.id; if (taken.has(pickSp + ':' + pickC)) pickC = (COLORS.find((c) => !taken.has(s.id + ':' + c.id)) || {}).id; renderPick(); };
    sg.append(b);
  }
  const cg = $('#colorGrid'); cg.textContent = '';
  for (const c of COLORS) {
    const isTaken = taken.has(pickSp + ':' + c.id);
    const b = el('button', 'c-btn' + (c.id === pickC ? ' on' : '') + (isTaken ? ' taken' : '')); b.setAttribute('role', 'radio'); b.setAttribute('aria-checked', c.id === pickC);
    b.disabled = isTaken; b.title = c.name + (isTaken ? ' (taken)' : '');
    const img = el('img'); img.src = window.FinFish.urlFor(pickSp, c.id); img.alt = '';
    const sw = el('span', 'swatch'); sw.style.background = c.hex;
    b.append(img, el('span', 'c-name', isTaken ? 'taken' : c.name));
    b.prepend(sw);
    b.onclick = () => { pickC = c.id; renderPick(); };
    cg.append(b);
  }
  const n = V.meta.n || 0;
  $('#pickPlayers').textContent = role === 'host' ? `You're the host · ${n}/${MAX_PLAYERS} fish in the tank` : `${n}/${MAX_PLAYERS} fish in the tank` + (V.meta.ph === 'play' || V.meta.ph === 'countdown' ? ' · game in progress, you\'ll join at the bottom' : '');
  if (n >= MAX_PLAYERS && !joined) { $('#pickError').textContent = 'This tank is full (20 fish max).'; $('#diveBtn').disabled = true; }
}
function dive() {
  const name = cleanName($('#nameInput').value) || (role === 'host' ? 'Host' : 'Fish ' + Math.floor(rand(10, 99)));
  localStorage.setItem(NAME_KEY, name); localStorage.setItem(FISH_KEY, JSON.stringify([pickSp, pickC]));
  $('#pickError').textContent = ''; $('#diveBtn').disabled = true;
  send({ t: 'hello', name, sp: pickSp, c: pickC });
  setTimeout(() => { $('#diveBtn').disabled = false; }, 2500);
}
function showGame() {
  setScreen('game'); $('#pick').hidden = true; $('#hud').hidden = false; $('#board').hidden = false;
  if (matchMedia('(max-width: 720px)').matches) setBoardCollapsed(true);
  renderMetaUI();
}
function showMsg(title, body) {
  $('#msgTitle').textContent = title; $('#msgBody').textContent = body; $('#msg').hidden = false;
  $('#endScreen').hidden = true; $('#countdown').hidden = true; closeMenu();
}
let cdShownAt = 0;
function onPhase(prev, ph) {
  if (ph === 'countdown') { $('#countdown').hidden = false; cdShownAt = now(); }
  if (ph === 'play' && prev === 'countdown') popCountdown('FEEDING TIME!', true);
  if (ph !== 'countdown' && ph !== 'play') $('#countdown').hidden = true;
  if (ph === 'end') showEnd(); else $('#endScreen').hidden = true;
  if (ph === 'lobby') { readyLocal = false; }
}
let cdLast = '';
function popCountdown(txt, finish) {
  const box = $('#countdown'), t = $('#cdText');
  if (txt === cdLast) return; cdLast = txt;
  box.hidden = false; t.textContent = txt; t.className = finish ? 'go' : 'num';
  void t.offsetWidth; t.classList.add('pop');
  if (finish) setTimeout(() => { if (cdLast === txt) { box.hidden = true; cdLast = ''; } }, 1300);
}
function showEnd() {
  const res = V.result || []; const pl = V.meta.pl || {};
  const pod = $('#podium'); pod.textContent = '';
  const order = [1, 0, 2];
  for (const i of order) {
    const r = res[i]; if (!r) continue; const m = pl[r[0]]; if (!m) continue;
    const col = el('div', 'pod pod' + (i + 1));
    const img = el('img', 'pod-fish'); img.src = window.FinFish.urlFor(m[1], m[2]); img.alt = '';
    col.append(img, el('div', 'pod-name', m[0] + (+r[0] === myId ? ' (you)' : '')), el('div', 'pod-pts', r[1] + ' pts'), el('div', 'pod-block', String(i + 1)));
    pod.append(col);
  }
  const ol = $('#finalList'); ol.textContent = '';
  res.forEach((r, i) => { const m = pl[r[0]]; if (!m) return; const li = el('li', +r[0] === myId ? 'me' : ''); li.append(el('b', '', '#' + (i + 1)), el('span', '', m[0]), el('em', '', r[1] + ' pts')); ol.append(li); });
  const host = role === 'host';
  $('#againBtn').hidden = !host; $('#lobbyBtn').hidden = !host;
  $('#endWait').textContent = host ? '' : 'Waiting for the host to start the next round…';
  $('#endScreen').hidden = false;
}

/* ---------------- HUD / menu / leaderboard ---------------- */
let readyLocal = false;
function renderMetaUI() {
  renderPick();
  const m = V.meta, set = m.set || DEFAULTS, host = role === 'host';
  $('#menuCode').textContent = room;
  document.querySelectorAll('#modeSeg button').forEach((b) => { b.classList.toggle('on', b.dataset.mode === set.mode); b.disabled = !host; });
  document.querySelectorAll('#foodSeg button').forEach((b) => { b.classList.toggle('on', b.dataset.food === set.food); b.disabled = !host; });
  $('#timerVal').textContent = set.min + ' min'; $('#goalVal').textContent = set.goal + ' pts';
  $('#timerStep').classList.toggle('dim', set.mode !== 'timer'); $('#goalStep').classList.toggle('dim', set.mode !== 'goal');
  document.querySelectorAll('.stepper button').forEach((b) => { b.disabled = !host; });
  $('#startOverBtn').disabled = !host;
  document.querySelectorAll('.host-only').forEach((x) => { x.hidden = host; });
  // lobby bar
  const inLobby = joined && m.ph === 'lobby';
  $('#lobbyBar').hidden = !inLobby;
  const pl = Object.entries(m.pl || {}); const ready = pl.filter(([, p]) => p[3]).length;
  $('#readyCount').textContent = `${ready}/${pl.length} ready` + (host ? '' : ' · host starts the game');
  const me = m.pl && m.pl[myId]; readyLocal = !!(me && me[3]);
  $('#readyBtn').textContent = readyLocal ? 'Ready! ✓' : "I'm ready"; $('#readyBtn').classList.toggle('on', readyLocal);
  $('#startBtn').hidden = !host; $('#startBtn').classList.toggle('all-ready', pl.length > 0 && ready === pl.length);
  $('#startBtn').textContent = pl.length > 0 && ready === pl.length ? 'Everyone ready · START!' : 'Start game';
  if (m.ph === 'end' && joined) showEnd();
}
function closeMenu() { $('#menu').hidden = true; $('#menuBtn').setAttribute('aria-expanded', 'false'); }
function setBoardCollapsed(v) { $('#board').classList.toggle('collapsed', v); $('#boardToggle').setAttribute('aria-expanded', String(!v)); }
const rowEls = new Map();
function renderBoard(players) {
  const set = V.meta.set || DEFAULTS, pl = V.meta.pl || {};
  const ranked = players.filter((p) => pl[p.id]).sort((a, b) => b.score - a.score || a.id - b.id);
  const top = Math.max(1, ...ranked.map((p) => p.score));
  const denom = set.mode === 'goal' ? set.goal : top;
  const box = $('#boardRows'); const seen = new Set();
  const rowH = matchMedia('(max-width: 720px)').matches ? 34 : 40;
  ranked.forEach((p, i) => {
    seen.add(p.id); const m = pl[p.id]; let r = rowEls.get(p.id);
    if (!r) {
      r = el('div', 'row'); r.innerHTML = '<b class="rk"></b><img alt=""><span class="nm"></span><span class="bar"><i></i></span><b class="pt"></b>';
      box.append(r); rowEls.set(p.id, r);
    }
    const col = (COLORS.find((c) => c.id === m[2]) || COLORS[0]).hex;
    r.className = 'row' + (i < 3 ? ' top' + (i + 1) : '') + (p.id === myId ? ' me' : '');
    r.style.transform = `translateY(${i * rowH}px)`; r.style.setProperty('--c', col);
    r.querySelector('.rk').textContent = i + 1;
    const img = r.querySelector('img'), src = window.FinFish.urlFor(m[1], m[2]); if (img.getAttribute('src') !== src) img.src = src;
    r.querySelector('.nm').textContent = m[0] + (p.id === myId ? ' · YOU' : '');
    r.querySelector('.bar i').style.width = Math.min(100, p.score / denom * 100) + '%';
    const pt = r.querySelector('.pt'); if (pt.textContent !== String(p.score)) { pt.textContent = p.score; pt.classList.remove('bump'); void pt.offsetWidth; pt.classList.add('bump'); }
  });
  for (const [id, r] of rowEls) if (!seen.has(id)) { r.remove(); rowEls.delete(id); }
  box.style.height = ranked.length * rowH + 'px';
  const myRank = ranked.findIndex((p) => p.id === myId);
  $('#boardMine').textContent = myRank >= 0 ? `YOU #${myRank + 1} · ${ranked[myRank].score}` : '';
  $('#boardFoot').textContent = (set.mode === 'goal' ? `FIRST TO ${set.goal} PTS` : `MOST POINTS IN ${set.min} MIN`) + ` · ${ranked.length}/${MAX_PLAYERS} FISH`;
}

/* ---------------- input ---------------- */
const keys = new Set(); let pointer = null, pointerKind = 'mouse', pointerDown = false, lastSent = { x: 9, y: 9, at: 0 };
// pointer.w is a fixed WORLD point: the fish swims to where you pointed and stops there. (It used to be a
// screen point; because the camera follows your fish, a parked mouse stayed ahead of it forever = endless drift.)
const STEER_KEYS = ['arrowup', 'arrowdown', 'arrowleft', 'arrowright', 'w', 'a', 's', 'd'];
const isTyping = (t) => t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable);
function releaseAll() { keys.clear(); pointer = null; pointerDown = false; sendZero(); }
addEventListener('keydown', (e) => {
  const k = e.key.toLowerCase();
  if (k === 'escape') closeMenu();
  if (k === 'meta' || k === 'control' || k === 'alt') { keys.clear(); return; } // macOS drops keyup for keys held with Cmd
  if (isTyping(e.target) || e.metaKey || e.ctrlKey || e.altKey) return;
  if (STEER_KEYS.includes(k)) { keys.add(k); e.preventDefault(); }
});
addEventListener('keyup', (e) => { const k = e.key.toLowerCase(); keys.delete(k); if (k === 'meta' || k === 'control' || k === 'alt') keys.clear(); });
addEventListener('blur', releaseAll);
document.addEventListener('visibilitychange', () => { if (document.hidden) releaseAll(); });
addEventListener('pagehide', releaseAll);
const cv = $('#tank');
const aim = (e) => { pointer = { x: e.clientX, y: e.clientY, w: screenToWorld(e.clientX, e.clientY) }; pointerKind = e.pointerType; };
cv.addEventListener('pointermove', (e) => { if (e.pointerType === 'mouse' || pointerDown) aim(e); });
cv.addEventListener('pointerdown', (e) => { pointerDown = true; aim(e); try { cv.setPointerCapture(e.pointerId); } catch {} closeMenu(); });
const endPointer = (e) => { pointerDown = false; if (e.pointerType !== 'mouse' || e.type !== 'pointerup') { pointer = null; sendZero(); } };
cv.addEventListener('pointerup', endPointer); cv.addEventListener('pointercancel', endPointer);
cv.addEventListener('pointerleave', (e) => { if (!pointerDown) { pointer = null; sendZero(); } });
cv.addEventListener('lostpointercapture', (e) => { if (e.pointerType !== 'mouse') { pointerDown = false; pointer = null; sendZero(); } });
function sendZero() { if (!joined) return; lastSent = { x: 0, y: 0, at: Date.now() }; try { send({ t: 'in', x: 0, y: 0 }); } catch {} }
function steer(me) {
  let x = 0, y = 0;
  if (keys.has('arrowleft') || keys.has('a')) x -= 1; if (keys.has('arrowright') || keys.has('d')) x += 1;
  if (keys.has('arrowup') || keys.has('w')) y -= 1; if (keys.has('arrowdown') || keys.has('s')) y += 1;
  if (x || y) { const m = Math.hypot(x, y); x /= m; y /= m; }
  else if (pointer && me) {
    const w = pointer.w; const dx = w.x - me.x, dy = w.y - me.y, d = Math.hypot(dx, dy);
    const dead = 12, full = pointerKind === 'mouse' ? 160 : 120;
    if (d > dead) { const k = Math.min(1, (d - dead) / full); x = dx / d * k; y = dy / d * k; }
  }
  x = Math.round(x * 20) / 20; y = Math.round(y * 20) / 20;
  const t = Date.now();
  if (x !== lastSent.x || y !== lastSent.y ? t - lastSent.at > 45 : t - lastSent.at > 500) { lastSent = { x, y, at: t }; send({ t: 'in', x, y }); }
}

/* ---------------- render ---------------- */
const ctx = cv.getContext('2d');
let VW = 0, VH = 0, DPR = 1, cam = { x: WORLD_W / 2, y: WORLD_H / 2, z: 1 };
function resize() { DPR = Math.min(2, devicePixelRatio || 1); VW = innerWidth; VH = innerHeight; cv.width = VW * DPR; cv.height = VH * DPR; cv.style.width = VW + 'px'; cv.style.height = VH + 'px'; }
addEventListener('resize', resize); resize();
const screenToWorld = (sx, sy) => ({ x: (sx - VW / 2) / cam.z + cam.x, y: (sy - VH / 2) / cam.z + cam.y });

const bubbles = Array.from({ length: 70 }, () => ({ x: rand(0, WORLD_W), y: rand(0, WORLD_H), r: rand(3, 11), s: rand(25, 70), w: rand(0, 6) }));
const weeds = Array.from({ length: 22 }, (_, i) => ({ x: (i + 0.5) * WORLD_W / 22 + rand(-30, 30), h: rand(90, 260), c: ['#2EDB7A', '#00C2A8', '#7CF25A', '#1FB86A'][i % 4], ph: rand(0, 6) }));
const rocks = Array.from({ length: 9 }, () => ({ x: rand(0, WORLD_W), r: rand(25, 60), c: ['#FF6A00', '#FF2E88', '#A970FF', '#00E5FF'][Math.floor(rand(0, 4))] }));
let dotPat = null;
function dots() {
  if (dotPat) return dotPat; const c = document.createElement('canvas'); c.width = c.height = 14; const g = c.getContext('2d');
  g.fillStyle = 'rgba(255,255,255,.13)'; g.beginPath(); g.arc(7, 7, 2.2, 0, 7); g.fill(); dotPat = ctx.createPattern(c, 'repeat'); return dotPat;
}

function interp() {
  const s = V.snaps; if (!s.length) return null;
  const rt = now() - INTERP_MS;
  let a = s[0], b = s[0];
  for (let i = 0; i < s.length - 1; i++) if (s[i].at <= rt && s[i + 1].at >= rt) { a = s[i]; b = s[i + 1]; break; }
  if (rt > s[s.length - 1].at) { a = b = s[s.length - 1]; }
  const k = b === a ? 1 : clamp((rt - a.at) / (b.at - a.at), 0, 1);
  const am = new Map(a.p.map((p) => [p[0], p]));
  const players = b.p.map((pb) => {
    const pa = am.get(pb[0]) || pb;
    return { id: pb[0], x: pa[1] + (pb[1] - pa[1]) * k, y: pa[2] + (pb[2] - pa[2]) * k, size: pb[3], score: pb[4], f: pb[5], a: pa[6] + (pb[6] - pa[6]) * k, vx: pb[7], vy: pb[8] };
  });
  const af = new Map(a.fd.map((f) => [f[0], f]));
  const foods = b.fd.map((fb) => { const fa = af.get(fb[0]) || fb; return { id: fb[0], x: fa[1] + (fb[1] - fa[1]) * k, y: fa[2] + (fb[2] - fa[2]) * k, k: fb[3], fade: fb[4] }; });
  return { players, foods };
}
const eatenIds = new Set();
function onEat(e) {
  const [fid, pid, x, y, k] = e; if (eatenIds.has(fid)) return; eatenIds.add(fid); if (eatenIds.size > 500) eatenIds.clear();
  V.eats.push({ fid, pid, x, y, k, t: now() });
  const fv = V.fish.get(pid); if (fv) { fv.snap = now(); fv.vs += 2.6; }
}
function fishView(id) { let f = V.fish.get(id); if (!f) { f = { ds: 1, vs: 0, mouth: 0, snap: 0, wig: rand(0, 6) }; V.fish.set(id, f); } return f; }

let shake = 0, lastFrame = now(), boardAt = 0, urgent = false;
function frame() {
  const t = now(); const dt = Math.min(0.05, (t - lastFrame) / 1000); lastFrame = t;
  const st = interp();
  const players = st ? st.players : [], foods = st ? st.foods.filter((f) => !eatenIds.has(f.id)) : [];
  const me = players.find((p) => p.id === myId);
  if (joined) steer(me);
  // urgency
  const set = V.meta.set || DEFAULTS;
  const maxScore = Math.max(0, ...players.map((p) => p.score));
  const u = joined && V.ph === 'play' && ((set.mode === 'timer' && V.tl <= 10000) || (set.mode === 'goal' && maxScore >= 0.75 * set.goal));
  if (u !== urgent) { urgent = u; document.body.classList.toggle('urgent', u); }
  // camera
  const zFit = Math.min(VW / WORLD_W, VH / WORLD_H);
  const zTarget = joined ? Math.max(0.42, Math.min(VW / 1150, VH / 760)) : Math.max(zFit, Math.min(VW / WORLD_W, VH / WORLD_H) * 1.0);
  cam.z += (zTarget - cam.z) * Math.min(1, dt * 4);
  let tx = WORLD_W / 2, ty = WORLD_H / 2; if (me) { tx = me.x; ty = me.y; }
  const hw = VW / 2 / cam.z, hh = VH / 2 / cam.z;
  tx = hw * 2 >= WORLD_W ? WORLD_W / 2 : clamp(tx, hw, WORLD_W - hw); ty = hh * 2 >= WORLD_H ? WORLD_H / 2 : clamp(ty, hh, WORLD_H - hh);
  cam.x += (tx - cam.x) * Math.min(1, dt * 6); cam.y += (ty - cam.y) * Math.min(1, dt * 6);
  if (!me || Math.abs(cam.x - tx) > 600) { cam.x = tx; cam.y = ty; }
  shake = urgent && !REDUCED ? 3 + 2 * Math.sin(t / 90) : 0;
  const sx = shake ? rand(-shake, shake) : 0, sy = shake ? rand(-shake, shake) : 0;

  ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
  ctx.fillStyle = '#0B2A5B'; ctx.fillRect(0, 0, VW, VH);
  ctx.setTransform(DPR * cam.z, 0, 0, DPR * cam.z, DPR * (VW / 2 - cam.x * cam.z + sx), DPR * (VH / 2 - cam.y * cam.z + sy));
  drawTank(t, dt);
  drawFood(foods, t);
  // fish (smaller first so big fish sit on top), names after
  const sorted = players.slice().sort((a, b) => a.size - b.size);
  for (const p of sorted) drawFish(p, foods, t, dt);
  drawEats(players, t);
  for (const p of sorted) drawName(p);
  if (urgent) { ctx.setTransform(DPR, 0, 0, DPR, 0, 0); const a = 0.10 + 0.08 * Math.sin(t / 160); ctx.fillStyle = `rgba(255,46,136,${a})`; ctx.fillRect(0, 0, VW, VH); }
  // HUD text
  updateHud(players, t);
  if (t - boardAt > 150 && joined) { boardAt = t; renderBoard(players); }
  requestAnimationFrame(frame);
}
function drawTank(t, dt) {
  const g = ctx.createLinearGradient(0, 0, 0, WORLD_H);
  g.addColorStop(0, '#2FE6FF'); g.addColorStop(0.45, '#08A6E0'); g.addColorStop(1, '#0B4FA8');
  ctx.fillStyle = g; ctx.fillRect(0, 0, WORLD_W, WORLD_H);
  ctx.fillStyle = dots(); ctx.fillRect(0, 0, WORLD_W, WORLD_H);
  // light rays
  ctx.save(); ctx.globalCompositeOperation = 'lighter';
  for (let i = 0; i < 7; i++) {
    const x = (i * 330 + Math.sin(t / 3000 + i) * 60) % (WORLD_W + 300) - 150;
    ctx.fillStyle = `rgba(255,255,255,${0.05 + 0.03 * Math.sin(t / 1300 + i * 2)})`;
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x + 110, 0); ctx.lineTo(x + 360, WORLD_H); ctx.lineTo(x + 200, WORLD_H); ctx.fill();
  }
  ctx.restore();
  // surface
  ctx.fillStyle = 'rgba(255,255,255,.55)'; ctx.beginPath(); ctx.moveTo(0, 0);
  for (let x = 0; x <= WORLD_W; x += 40) ctx.lineTo(x, 8 + Math.sin(x / 70 + t / 500) * 5);
  ctx.lineTo(WORLD_W, 0); ctx.fill();
  // bubbles
  const bs = urgent ? 2.6 : 1;
  ctx.lineWidth = 2.5; ctx.strokeStyle = 'rgba(255,255,255,.85)'; ctx.fillStyle = 'rgba(255,255,255,.18)';
  for (const b of bubbles) {
    b.y -= b.s * bs * dt; if (b.y < -20) { b.y = WORLD_H - SAND_H; b.x = rand(0, WORLD_W); }
    const x = b.x + Math.sin(t / 600 + b.w) * 8;
    ctx.beginPath(); ctx.arc(x, b.y, b.r, 0, 7); ctx.fill(); ctx.stroke();
  }
  // seaweed
  ctx.lineCap = 'round';
  for (const w of weeds) {
    const sway = Math.sin(t / 900 + w.ph) * 22, base = WORLD_H - SAND_H + 20;
    for (const [col, lw] of [['#111', 20], [w.c, 12]]) {
      ctx.strokeStyle = col; ctx.lineWidth = lw; ctx.beginPath(); ctx.moveTo(w.x, base);
      ctx.quadraticCurveTo(w.x + sway * 1.6, base - w.h * 0.5, w.x + sway, base - w.h); ctx.stroke();
    }
  }
  // sand + rocks
  ctx.fillStyle = '#FFD86B'; ctx.strokeStyle = '#111'; ctx.lineWidth = 5;
  ctx.beginPath(); ctx.moveTo(0, WORLD_H);
  for (let x = 0; x <= WORLD_W; x += 50) ctx.lineTo(x, WORLD_H - SAND_H + Math.sin(x / 140) * 10);
  ctx.lineTo(WORLD_W, WORLD_H); ctx.closePath(); ctx.fill(); ctx.stroke();
  for (const r of rocks) { ctx.fillStyle = r.c; ctx.beginPath(); ctx.ellipse(r.x, WORLD_H - SAND_H + 18, r.r, r.r * 0.55, 0, Math.PI, 0); ctx.fill(); ctx.stroke(); }
  // tank walls
  ctx.strokeStyle = '#111'; ctx.lineWidth = 10; ctx.strokeRect(0, 0, WORLD_W, WORLD_H);
}
function flakePath(r, seed) {
  ctx.beginPath();
  for (let i = 0; i < 6; i++) { const a = i / 6 * Math.PI * 2; const rr = r * (0.75 + 0.35 * Math.abs(Math.sin(seed * 7.3 + i * 2.1))); i ? ctx.lineTo(Math.cos(a) * rr, Math.sin(a) * rr) : ctx.moveTo(Math.cos(a) * rr, Math.sin(a) * rr); }
  ctx.closePath();
}
function drawFlake(x, y, k, rot, scale, seed, alpha, t) {
  ctx.save(); ctx.translate(x, y); ctx.rotate(rot); ctx.scale(scale, scale * (0.65 + 0.35 * Math.abs(Math.sin(t / 180 + seed)))); ctx.globalAlpha = alpha;
  flakePath(FOOD_R * (k === 2 ? 1.15 : 1), seed);
  ctx.fillStyle = k === 2 ? '#FFC21A' : '#FF5FA8'; ctx.fill(); ctx.lineWidth = 2.5; ctx.strokeStyle = '#111'; ctx.stroke();
  ctx.fillStyle = 'rgba(255,255,255,.75)'; ctx.beginPath(); ctx.arc(-2.5, -2.5, 2.4, 0, 7); ctx.fill();
  ctx.restore();
  if (k === 2 && alpha > 0.5) { // sparkle
    const s = 5 + 3 * Math.sin(t / 120 + seed); ctx.save(); ctx.translate(x + 10, y - 10); ctx.fillStyle = '#fff'; ctx.strokeStyle = '#111'; ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.moveTo(0, -s); ctx.lineTo(s * 0.3, 0); ctx.lineTo(0, s); ctx.lineTo(-s * 0.3, 0); ctx.closePath(); ctx.fill(); ctx.stroke(); ctx.restore();
  }
}
function drawFood(foods, t) {
  for (const f of foods) drawFlake(f.x, f.y, f.k, t / 400 + f.id, 1, f.id, clamp(f.fade, 0, 1), t);
}
function localMouth(p, ds) {
  const s = speciesOf((V.meta.pl[p.id] || [])[1]);
  const w = FISH_LEN * ds, h = FISH_HT * ds * fatOf(ds);
  return { lx: ((s.mouth[0] + 14) / 268 - 0.5) * w, ly: ((s.mouth[1] + 14) / 208 - 0.5) * h, w, h };
}
function drawFish(p, foods, t, dt) {
  const m = V.meta.pl[p.id]; if (!m) return;
  const fv = fishView(p.id);
  // bouncy grow spring toward real size
  fv.vs += (p.size - fv.ds) * 160 * dt; fv.vs *= Math.exp(-9 * dt); fv.ds += fv.vs * dt;
  if (fv.ds < 0.5) fv.ds = 0.5;
  const ds = fv.ds;
  const { lx, ly, w, h } = localMouth(p, ds);
  // world mouth
  const ca = Math.cos(p.a), sa = Math.sin(p.a);
  const mx = p.x + (lx * ca - ly * sa) * p.f, my = p.y + (lx * sa + ly * ca);
  // mouth opens only when a flake is right about to touch it
  let near = 1e9; for (const f of foods) { const d = Math.hypot(f.x - mx, f.y - my); if (d < near) near = d; }
  const er = eatRadius(p.size), openR = er + 28;
  let want = near < openR ? clamp(1 - (near - er) / (openR - er), 0, 1) : 0;
  const since = t - fv.snap; if (since < 220) want = since < 90 ? 1 : 0; // snap: wide, then chomp shut
  fv.mouth += (want - fv.mouth) * Math.min(1, dt * (since < 220 ? 30 : 14));
  const spr = window.FinFish.sprite(m[1], m[2]);
  const speed = Math.hypot(p.vx, p.vy);
  fv.wig += dt * (3 + speed / 40);
  ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.a); ctx.scale(p.f, 1);
  const squash = since < 220 ? 1 + 0.06 * Math.sin(since / 220 * Math.PI) : 1;
  ctx.scale(1 + Math.sin(fv.wig) * 0.025, squash);
  if (p.id === myId) { ctx.fillStyle = 'rgba(255,230,0,.35)'; ctx.beginPath(); ctx.ellipse(0, 0, w * 0.48, h * 0.45, 0, 0, 7); ctx.fill(); }
  if (spr) ctx.drawImage(spr, -w / 2, -h / 2, w, h);
  else { ctx.fillStyle = (COLORS.find((c) => c.id === m[2]) || COLORS[0]).hex; ctx.beginPath(); ctx.ellipse(0, 0, w * 0.35, h * 0.3, 0, 0, 7); ctx.fill(); }
  if (fv.mouth > 0.04) {
    const o = fv.mouth, sc = ds;
    ctx.translate(lx - 3 * sc, ly);
    ctx.fillStyle = '#111'; ctx.beginPath(); ctx.ellipse(0, 0, (4 + 6 * o) * sc, (2 + 9 * o) * sc, 0, 0, 7); ctx.fill();
    ctx.fillStyle = '#FF2E88'; ctx.beginPath(); ctx.ellipse(-1 * sc, 3 * o * sc, (2 + 3.5 * o) * sc, (1 + 4 * o) * sc, 0, 0, 7); ctx.fill();
    ctx.strokeStyle = '#111'; ctx.lineWidth = 2.5 * sc; ctx.beginPath(); ctx.ellipse(0, 0, (4 + 6 * o) * sc, (2 + 9 * o) * sc, 0, 0, 7); ctx.stroke();
  }
  ctx.restore();
  p._mx = mx; p._my = my; p._h = h;
}
function drawEats(players, t) {
  const byId = new Map(players.map((p) => [p.id, p]));
  V.eats = V.eats.filter((e) => t - e.t < 900);
  for (const e of V.eats) {
    const p = byId.get(e.pid); const age = t - e.t;
    if (p && age < 160) { // suck the flake into the mouth
      const k = age / 160, x = e.x + ((p._mx || p.x) - e.x) * k * k, y = e.y + ((p._my || p.y) - e.y) * k * k;
      drawFlake(x, y, e.k, age / 30, 1 - k * 0.8, e.fid, 1, t);
    }
    if (p) { // +1 / +2 pop
      const k = age / 900; ctx.save(); ctx.globalAlpha = 1 - k; ctx.font = `700 ${22 + 6 * (e.k - 1)}px Bungee, Fredoka, sans-serif`; ctx.textAlign = 'center';
      ctx.lineWidth = 5; ctx.strokeStyle = '#111'; ctx.fillStyle = e.k === 2 ? '#FFC21A' : '#FF5FA8';
      const y = (p._my || p.y) - 20 - k * 50; ctx.strokeText('+' + e.k, p._mx || p.x, y); ctx.fillText('+' + e.k, p._mx || p.x, y); ctx.restore();
      if (age < 200) { ctx.save(); ctx.strokeStyle = '#fff'; ctx.lineWidth = 3; const r = 8 + age / 8; for (let i = 0; i < 6; i++) { const a = i * Math.PI / 3; ctx.beginPath(); ctx.moveTo((p._mx || p.x) + Math.cos(a) * r, (p._my || p.y) + Math.sin(a) * r); ctx.lineTo((p._mx || p.x) + Math.cos(a) * (r + 7), (p._my || p.y) + Math.sin(a) * (r + 7)); ctx.stroke(); } ctx.restore(); }
    }
  }
}
function drawName(p) {
  const m = V.meta.pl[p.id]; if (!m) return;
  const fv = V.fish.get(p.id); const h = (p._h || FISH_HT) ; const y = p.y - h * 0.42 - 8;
  const mine = p.id === myId;
  const label = mine ? 'YOU · ' + m[0] : m[0];
  ctx.save(); ctx.font = `700 ${mine ? 17 : 15}px Fredoka, sans-serif`; ctx.textAlign = 'center';
  const tw = ctx.measureText(label).width + 14;
  ctx.fillStyle = mine ? '#FFE600' : 'rgba(255,255,255,.92)'; ctx.strokeStyle = '#111'; ctx.lineWidth = 2.5;
  ctx.beginPath(); ctx.roundRect ? ctx.roundRect(p.x - tw / 2, y - 18, tw, 22, 6) : ctx.rect(p.x - tw / 2, y - 18, tw, 22); ctx.fill(); ctx.stroke();
  ctx.fillStyle = '#111'; ctx.fillText(label, p.x, y - 1);
  if (V.meta.ph === 'lobby' && m[3]) { ctx.fillStyle = '#2EDB7A'; ctx.beginPath(); ctx.arc(p.x + tw / 2 + 2, y - 16, 8, 0, 7); ctx.fill(); ctx.stroke(); ctx.fillStyle = '#111'; ctx.font = '700 11px Fredoka'; ctx.fillText('✓', p.x + tw / 2 + 2, y - 12); }
  ctx.restore();
  void fv;
}
let hudKey = '';
function updateHud(players, t) {
  if (!joined) return;
  const set = V.meta.set || DEFAULTS; const ph = V.ph;
  let phase = 'FREE SWIM', timer = '';
  if (ph === 'lobby') { phase = 'FREE SWIM · ロビー'; timer = set.mode === 'timer' ? `${set.min}:00 TIMER` : `GOAL ${set.goal} PTS`; }
  else if (ph === 'countdown') { phase = 'GET READY'; timer = set.mode === 'timer' ? `${set.min}:00` : `GOAL ${set.goal}`; const n = Math.ceil(V.cd / 1000); if (n > 0) popCountdown(String(n)); }
  else if (ph === 'play') {
    phase = 'FEEDING TIME!';
    if (set.mode === 'timer') { const s = Math.ceil(V.tl / 1000); timer = Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); }
    else { const top = Math.max(0, ...players.map((p) => p.score)); timer = `${top}/${set.goal} PTS`; }
  } else if (ph === 'end') { phase = 'ROUND OVER'; timer = 'FINISH!'; }
  const key = phase + timer + urgent;
  if (key !== hudKey) { hudKey = key; $('#phaseText').textContent = phase; $('#timerPill').textContent = timer; $('#timerPill').classList.toggle('blink', urgent); }
}

/* ---------------- copy helpers / wiring ---------------- */
function toast(msg) { const tt = $('#toast'); tt.textContent = msg; tt.classList.add('show'); clearTimeout(toast.t); toast.t = setTimeout(() => tt.classList.remove('show'), 1600); }
async function copy(text, label) {
  try { await navigator.clipboard.writeText(text); }
  catch { const ta = el('textarea'); ta.value = text; document.body.append(ta); ta.select(); try { document.execCommand('copy'); } catch {} ta.remove(); }
  toast(label + ' copied!');
}
const inviteLink = () => location.origin + location.pathname + '?room=' + room + (LOCAL_BUS ? '&local=1' : '');
$('#copyCodeBtn').onclick = $('#pickCode').onclick = $('#menuCopy').onclick = () => copy(room, 'Room code ' + room);
$('#copyLinkBtn').onclick = $('#menuLink').onclick = () => copy(inviteLink(), 'Invite link');
$('#createBtn').onclick = () => createRoom();
$('#joinForm').onsubmit = (e) => { e.preventDefault(); const c = cleanCode($('#codeInput').value); if (c.length < 4) { $('#homeError').textContent = 'Room codes are 5 letters.'; return; } joinRoom(c); };
$('#codeInput').addEventListener('input', (e) => { e.target.value = cleanCode(e.target.value); });
$('#diveBtn').onclick = dive;
$('#nameInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') dive(); });
$('#readyBtn').onclick = () => send({ t: 'ready', v: !readyLocal });
$('#startBtn').onclick = () => hostStart();
$('#againBtn').onclick = () => hostStart();
$('#lobbyBtn').onclick = () => hostToLobby();
$('#menuBtn').onclick = (e) => { e.stopPropagation(); const m = $('#menu'); m.hidden = !m.hidden; if (!m.hidden) releaseAll(); $('#menuBtn').setAttribute('aria-expanded', String(!m.hidden)); };
$('#menu').addEventListener('pointerdown', (e) => e.stopPropagation());
document.addEventListener('pointerdown', (e) => { if (!$('#menu').hidden && !e.target.closest('#menu') && !e.target.closest('#menuBtn')) closeMenu(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !$('#menu').hidden) { closeMenu(); $('#menuBtn').focus(); } });
document.querySelectorAll('#modeSeg button').forEach((b) => { b.onclick = () => hostSet({ mode: b.dataset.mode }); });
document.querySelectorAll('#foodSeg button').forEach((b) => { b.onclick = () => hostSet({ food: b.dataset.food }); });
$('#timerStep').addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) hostSet({ mode: 'timer', dmin: +b.dataset.d }); });
$('#goalStep').addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) hostSet({ mode: 'goal', dgoal: +b.dataset.d }); });
$('#startOverBtn').onclick = () => { hostToLobby(); closeMenu(); toast('Back to the lobby'); };
$('#boardToggle').onclick = () => setBoardCollapsed(!$('#board').classList.contains('collapsed'));
function quit() {
  leaving = true;
  if (role === 'host') sendAll({ t: 'bye' }); else send({ t: 'bye' });
  setTimeout(() => { destroyPeer(); location.href = location.pathname + (LOCAL_BUS ? '?local=1' : ''); }, 150);
}
$('#quitBtn').onclick = quit;
$('#msgBtn').onclick = () => { leaving = true; destroyPeer(); location.href = location.pathname + (LOCAL_BUS ? '?local=1' : ''); };
addEventListener('pagehide', () => { leaving = true; if (role === 'host') sendAll({ t: 'bye' }); else if (hostConn) try { hostConn.send({ t: 'bye' }); } catch {} destroyPeer(); });
/* ---------------- boot ---------------- */
window.FinFish.loadAll('assets/fish/').then(() => {
  SPECIES.forEach((s) => COLORS.forEach((c) => window.FinFish.sprite(s.id, c.id)));
  const code = cleanCode(QS.get('room'));
  if (code.length >= 4) { $('#codeInput').value = code; showHome(); joinRoom(code); showPickWhenConnected(); }
  else showHome();
});
function showPickWhenConnected() {
  const iv = setInterval(() => { if (lastHostMsg) { clearInterval(iv); $('#homeError').textContent = ''; showPick(); } else if (role !== 'client') clearInterval(iv); }, 100);
}
$('#joinForm').addEventListener('submit', showPickWhenConnected);
requestAnimationFrame(frame);
if (QS.has('test')) window.__finops = { V, H, cam, w2s: (x, y) => ({ x: (x - cam.x) * cam.z + VW / 2, y: (y - cam.y) * cam.z + VH / 2 }), get role() { return role; }, get myId() { return myId; }, get room() { return room; }, hostSet, hostStart };
})();
