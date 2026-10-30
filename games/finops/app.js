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
const TICK_MS = 33, SNAP_EVERY = 2, INTERP_MAX = 150, INTERP_MIN = 50, HOST_INTERP = 45;
const FOOD_R = 9, PILL_R = 18;
// flakes / second (scaled by player count). Round 2: 20% less food at every level (was 1.1 / 2.1 / 3.6).
const FOOD_RATE = { low: 0.88, med: 1.68, high: 2.88 };
const DEFAULTS = { mode: 'timer', min: 1, goal: 20, food: 'med', pw: true };
// Magic pill power-up: one at a time, every 25-40 s at random; powered fish is 2x with metal jaws for up to 15 s.
const PILL_GAP = [25000, 40000], PW_MS = 15000, PW_WARN = 3000;
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
// Mouse steering is off by default (keyboard only); touch steering stays on so phones are playable. ?mouse=1 re-enables it.
const MOUSE_ON = QS.get('mouse') === '1';
const TOUCH = matchMedia('(pointer: coarse)').matches || 'ontouchstart' in window;

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
    const key = m.from + '|' + (m.l || '');
    if (m.k === 'open') { const c = this.mk(m.from, m.l); this.bc.postMessage({ k: 'ack', to: m.from, from: this.id, l: m.l }); this.emit('connection', c); setTimeout(() => { c.open = true; c.emit('open'); }, 0); }
    else if (m.k === 'ack') { const c = this.conns[key]; if (c && !c.open) { c.open = true; clearTimeout(c.t); c.emit('open'); } }
    else if (m.k === 'd') { const c = this.conns[key]; if (c) c.emit('data', m.d); }
    else if (m.k === 'x') { const c = this.conns[key]; if (c) { delete this.conns[key]; c.emit('close'); } }
  }
  mk(other, l = '') {
    const c = new Emitter(); c.peer = other; c.open = false; c.label = l; const key = other + '|' + l;
    c.send = (d) => { if (!this.destroyed) this.bc.postMessage({ k: 'd', to: other, from: this.id, l, d }); };
    c.close = () => { if (this.conns[key]) { delete this.conns[key]; this.bc.postMessage({ k: 'x', to: other, from: this.id, l }); } };
    this.conns[key] = c; return c;
  }
  connect(id, o = {}) { const l = o.label || ''; const c = this.mk(id, l); this.bc.postMessage({ k: 'open', to: id, from: this.id, l }); c.t = setTimeout(() => { if (!l) this.emit('error', { type: 'peer-unavailable' }); }, 2500); return c; }
  destroy() { if (this.destroyed) return; Object.values(this.conns).forEach((c) => c.close()); this.destroyed = true; this.bc.close(); }
}
// The probe trick above needs the owner to answer; mark opened once claim succeeds.
const makePeer = (id) => {
  if (LOCAL_BUS) { const p = new LocalPeer(id); p.on('open', () => { p.opened = true; }); return p; }
  return id ? new Peer(id, peerOpts) : new Peer(peerOpts);
};

/* ---------------- state ---------------- */
let peer = null, role = null, room = '', myId = null, hostConn = null, joined = false, leaving = false;
let lastHostMsg = 0, uConn = null;
const H = { // host-only state
  conns: new Map(),   // pid -> {c, u, lastSeen}  (c = reliable/ordered channel, u = unordered channel for snapshots + input)
  byPeer: new Map(),  // peer id -> pid
  players: new Map(), // pid -> player
  foods: [], nextFood: 1, nextPid: 1, set: { ...DEFAULTS }, ph: 'lobby', cdEnd: 0, playStart: 0,
  ev: [], tick: 0, spawnAcc: 0, metaVer: 0, result: null, timer: null, lastT: 0, pill: null, nextPill: 0,
};
// client view state
const V = { meta: { pl: {}, set: { ...DEFAULTS }, ph: 'lobby', host: 0, n: 0 }, snaps: [], fish: new Map(), eats: [], fx: [], splats: [], ph: 'lobby', tl: 0, cd: 0, result: null,
  seq: -1, lastAt: 0, gapM: 66, gapD: 8, idelay: 100, rtt: 0, pill: null };

/* ---------------- host: sim ---------------- */
function hostPlayerList() { return [...H.players.values()]; }
function spawnPos(bottom) {
  return bottom ? { x: rand(120, WORLD_W - 120), y: WORLD_H - SAND_H - 50 } : { x: rand(200, WORLD_W - 200), y: rand(250, WORLD_H - 250) };
}
function addPlayer(pid, info) {
  const late = H.ph === 'countdown' || H.ph === 'play';
  const p = { id: pid, name: info.name, sp: info.sp, c: info.c, ready: false, score: 0, eats: 0, size: 1, base: 1, pw: 0, out: false, ...spawnPos(late), vx: 0, vy: 0, ax: 0, ay: 0, f: 1, a: 0, lastScore: 0, inQ: -1 };
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
function sendSnap(m) { for (const e of H.conns.values()) { const ch = e.u && e.u.open ? e.u : e.c; if (ch.open) try { ch.send(m); } catch {} } }
function fxAll(m) { m.t = 'fx'; sendAll(m); onClientData(m); } // reliable one-off effects (pill swallowed, chomp, power ended)

function resetRound() {
  for (const p of H.players.values()) {
    if (p.out) Object.assign(p, spawnPos(false), { vx: 0, vy: 0 }); // eaten fish respawn for the new round
    p.score = 0; p.eats = 0; p.size = 1; p.base = 1; p.pw = 0; p.out = false; p.lastScore = 0;
  }
  H.foods = []; H.result = null; H.pill = null; H.endAt = 0; H.nextPill = 0;
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
  // Everyone is ranked by score, including eaten fish (their score is locked in when they're eaten).
  const ranked = hostPlayerList().sort((a, b) => b.score - a.score || a.lastScore - b.lastScore);
  H.result = ranked.map((p) => [p.id, p.score, p.out ? 1 : 0]);
  for (const p of H.players.values()) if (p.pw) { p.pw = 0; p.size = p.base; }
  H.ph = 'end'; H.foods = []; H.pill = null; bumpMeta();
}
function timeLeft() {
  if (H.ph !== 'play' || H.set.mode !== 'timer') return H.set.min * 60000;
  return Math.max(0, H.playStart + H.set.min * 60000 - Date.now());
}
function mouthOf(p) {
  const s = speciesOf(p.sp);
  const w = FISH_LEN * p.size, h = FISH_HT * p.size * fatOf(p.size);
  let lx = ((s.mouth[0] + 14) / 268 - 0.5) * w, ly = ((s.mouth[1] + 14) / 208 - 0.5) * h;
  if (p.pw) ({ lx, ly } = sharkMouth(FISH_LEN * p.size, FISH_HT * p.size));
  const ca = Math.cos(p.a), sa = Math.sin(p.a);
  const rx = lx * ca - ly * sa, ry = lx * sa + ly * ca;
  return { x: p.x + rx * p.f, y: p.y + ry };
}
const fatOf = (size) => 1 + (size - 1) * 0.2;
const radiusOf = (size) => 30 * size;
const SHARK_X = 2; // great white = 2x the fish's normal size
// shark snout: front of the body, a touch below centre (fish-local, before flip/tilt)
const sharkMouth = (w, h) => ({ lx: 0.43 * w, ly: 0.1 * h * 0.6 });
const eatRadius = (size) => FOOD_R + 8 + 4 * size;
// suction: a cone in front of the mouth that captures flakes, then pulls them in over CAP_MS
const suckRadius = (size) => eatRadius(size) * 1.9;
const SUCK_COS = Math.cos(60 * Math.PI / 180); // wide ~120° cone around the facing direction
const CAP_MS = REDUCED ? 90 : 150;
const faceOf = (p) => ({ x: Math.cos(p.a) * p.f, y: Math.sin(p.a) });
// forgiving capture: anything whose own radius overlaps the mouth (drawn mouth position, same on host + clients)
// is sucked in from any side; a little extra reach straight ahead of the face. No vacuuming from afar.
const lipRadius = (size) => 12 + 7 * size;
const PILL_CAP_R = 26; // the pill's drawn capsule half-length (PILL_R * 1.45 scale)
function inSuction(p, m, f, itemR = FOOD_R) {
  const dx = f.x - m.x, dy = f.y - m.y, d = Math.hypot(dx, dy), touch = lipRadius(p.size) + itemR;
  if (d <= touch) return true;
  if (d > touch * 1.35) return false;
  const fc = faceOf(p); return (dx * fc.x + dy * fc.y) / (d || 1) >= SUCK_COS;
}

// Fish movement shared by the host simulation and the client's own-fish prediction (same numbers on both sides).
function moveFish(p, dt) {
  const mag = Math.hypot(p.ax, p.ay); let ax = p.ax, ay = p.ay; if (mag > 1) { ax /= mag; ay /= mag; }
  const maxV = 340 / Math.pow(p.base || p.size, 0.3), acc = 1100;
  p.vx += ax * acc * dt; p.vy += ay * acc * dt;
  const drag = Math.exp(-(mag > 0.05 ? 1.6 : 3.2) * dt); p.vx *= drag; p.vy *= drag;
  const sp = Math.hypot(p.vx, p.vy); if (sp > maxV) { p.vx *= maxV / sp; p.vy *= maxV / sp; }
  p.x += p.vx * dt; p.y += p.vy * dt;
  if (p.vx > 25) p.f = 1; else if (p.vx < -25) p.f = -1;
  const tilt = clamp(Math.atan2(p.vy, Math.abs(p.vx) + 60) * 0.7, -0.55, 0.55) * p.f;
  p.a += (tilt - p.a) * Math.min(1, dt * 8);
}
function wallFish(p) {
  const r = radiusOf(p.size);
  if (p.x < r) { p.x = r; p.vx = Math.abs(p.vx) * 0.5; } if (p.x > WORLD_W - r) { p.x = WORLD_W - r; p.vx = -Math.abs(p.vx) * 0.5; }
  const top = r * 0.8 + 10, bot = WORLD_H - SAND_H + 10 - r * 0.8;
  if (p.y < top) { p.y = top; p.vy = Math.abs(p.vy) * 0.5; } if (p.y > bot) { p.y = bot; p.vy = -Math.abs(p.vy) * 0.5; }
}
// falling + fluttering + shy of nearby mouths (flakes and the pill)
function moveFlake(f, dt, mouths, shy = 1) {
  f.t += dt; f.dart -= dt;
  if (f.dart <= 0) { f.dart = rand(0.5, 1.6); f.dvx = rand(-1, 1) * rand(60, 140) * shy; }
  f.dvx *= Math.exp(-2.5 * dt);
  let ev = 0, evy = 0, near = false;
  for (const [, m] of mouths) {
    const dx = f.x - m.x, dy = f.y - m.y, d = Math.hypot(dx, dy);
    if (d < 140) near = true;
    if (d < 95 * shy && d > 40) { const k = (1 - d / (95 * shy)) * 120 * shy; ev += dx / d * k; evy += dy / d * k * 0.4; }
  }
  if (near) { ev *= 0.25; evy *= 0.25; f.dvx *= Math.exp(-6 * dt); } // close to a mouth: barely dodges, so it's catchable
  f.x += (f.drift + f.dvx + ev + Math.cos(f.t * f.wf + f.ph) * f.wob * f.wf * 0.5) * dt;
  f.y += (f.vy + evy) * dt;
  f.x = clamp(f.x, 15, WORLD_W - 15);
  if (f.y > WORLD_H - SAND_H + 5) f.fade -= dt * 1.5;
}
function spawnPill() {
  H.pill = { id: H.nextFood++, x: rand(160, WORLD_W - 160), y: -20, vy: rand(55, 80), drift: rand(-20, 20),
    wob: rand(30, 60), wf: rand(1.2, 2.4), ph: rand(0, 6.28), dart: rand(0.4, 1.2), dvx: 0, fade: 1, t: 0 };
}
const pillGap = () => rand(PILL_GAP[0], PILL_GAP[1]);
function powerUp(p) { p.pw = Date.now() + PW_MS; p.size = p.base * SHARK_X; fxAll({ k: 'pill', pid: p.id }); }
function powerDown(p, why) { if (!p.pw) return; p.pw = 0; p.size = p.base; fxAll({ k: 'unpw', pid: p.id, why }); }
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
  if (H.ph === 'play' && !H.nextPill) H.nextPill = Date.now() + pillGap();
  const all = hostPlayerList(), players = all.filter((p) => !p.out); // eaten fish are out of the water
  // fish movement
  const nowMs = Date.now();
  for (const p of players) {
    if (p.id !== 0 && nowMs - (p.inAt || 0) > INPUT_TIMEOUT_MS) { p.ax = 0; p.ay = 0; } // stalled client: stop steering, drag glides it to a stop
    moveFish(p, dt);
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
  for (const p of players) wallFish(p);
  // food
  if (H.ph === 'play') {
    const rate = FOOD_RATE[H.set.food] * (1 + 0.12 * (players.length - 1));
    H.spawnAcc += rate * dt; while (H.spawnAcc >= 1 && H.foods.length < 160) { H.spawnAcc -= 1; spawnFood(); }
    const mouths = players.map((p) => [p, mouthOf(p)]);
    for (const f of H.foods) {
      if (f.capd) { // captured: host pulls it into the mouth, then removes it
        const p = H.players.get(f.cap), age = Date.now() - f.capAt;
        if (!p || age >= CAP_MS) { f.fade = -1; continue; }
        const m = mouthOf(p), k = age / CAP_MS; f.x += (m.x - f.x) * k; f.y += (m.y - f.y) * k; continue;
      }
      moveFlake(f, dt, mouths);
      if (f.fade > 0 && f.y > 0) for (const [p, m] of mouths) {
        if (inSuction(p, m, f)) {
          f.capd = true; f.cap = p.id; f.capAt = Date.now(); p.score += f.k; p.eats++; p.lastScore = Date.now();
          p.base = Math.min(MAX_SIZE, p.base * GROW); p.size = p.base * (p.pw ? SHARK_X : 1);
          H.ev.push([f.id, p.id, Math.round(f.x), Math.round(f.y), f.k, H.tick]); break;
        }
      }
    }
    H.foods = H.foods.filter((f) => f.fade > 0);
    // magic pill: host decides spawn, capture and power state
    const pw = H.set.pw;
    if (!pw) H.pill = null;
    if (pw && !H.pill && nowMs >= H.nextPill && !players.some((p) => p.pw)) spawnPill();
    const pl = H.pill;
    if (pl) {
      if (pl.capd) {
        const p = H.players.get(pl.cap), age = nowMs - pl.capAt;
        if (!p || p.out || age >= CAP_MS) { H.pill = null; H.nextPill = nowMs + pillGap(); if (p && !p.out) powerUp(p); }
        else { const m = mouthOf(p), k = age / CAP_MS; pl.x += (m.x - pl.x) * k; pl.y += (m.y - pl.y) * k; }
      } else {
        moveFlake(pl, dt, mouths, 1.25);
        if (pl.fade <= 0) { H.pill = null; H.nextPill = nowMs + pillGap(); }
        else if (pl.y > 0) for (const [p, m] of mouths) if (!p.pw && inSuction(p, m, pl, PILL_CAP_R)) { pl.capd = true; pl.cap = p.id; pl.capAt = nowMs; break; }
      }
    }
    // powered fish: chomp one other fish (mouth touching its body), or wear off after PW_MS
    for (const p of players) {
      if (!p.pw || p.out) continue;
      if (nowMs >= p.pw) { powerDown(p, 'time'); continue; }
      const m = mouthOf(p), fc = faceOf(p), bx = m.x + fc.x * 10 * p.base, by = m.y + fc.y * 10 * p.base;
      for (const q of players) {
        if (q === p || q.out || q.pw) continue; // a powered fish can't be eaten
        const d = Math.hypot(q.x - bx, (q.y - by) * 1.25);
        if (d < radiusOf(q.size) + 8) {
          q.out = true; q.ax = q.ay = 0; q.vx = q.vy = 0;
          fxAll({ k: 'chomp', a: p.id, b: q.id, x: Math.round(q.x), y: Math.round(q.y), f: q.f, s: +q.size.toFixed(3), sc: q.score });
          powerDown(p, 'ate');
          break;
        }
      }
    }
    // win checks. If eating leaves only one fish swimming, the round ends early; winner = highest score
    // (eaten fish keep their locked-in score).
    const swimming = players.filter((p) => !p.out);
    if (all.length >= 2 && swimming.length <= 1 && all.some((p) => p.out)) { if (!H.endAt) H.endAt = nowMs + 1800; if (nowMs >= H.endAt) hostEnd(); } // let the splat play out first
    else if (H.set.mode === 'timer' && timeLeft() <= 0) hostEnd();
    else if (H.set.mode === 'goal' && swimming.some((p) => p.score >= H.set.goal)) hostEnd();
  }
  // drop silent clients
  const cut = Date.now() - 9000;
  for (const [pid, e] of H.conns) if (e.lastSeen < cut) { try { e.c.close(); } catch {} dropClient(pid); }
  // snapshots
  H.tick++;
  const pl = H.pill;
  // compact snapshot; eat events ride along in the next 3 snapshots so a lost/reordered packet can't drop one
  H.ev = H.ev.filter((e) => H.tick - e[5] <= SNAP_EVERY * 3);
  const snap = { t: 's', n: H.tick, ph: H.ph, tl: timeLeft(), cd: H.ph === 'countdown' ? Math.max(0, H.cdEnd - Date.now()) : 0,
    p: all.map((p) => [p.id, Math.round(p.x), Math.round(p.y), +p.size.toFixed(3), p.score, p.f, +p.a.toFixed(2), Math.round(p.vx), Math.round(p.vy), p.out ? 1 : 0, p.pw ? Math.max(1, p.pw - nowMs) : 0]),
    fd: H.foods.map((f) => [f.id, Math.round(f.x), Math.round(f.y), f.k, +f.fade.toFixed(2), f.capd ? f.cap + 1 : 0, f.capd ? Date.now() - f.capAt : 0]),
    pl: pl ? [pl.id, Math.round(pl.x), Math.round(pl.y), +pl.fade.toFixed(2), pl.capd ? pl.cap + 1 : 0, pl.capd ? nowMs - pl.capAt : 0] : 0,
    ev: H.ev.map((e) => e.slice(0, 5)) };
  onClientData(snap);
  if (H.tick % SNAP_EVERY === 0) sendSnap(snap);
}

function dropClient(pid) {
  const e = H.conns.get(pid); if (e) { if (e.u) try { e.u.close(); } catch {} H.byPeer.delete(e.c.peer); }
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
    case 'in': if (p) { const q = +d.q || 0; if (q && q <= p.inQ) break; p.inQ = q; p.ax = clamp(+d.x || 0, -1, 1); p.ay = clamp(+d.y || 0, -1, 1); p.inAt = Date.now(); } break;
    case 'p': if (d.c != null) try { e.c.send({ t: 'po', c: d.c }); } catch {} break;
    case 'ready': if (p) { p.ready = !!d.v; bumpMeta(); } break;
    case 'bye': try { e.c.close(); } catch {} dropClient(pid); break;
  }
}
function onHostConnection(c) {
  if (c.label === 'u') { // second, unordered channel from an already-connected player
    c.on('open', () => { const pid = H.byPeer.get(c.peer), e = pid != null && H.conns.get(pid); if (!e) { try { c.close(); } catch {} return; } e.u = c;
      c.on('data', (d) => hostHandle(pid, d)); c.on('close', () => { if (e.u === c) e.u = null; }); });
    c.on('error', () => {}); return;
  }
  const pid = H.nextPid++;
  const ent = { c, u: null, lastSeen: Date.now() };
  H.conns.set(pid, ent); H.byPeer.set(c.peer, pid);
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
  if (patch.pw != null) { s.pw = !!patch.pw; if (!s.pw) { H.pill = null; for (const p of H.players.values()) powerDown(p, 'off'); } }
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
    c.on('open', () => {
      if (hostConn !== c) return; lastHostMsg = Date.now(); history.replaceState(null, '', '?room=' + code + (LOCAL_BUS ? '&local=1' : ''));
      // second, unordered channel for snapshots + steering so one late packet doesn't hold up the newer ones behind it
      try { const u = p.connect(PREFIX + code, { reliable: false, serialization: 'json', label: 'u' });
        u.on('open', () => { if (hostConn === c) uConn = u; }); u.on('data', (d) => { if (hostConn === c) { lastHostMsg = Date.now(); onClientData(d); } });
        u.on('close', () => { if (uConn === u) uConn = null; }); u.on('error', () => { if (uConn === u) uConn = null; }); } catch {}
    });
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
  hostConn = null; uConn = null; destroyPeer();
  showMsg('The host left the tank', 'This game ended because the host closed their page. Start a new tank to keep playing.');
}
function send(m) {
  if (role === 'host') { if (m.t === 'ready') { const p = H.players.get(0); if (p) { p.ready = !!m.v; bumpMeta(); } } else if (m.t === 'in') { const p = H.players.get(0); if (p) { p.ax = m.x; p.ay = m.y; p.inAt = Date.now(); } } else if (m.t === 'hello') hostHello(m); return; }
  const ch = m.t === 'in' && uConn && uConn.open ? uConn : hostConn;
  if (ch) try { ch.send(m); } catch {}
}
function hostHello(m) {
  if (H.players.has(0)) return;
  if (fishTaken(m.sp, m.c)) { onClientData({ t: 'taken' }); return; }
  addPlayer(0, { name: cleanName(m.name) || 'Host', sp: m.sp, c: m.c }); onClientData({ t: 'welcome', id: 0 });
}
setInterval(() => { if (role === 'client' && lastHostMsg && hostConn && Date.now() - lastHostMsg > 7000) hostLeft(); }, 1000);
setInterval(() => { if (role === 'client' && hostConn) send({ t: 'p', c: now() }); }, 1000); // keep-alive + round-trip time

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
      if (role === 'client') { // unordered channel: drop stale snapshots, track arrival jitter for the interpolation delay
        if (d.n != null && d.n <= V.seq) break; V.seq = d.n != null ? d.n : V.seq;
        const t = now(); if (V.lastAt) { const g = Math.min(400, t - V.lastAt); V.gapM += (g - V.gapM) * 0.1; V.gapD += (Math.abs(g - V.gapM) - V.gapD) * 0.1; } V.lastAt = t;
        V.idelay = clamp(V.gapM + 2.5 * V.gapD + 4, INTERP_MIN, INTERP_MAX);
      } else V.idelay = HOST_INTERP;
      d.at = now(); V.snaps.push(d); if (V.snaps.length > 10) V.snaps.shift();
      V.ph = d.ph; V.tl = d.tl; V.cd = d.cd;
      for (const e of d.ev || []) onEat(e);
      for (const f of d.fd || []) if (f[5]) onEat([f[0], f[5] - 1, f[1], f[2], f[3]], f[6]);
      if (d.pl && d.pl[4]) onEat([d.pl[0], d.pl[4] - 1, d.pl[1], d.pl[2], 3], d.pl[5]);
      if (role === 'client') reconcile(d);
      break;
    }
    case 'po': { const r = now() - d.c; if (r >= 0 && r < 5000) V.rtt = V.rtt ? V.rtt + (r - V.rtt) * 0.25 : r; break; }
    case 'fx': onFx(d); break;
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
  else if (ph === 'play') { $('#countdown').hidden = true; cdLast = ''; }
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
  // Always hide after FEEDING TIME. (Bug: on non-host fish a snapshot still saying 'countdown' could arrive just after
  // the 'play' message and re-pop "1"; this timer then saw cdLast !== txt and never hid it, so the "1" stayed stuck.)
  if (finish) { clearTimeout(popCountdown.t); popCountdown.t = setTimeout(() => { if (V.meta.ph !== 'countdown') { box.hidden = true; cdLast = ''; } }, 1300); }
}
function showEnd() {
  const res = V.result || []; const pl = V.meta.pl || {};
  const pod = $('#podium'); pod.textContent = '';
  const order = [1, 0, 2];
  for (const i of order) {
    const r = res[i]; if (!r) continue; const m = pl[r[0]]; if (!m) continue;
    const col = el('div', 'pod pod' + (i + 1));
    const img = el('img', 'pod-fish'); img.src = window.FinFish.urlFor(m[1], m[2]); img.alt = '';
    col.append(img, el('div', 'pod-name', (r[2] ? '💥 ' : '') + m[0] + (+r[0] === myId ? ' (you)' : '')), el('div', 'pod-pts', r[1] + ' pts'), el('div', 'pod-block', String(i + 1)));
    pod.append(col);
  }
  const ol = $('#finalList'); ol.textContent = '';
  res.forEach((r, i) => { const m = pl[r[0]]; if (!m) return; const li = el('li', +r[0] === myId ? 'me' : ''); li.append(el('b', '', '#' + (i + 1)), el('span', '', (r[2] ? '💥 ' : '') + m[0] + (r[2] ? ' (chomped)' : '')), el('em', '', r[1] + ' pts')); ol.append(li); });
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
  const pwOn = set.pw !== false;
  document.querySelectorAll('#pwSeg button').forEach((b) => { b.classList.toggle('on', (b.dataset.pw === '1') === pwOn); b.disabled = !host; });
  $('#timerVal').textContent = set.min + ' min'; $('#goalVal').textContent = set.goal + ' pts';
  $('#timerStep').classList.toggle('dim', set.mode !== 'timer'); $('#goalStep').classList.toggle('dim', set.mode !== 'goal');
  document.querySelectorAll('.stepper button').forEach((b) => { b.disabled = !host; });
  $('#startOverBtn').disabled = !host;
  document.querySelectorAll('.host-only').forEach((x) => { x.hidden = host; });
  // lobby bar
  const inLobby = joined && m.ph === 'lobby';
  $('#lobbyBar').hidden = !inLobby;
  const pl = Object.entries(m.pl || {}); const ready = pl.filter(([, p]) => p[3]).length;
  $('#readyCount').textContent = `${ready}/${pl.length} ready` + (host ? '' : ' · host starts the game') + ` · Power-ups ${set.pw !== false ? 'On ✨' : 'Off'}`;
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
    r.className = 'row' + (i < 3 ? ' top' + (i + 1) : '') + (p.id === myId ? ' me' : '') + (p.out ? ' out' : '') + (p.pw ? ' pw' : '');
    r.style.transform = `translateY(${i * rowH}px)`; r.style.setProperty('--c', col);
    r.querySelector('.rk').textContent = i + 1;
    const img = r.querySelector('img'), src = window.FinFish.urlFor(m[1], m[2]); if (img.getAttribute('src') !== src) img.src = src;
    r.querySelector('.nm').textContent = (p.out ? '💥 ' : p.pw ? '🦈 ' : '') + m[0] + (p.id === myId ? ' · YOU' : '');
    r.querySelector('.bar i').style.width = Math.min(100, p.score / denom * 100) + '%';
    const pt = r.querySelector('.pt'); if (pt.textContent !== String(p.score)) { pt.textContent = p.score; pt.classList.remove('bump'); void pt.offsetWidth; pt.classList.add('bump'); }
  });
  for (const [id, r] of rowEls) if (!seen.has(id)) { r.remove(); rowEls.delete(id); }
  box.style.height = ranked.length * rowH + 'px';
  const myRank = ranked.findIndex((p) => p.id === myId);
  $('#boardMine').textContent = myRank >= 0 ? `YOU #${myRank + 1} · ${ranked[myRank].score}` : '';
  $('#boardFoot').textContent = (set.mode === 'goal' ? `FIRST TO ${set.goal} PTS` : `MOST POINTS IN ${set.min} MIN`) + ` · ${ranked.length}/${MAX_PLAYERS} FISH · POWER-UPS ${set.pw !== false ? 'ON' : 'OFF'}`;
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
// Mouse steering is off unless ?mouse=1; pen/touch (tap and drag) always steer.
const usePtr = (e) => e.pointerType !== 'mouse' || MOUSE_ON;
$('#steerHint').textContent = TOUCH ? 'Tap and drag to swim · arrow keys / WASD on a keyboard' : 'Swim with the arrow keys or WASD' + (MOUSE_ON ? ' (or your mouse)' : '');
const aim = (e) => { pointer = { x: e.clientX, y: e.clientY, w: screenToWorld(e.clientX, e.clientY) }; pointerKind = e.pointerType; };
cv.addEventListener('pointermove', (e) => { if (usePtr(e) && (e.pointerType === 'mouse' || pointerDown)) aim(e); });
cv.addEventListener('pointerdown', (e) => { closeMenu(); if (!usePtr(e)) return; pointerDown = true; aim(e); try { cv.setPointerCapture(e.pointerId); } catch {} });
const endPointer = (e) => { if (!usePtr(e)) return; pointerDown = false; if (e.pointerType !== 'mouse' || e.type !== 'pointerup') { pointer = null; sendZero(); } };
cv.addEventListener('pointerup', endPointer); cv.addEventListener('pointercancel', endPointer);
cv.addEventListener('pointerleave', (e) => { if (usePtr(e) && !pointerDown) { pointer = null; sendZero(); } });
cv.addEventListener('lostpointercapture', (e) => { if (e.pointerType !== 'mouse') { pointerDown = false; pointer = null; sendZero(); } });
let inQ = 0, myIn = { x: 0, y: 0 };
function sendIn(x, y) { myIn = { x, y }; lastSent = { x, y, at: Date.now() }; if (role === 'host') { const p = H.players.get(myId); if (p) { p.ax = x; p.ay = y; p.inAt = Date.now(); } return; } try { send({ t: 'in', x, y, q: ++inQ }); } catch {} }
function sendZero() { if (!joined) return; sendIn(0, 0); }
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
  if (me && me.out) x = y = 0;
  myIn = { x, y };
  // send on change (at most every 30 ms) plus a 250 ms heartbeat; seq numbers let the host ignore reordered inputs
  const t = Date.now();
  if (x !== lastSent.x || y !== lastSent.y ? t - lastSent.at > 30 : t - lastSent.at > 250) sendIn(x, y);
}
// immediate send on key changes so the first frame of a keypress isn't lost waiting for the next animation frame
addEventListener('keydown', (e) => { if (joined && STEER_KEYS.includes(e.key.toLowerCase()) && !e.repeat && !isTyping(e.target)) steer(V.lastMe); });
addEventListener('keyup', (e) => { if (joined && STEER_KEYS.includes(e.key.toLowerCase())) steer(V.lastMe); });

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

// Others are drawn slightly in the past (adaptive delay from snapshot jitter, ~50-150 ms on clients, 45 ms on the host),
// with up to 100 ms of extrapolation if snapshots are late. Your own fish is drawn from local prediction (clients)
// or the live simulation (host), so it reacts to your keys immediately.
function interp() {
  const s = V.snaps; if (!s.length) return null;
  const rt = now() - V.idelay;
  let a = s[0], b = s[0], ex = 0;
  for (let i = 0; i < s.length - 1; i++) if (s[i].at <= rt && s[i + 1].at >= rt) { a = s[i]; b = s[i + 1]; break; }
  if (rt > s[s.length - 1].at) { a = b = s[s.length - 1]; ex = Math.min(100, rt - b.at) / 1000; }
  const k = b === a ? 1 : clamp((rt - a.at) / (b.at - a.at), 0, 1);
  const am = new Map(a.p.map((p) => [p[0], p]));
  const players = b.p.map((pb) => {
    const pa = am.get(pb[0]) || pb;
    return { id: pb[0], x: pa[1] + (pb[1] - pa[1]) * k + pb[7] * ex, y: pa[2] + (pb[2] - pa[2]) * k + pb[8] * ex, size: pb[3], score: pb[4], f: pb[5], a: pa[6] + (pb[6] - pa[6]) * k, vx: pb[7], vy: pb[8], out: !!pb[9], pw: pb[10] || 0 };
  });
  // own fish
  const mine = players.find((p) => p.id === myId), last = s[s.length - 1].p.find((p) => p[0] === myId);
  if (mine && !mine.out) {
    if (role === 'host') { const h = H.players.get(myId); if (h) Object.assign(mine, { x: h.x, y: h.y, f: h.f, a: h.a, vx: h.vx, vy: h.vy, size: h.size, score: h.score }); }
    else if (pred.ok) { Object.assign(mine, { x: pred.x, y: pred.y, f: pred.f, a: pred.a, vx: pred.vx, vy: pred.vy }); if (last) { mine.size = last[3]; mine.score = last[4]; mine.pw = last[10] || 0; } }
  }
  const af = new Map(a.fd.map((f) => [f[0], f]));
  const foods = b.fd.map((fb) => { const fa = af.get(fb[0]) || fb; return { id: fb[0], x: fa[1] + (fb[1] - fa[1]) * k, y: fa[2] + (fb[2] - fa[2]) * k, k: fb[3], fade: fb[4] }; });
  let pill = null;
  if (b.pl && !b.pl[4]) { const pa = a.pl && a.pl[0] === b.pl[0] ? a.pl : b.pl; pill = { id: b.pl[0], x: pa[1] + (b.pl[1] - pa[1]) * k, y: pa[2] + (b.pl[2] - pa[2]) * k, fade: b.pl[3] }; }
  return { players, foods, pill };
}
/* client-side prediction of your own fish, softly reconciled with the host */
const pred = { ok: false, x: 0, y: 0, vx: 0, vy: 0, ax: 0, ay: 0, f: 1, a: 0, size: 1, base: 1, hist: [], cx: 0, cy: 0, snaps: 0, errs: [] };
const SNAP_ERR = 220;
function reconcile(d) {
  const e = d.p.find((p) => p[0] === myId);
  if (!e || e[9]) { pred.ok = false; return; }
  pred.size = e[3]; pred.base = e[10] ? e[3] / 2 : e[3];
  if (!pred.ok) { Object.assign(pred, { ok: true, x: e[1], y: e[2], vx: e[7], vy: e[8], f: e[5], a: e[6], hist: [], cx: 0, cy: 0 }); return; }
  // the host's state reflects our inputs from about one round trip ago: compare with where we predicted we were then
  const tt = now() - (V.rtt || 60) - TICK_MS / 2; let h = null;
  for (let i = pred.hist.length - 1; i >= 0; i--) if (pred.hist[i].t <= tt) { h = pred.hist[i]; break; }
  if (!h) h = pred.hist[0]; if (!h) return;
  const ex = e[1] - h.x, ey = e[2] - h.y, err = Math.hypot(ex, ey);
  pred.errs.push(Math.round(err)); if (pred.errs.length > 300) pred.errs.shift();
  if (err > SNAP_ERR) { Object.assign(pred, { x: e[1], y: e[2], vx: e[7], vy: e[8], hist: [], cx: 0, cy: 0 }); pred.snaps++; return; }
  pred.cx = ex; pred.cy = ey; // applied gradually in predStep
  pred.vx += (e[7] - h.vx) * 0.25; pred.vy += (e[8] - h.vy) * 0.25;
}
function predStep(dt) {
  if (role !== 'client' || !pred.ok) return;
  pred.ax = myIn.x; pred.ay = myIn.y;
  moveFish(pred, dt);
  const k = Math.min(1, dt * 10), dx = pred.cx * k, dy = pred.cy * k;
  pred.x += dx; pred.y += dy; pred.cx -= dx; pred.cy -= dy;
  for (const h of pred.hist) { h.x += dx; h.y += dy; }
  wallFish(pred);
  const t = now(); pred.hist.push({ t, x: pred.x, y: pred.y, vx: pred.vx, vy: pred.vy });
  while (pred.hist.length && t - pred.hist[0].t > 1000) pred.hist.shift();
}
/* one-off effects from the host: pill swallowed, chomp + splat, power-up over */
function onFx(d) {
  const t = now(), pl = V.meta.pl || {};
  if (d.k === 'pill') { const fv = fishView(d.pid); if (fv.wx != null) BURSTS.push({ x: fv.wx, y: fv.wy, t, r: Math.random() * 6 }); fv.pwAt = t; fv.vs += 4; if (d.pid === myId) toast('MAGIC SHARK PILL! You\'re a great white 🦈 chomp ONE fish in 15s!'); else if (pl[d.pid]) toast(pl[d.pid][0] + ' got the magic shark pill! 🦈'); }
  else if (d.k === 'unpw') { const fv = fishView(d.pid); fv.unAt = t; if (d.why === 'ate' && !REDUCED) { fv.holdShark = t + 650; fv.sharkSize = fv.ds; } else fv.vs -= 2; if (d.pid === myId && d.why === 'time') toast('Power-up wore off'); }
  else if (d.k === 'chomp') {
    const m = pl[d.b] || [];
    V.splats.push(mkSplat(d.x, d.y, d.s, t));
    const fa = fishView(d.a); fa.chompAt = t; fa.snap = t;
    if (d.b === myId) { pred.ok = false; releaseAll(); }
    else if (d.a === myId) toast('CHOMP! You ate ' + (m[0] || 'a fish') + '!');
    else toast((pl[d.a] || ['Someone'])[0] + ' ate ' + (m[0] || 'a fish') + '!');
  }
}
const eatenIds = new Set();
// A capture (from the host event or a snapshot flake with a captured-by id) starts the pull locally.
// The flake tracks the live mouth each frame; the gulp + grow fire on arrival.
function onEat(e, age = 0) {
  const [fid, pid, x, y, k] = e; if (eatenIds.has(fid)) return; eatenIds.add(fid); if (eatenIds.size > 500) eatenIds.clear();
  const t0 = now() - Math.min(age, CAP_MS);
  const fv = fishView(pid);
  if (k === 3) { V.pillCap = fid; }
  V.eats.push({ fid, pid, x, y, k, t: t0, arrive: t0 + CAP_MS, parts: REDUCED ? [] : Array.from({ length: 7 }, () => ({ a: rand(-1.1, 1.1), r: rand(1.6, 3.2), d: rand(0, 0.35), l: rand(8, 16) })) });
  fv.holdSize = fv.holdUntil > now() ? fv.holdSize : fv.ds; fv.holdUntil = t0 + CAP_MS;
}
function fishView(id) { let f = V.fish.get(id); if (!f) { f = { ds: 1, vs: 0, mouth: 0, snap: -1e9, holdUntil: 0, holdSize: 1, wig: rand(0, 6) }; V.fish.set(id, f); } return f; }

let shake = 0, lastFrame = now(), boardAt = 0, urgent = false;
function frame() {
  const t = now(); const dt = Math.min(0.05, (t - lastFrame) / 1000); lastFrame = t;
  if (joined) steer(pred.ok && role === 'client' ? pred : (V.lastMe || null));
  predStep(dt);
  const st = interp();
  const all = st ? st.players : [], foods = st ? st.foods.filter((f) => !eatenIds.has(f.id)) : [];
  const players = all.filter((p) => !p.out), pill = st && st.pill && !eatenIds.has(st.pill.id) ? st.pill : null;
  const meAll = all.find((p) => p.id === myId), me = meAll && !meAll.out ? meAll : null; V.lastMe = me;
  const spectating = !!(joined && meAll && meAll.out);
  const eb = $('#eatenBanner'); if (eb.hidden === (spectating && V.ph === 'play')) eb.hidden = !(spectating && V.ph === 'play');
  if (QS.has('test')) { const dm = new Map(); for (const p of all) dm.set(p.id, { x: p.x, y: p.y, size: p.size, out: p.out, pw: p.pw }); window.__drawn = dm; }
  // urgency
  const set = V.meta.set || DEFAULTS;
  const maxScore = Math.max(0, ...players.map((p) => p.score));
  const u = joined && V.ph === 'play' && ((set.mode === 'timer' && V.tl <= 10000) || (set.mode === 'goal' && maxScore >= 0.75 * set.goal));
  if (u !== urgent) { urgent = u; document.body.classList.toggle('urgent', u); }
  // camera
  const zFit = Math.min(VW / WORLD_W, VH / WORLD_H);
  const zTarget = joined && !spectating ? Math.max(0.42, Math.min(VW / 1150, VH / 760)) : Math.max(zFit, Math.min(VW / WORLD_W, VH / WORLD_H) * 1.0);
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
  if (pill) { if (pill.fade > 0.5) pillAlert(pill.id); drawPill(pill.x, pill.y, t, clamp(pill.fade, 0, 1), 1, true); }
  // fish (smaller first so big fish sit on top), names after
  const sorted = players.slice().sort((a, b) => a.size - b.size);
  const bait = pill ? foods.concat([{ x: pill.x, y: pill.y }]) : foods;
  sharkPrey = players.filter((q) => !(q.pw > 0));
  for (const p of sorted) { const fv = fishView(p.id); fv.wx = p.x + p.f * 0.43 * FISH_LEN * fv.ds; fv.wy = p.y; drawFish(p, bait, t, dt); }
  drawEats(players, t);
  drawSplats(t);
  drawBursts(t);
  for (const p of sorted) drawName(p);
  if (pill && V.ph === 'play') { ctx.setTransform(DPR, 0, 0, DPR, 0, 0); drawPillArrow(pill, t); ctx.setTransform(DPR * cam.z, 0, 0, DPR * cam.z, DPR * (VW / 2 - cam.x * cam.z + sx), DPR * (VH / 2 - cam.y * cam.z + sy)); }
  if (urgent) { ctx.setTransform(DPR, 0, 0, DPR, 0, 0); const a = 0.10 + 0.08 * Math.sin(t / 160); ctx.fillStyle = `rgba(255,46,136,${a})`; ctx.fillRect(0, 0, VW, VH); }
  // HUD text
  updateHud(players, t);
  if (t - boardAt > 150 && joined) { boardAt = t; renderBoard(all); }
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
  if (p.pw > 0) { const sm = sharkMouth(FISH_LEN * ds, FISH_HT * ds); return { ...sm, w, h }; }
  return { lx: ((s.mouth[0] + 14) / 268 - 0.5) * w, ly: ((s.mouth[1] + 14) / 208 - 0.5) * h, w, h };
}
function drawFish(p, foods, t, dt) {
  const m = V.meta.pl[p.id]; if (!m) return;
  const fv = fishView(p.id);
  // bouncy grow spring toward real size
  if (fv.holdUntil && t >= fv.holdUntil) { fv.holdUntil = 0; fv.snap = t; fv.vs += 2.6; } // gulp, then grow
  const sharkHeld = fv.holdShark && t < fv.holdShark; // stay a shark for a beat after the chomp, then morph back
  const target = sharkHeld ? fv.sharkSize : fv.holdUntil ? Math.min(p.size, fv.holdSize) : p.size;
  fv.vs += (target - fv.ds) * 160 * dt; fv.vs *= Math.exp(-9 * dt); fv.ds += fv.vs * dt;
  if (fv.ds < 0.5) fv.ds = 0.5;
  const ds = fv.ds;
  const { lx, ly, w, h } = localMouth(p, ds);
  // world mouth
  const ca = Math.cos(p.a), sa = Math.sin(p.a);
  const mx = p.x + (lx * ca - ly * sa) * p.f, my = p.y + (lx * sa + ly * ca);
  // mouth opens only when a flake is right about to touch it
  let near = 1e9; for (const f of foods) { const d = Math.hypot(f.x - mx, f.y - my); if (d < near) near = d; }
  // mouth only starts opening when a flake is really close (just outside the suction zone)
  const sr = suckRadius(p.size), openR = sr + 14;
  let want = near < openR ? clamp(1 - (near - sr * 0.6) / (openR - sr * 0.6), 0, 0.75) : 0;
  const pulling = fv.holdUntil && t < fv.holdUntil;
  if (pulling) want = 1.15; // gape wide while the flake is being sucked in
  const since = t - fv.snap; if (since < 200) want = 0; // chomp shut on arrival
  fv.mouth += (want - fv.mouth) * Math.min(1, dt * (pulling || since < 200 ? 40 : 14));
  const spr = window.FinFish.sprite(m[1], m[2]);
  const speed = Math.hypot(p.vx, p.vy);
  fv.wig += dt * (3 + speed / 40);
  ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.a); ctx.scale(p.f, 1);
  // gulp: quick squash (shorter, taller) then rebound
  const gk = since < 260 ? since / 260 : 1, gulp = gk < 1 ? Math.sin(gk * Math.PI) * Math.exp(-gk * 2) : 0;
  ctx.scale((1 + Math.sin(fv.wig) * 0.025) * (1 - 0.12 * gulp), 1 + 0.14 * gulp);
  const pw = p.pw > 0, warn = pw && p.pw < PW_WARN, blinkOff = warn && Math.floor(t / (p.pw < 1200 ? 90 : 160)) % 2 === 0;
  // shark morph 0 = fish, 1 = great white (smooth in and out; blinks back to the fish in the last 3 s)
  const skT = (pw && !blinkOff) || sharkHeld ? 1 : 0; fv.sk = fv.sk == null ? skT : fv.sk + (skT - fv.sk) * (REDUCED ? 1 : Math.min(1, dt * (warn ? 30 : 6)));
  const sk = fv.sk, hex = (COLORS.find((c) => c.id === m[2]) || COLORS[0]).hex;
  if (pw && !blinkOff) drawAura(w, h * 0.7, t);
  if (p.id === myId) { ctx.fillStyle = 'rgba(255,230,0,.35)'; ctx.beginPath(); ctx.ellipse(0, 0, w * 0.48, h * (0.45 - 0.15 * sk), 0, 0, 7); ctx.fill(); }
  if (sk < 0.99) {
    ctx.save(); ctx.globalAlpha = 1 - sk; const fs = 1 - 0.5 * sk; // the fish shrinks into the shark
    if (spr) ctx.drawImage(spr, -w * fs / 2, -h * fs / 2, w * fs, h * fs);
    else { ctx.fillStyle = hex; ctx.beginPath(); ctx.ellipse(0, 0, w * 0.35 * fs, h * 0.3 * fs, 0, 0, 7); ctx.fill(); }
    ctx.restore();
  }
  if (sk > 0.01) { ctx.save(); ctx.globalAlpha = sk; const ss = 0.55 + 0.45 * sk; ctx.scale(ss, ss); drawShark(FISH_LEN * ds, FISH_HT * ds, ds, t, fv, hex); ctx.restore(); }
  if (fv.mouth > 0.04 && sk < 0.5) {
    const o = fv.mouth, sc = ds;
    ctx.translate(lx - 3 * sc, ly);
    ctx.fillStyle = '#111'; ctx.beginPath(); ctx.ellipse(0, 0, (4 + 6 * o) * sc, (2 + 9 * o) * sc, 0, 0, 7); ctx.fill();
    ctx.fillStyle = '#FF2E88'; ctx.beginPath(); ctx.ellipse(-1 * sc, 3 * o * sc, (2 + 3.5 * o) * sc, (1 + 4 * o) * sc, 0, 0, 7); ctx.fill();
    ctx.strokeStyle = '#111'; ctx.lineWidth = 2.5 * sc; ctx.beginPath(); ctx.ellipse(0, 0, (4 + 6 * o) * sc, (2 + 9 * o) * sc, 0, 0, 7); ctx.stroke();
  }
  ctx.restore();
  if (pw && p.id === myId) { // countdown ring above your powered fish
    ctx.save(); ctx.lineWidth = 6; ctx.strokeStyle = warn ? (blinkOff ? '#fff' : '#FF2E88') : '#A970FF'; ctx.beginPath();
    ctx.arc(p.x, p.y - h * 0.62 - 30, 13, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * clamp(p.pw / PW_MS, 0, 1)); ctx.stroke(); ctx.restore();
  }
  p._mx = mx; p._my = my; p._h = h;
}
// rainbow aura around a powered fish (fish-local coords)
function drawAura(w, h, t) {
  ctx.save(); ctx.globalCompositeOperation = 'lighter';
  const pulse = REDUCED ? 1 : 1 + 0.06 * Math.sin(t / 140);
  for (let i = 0; i < 3; i++) {
    const hue = (t / 8 + i * 120) % 360;
    ctx.fillStyle = `hsla(${hue},100%,62%,${0.18 - i * 0.04})`;
    ctx.beginPath(); ctx.ellipse(0, 0, w * (0.58 + i * 0.07) * pulse, h * (0.58 + i * 0.07) * pulse, 0, 0, 7); ctx.fill();
  }
  ctx.restore();
  if (!REDUCED) for (let i = 0; i < 6; i++) { // orbiting sparkles
    const a = t / 500 + i * Math.PI / 3, x = Math.cos(a) * w * 0.6, y = Math.sin(a) * h * 0.6, s = 5 + 2 * Math.sin(t / 90 + i);
    ctx.fillStyle = '#fff'; ctx.beginPath(); ctx.moveTo(x, y - s); ctx.lineTo(x + s * 0.3, y); ctx.lineTo(x, y + s); ctx.lineTo(x - s * 0.3, y); ctx.closePath(); ctx.fill();
  }
}
// cartoon great white (fish-local, facing +x). w/h = fish box at this size; outline in the player's colour.
let sharkPrey = [];
function drawShark(w, h, ds, t, fv, hex) {
  const L = w, H = h * 0.6, lw = Math.max(3, 2.2 * ds);
  const since = t - (fv.chompAt || -1e9);
  // jaws: idle menace, gape when prey is close, SNAP shut on the chomp, then relax
  let near = 1e9; const me = fv.wx != null ? fv : null;
  if (me) for (const q of sharkPrey) { const d = Math.hypot(q.x - me.wx, q.y - me.wy); if (d < near && d > 1) near = d; }
  let want = 0.22 + (REDUCED ? 0 : 0.08 * Math.sin(t / 170));
  if (near < L * 1.1) want = 0.9;
  if (since < 90) want = 1; else if (since < 360) want = 0;
  fv.jaw = fv.jaw == null ? want : fv.jaw + (want - fv.jaw) * Math.min(1, (since < 360 ? 0.5 : 0.15) * 60 / 60);
  const jo = clamp(fv.jaw, 0, 1);
  const tw = REDUCED ? 0 : Math.sin(fv.wig * 1.3) * 0.12; // tail swish
  ctx.lineJoin = 'round'; ctx.lineWidth = lw; ctx.strokeStyle = '#111';
  // tail (crescent)
  ctx.save(); ctx.translate(-0.42 * L, 0); ctx.rotate(tw);
  ctx.beginPath(); ctx.moveTo(0, -0.08 * H); ctx.quadraticCurveTo(-0.08 * L, -0.35 * H, -0.2 * L, -0.62 * H); ctx.quadraticCurveTo(-0.12 * L, -0.15 * H, -0.12 * L, 0);
  ctx.quadraticCurveTo(-0.12 * L, 0.15 * H, -0.17 * L, 0.45 * H); ctx.quadraticCurveTo(-0.06 * L, 0.25 * H, 0, 0.08 * H); ctx.closePath();
  ctx.fillStyle = '#7d8c9b'; ctx.fill(); ctx.stroke(); ctx.restore();
  // dorsal fin
  ctx.beginPath(); ctx.moveTo(-0.12 * L, -0.4 * H); ctx.quadraticCurveTo(-0.02 * L, -0.75 * H, -0.04 * L, -0.98 * H); ctx.quadraticCurveTo(0.06 * L, -0.6 * H, 0.12 * L, -0.42 * H); ctx.closePath();
  ctx.fillStyle = '#7d8c9b'; ctx.fill(); ctx.strokeStyle = hex; ctx.lineWidth = lw * 2.2; ctx.stroke(); ctx.strokeStyle = '#111'; ctx.lineWidth = lw; ctx.stroke();
  // lower jaw (hinged near the eye, swings down)
  const hx = 0.2 * L, hy = 0.12 * H;
  ctx.save(); ctx.translate(hx, hy); ctx.rotate(jo * 0.55);
  ctx.beginPath(); ctx.moveTo(-0.02 * L, 0); ctx.lineTo(0.22 * L, 0.02 * H); ctx.quadraticCurveTo(0.2 * L, 0.3 * H, 0.0, 0.32 * H); ctx.quadraticCurveTo(-0.06 * L, 0.2 * H, -0.02 * L, 0); ctx.closePath();
  ctx.fillStyle = '#f4f6f8'; ctx.fill(); ctx.stroke();
  ctx.fillStyle = '#fff'; for (let i = 0; i < 6; i++) { const x = 0.19 * L - i * 0.035 * L; ctx.beginPath(); ctx.moveTo(x, 0.02 * H); ctx.lineTo(x - 0.017 * L, -0.11 * H); ctx.lineTo(x - 0.034 * L, 0.02 * H); ctx.closePath(); ctx.fill(); ctx.lineWidth = lw * 0.5; ctx.stroke(); }
  ctx.restore(); ctx.lineWidth = lw;
  // mouth cavity
  if (jo > 0.05) { ctx.fillStyle = '#5a0b1e'; ctx.beginPath(); ctx.moveTo(hx, hy); ctx.lineTo(hx + 0.23 * L * Math.cos(jo * 0.55), hy + 0.23 * L * Math.sin(jo * 0.55)); ctx.lineTo(0.47 * L, 0.06 * H); ctx.closePath(); ctx.fill(); }
  // body: grey top, white belly
  ctx.beginPath(); ctx.moveTo(0.5 * L, 0.02 * H);
  ctx.bezierCurveTo(0.45 * L, -0.38 * H, 0.15 * L, -0.5 * H, -0.12 * L, -0.42 * H); ctx.bezierCurveTo(-0.3 * L, -0.34 * H, -0.4 * L, -0.15 * H, -0.44 * L, -0.06 * H);
  ctx.lineTo(-0.44 * L, 0.06 * H); ctx.bezierCurveTo(-0.3 * L, 0.3 * H, -0.05 * L, 0.42 * H, hx, hy + 0.06 * H); ctx.lineTo(0.47 * L, 0.06 * H); ctx.closePath();
  const g = ctx.createLinearGradient(0, -0.45 * H, 0, 0.4 * H); g.addColorStop(0, '#6f7f8f'); g.addColorStop(0.5, '#9aa8b5'); g.addColorStop(0.56, '#f4f6f8'); g.addColorStop(1, '#ffffff');
  ctx.fillStyle = g; ctx.fill(); ctx.strokeStyle = hex; ctx.lineWidth = lw * 2.2; ctx.stroke(); ctx.strokeStyle = '#111'; ctx.lineWidth = lw; ctx.stroke();
  // upper teeth
  ctx.fillStyle = '#fff'; for (let i = 0; i < 7; i++) { const x = 0.46 * L - i * 0.037 * L, y = 0.06 * H - (0.46 * L - x) * 0.18 * H / L * 0.3 + (jo > 0.05 ? 0 : 0);
    ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x - 0.018 * L, y + (0.1 + 0.05 * jo) * H); ctx.lineTo(x - 0.036 * L, y); ctx.closePath(); ctx.fill(); ctx.lineWidth = lw * 0.5; ctx.stroke(); }
  ctx.lineWidth = lw;
  // pectoral fin
  ctx.beginPath(); ctx.moveTo(0.02 * L, 0.12 * H); ctx.quadraticCurveTo(-0.08 * L, 0.45 * H, -0.16 * L, 0.6 * H); ctx.quadraticCurveTo(-0.06 * L, 0.3 * H, 0.1 * L, 0.14 * H); ctx.closePath();
  ctx.fillStyle = '#7d8c9b'; ctx.fill(); ctx.stroke();
  // gills
  ctx.lineWidth = lw * 0.8; for (let i = 0; i < 3; i++) { const x = 0.12 * L - i * 0.035 * L; ctx.beginPath(); ctx.moveTo(x, -0.18 * H); ctx.quadraticCurveTo(x - 0.025 * L, 0, x, 0.14 * H); ctx.stroke(); }
  // mean eye + angry brow
  const ex = 0.3 * L, ey = -0.14 * H, er = Math.max(4, 0.045 * L * 0.6);
  ctx.fillStyle = '#fff'; ctx.beginPath(); ctx.arc(ex, ey, er, 0, 7); ctx.fill(); ctx.lineWidth = lw * 0.7; ctx.stroke();
  ctx.fillStyle = '#111'; ctx.beginPath(); ctx.arc(ex + er * 0.25, ey + er * 0.1, er * 0.55, 0, 7); ctx.fill();
  ctx.lineWidth = lw * 1.3; ctx.beginPath(); ctx.moveTo(ex - er * 1.6, ey - er * 1.7); ctx.lineTo(ex + er * 1.5, ey - er * 0.6); ctx.stroke();
  ctx.lineWidth = lw;
}
// MAGIC SHARK PILL: cached glossy capsule (no emblems or rays), pulsing halo,
// holographic shimmer sweep, orbiting sparkles and a glitter trail. Sprites are built once.
const PILL_SPR = {};
function pillSprites() {
  if (PILL_SPR.cap) return PILL_SPR;
  const mk = (w, h) => { const c = document.createElement('canvas'); c.width = w; c.height = h; return c; };
  const S = 4, r = PILL_R * 0.8, L = PILL_R * 1.25, W = (L + 2 * r + 8) * S, Hh = (2 * r + 8) * S;
  const cap = mk(W, Hh), g = cap.getContext('2d'); g.translate(W / 2, Hh / 2); g.scale(S, S);
  const body = (c) => { c.beginPath(); c.arc(-L / 2, 0, r, Math.PI / 2, Math.PI * 1.5); c.lineTo(L / 2, -r); c.arc(L / 2, 0, r, -Math.PI / 2, Math.PI / 2); c.closePath(); };
  g.save(); body(g); g.clip();
  let gr = g.createLinearGradient(0, -r, 0, r); gr.addColorStop(0, '#FF7AC0'); gr.addColorStop(0.5, '#FF2E88'); gr.addColorStop(1, '#B0005A');
  g.fillStyle = gr; g.fillRect(-L - r, -r, L + r, 2 * r);
  gr = g.createLinearGradient(0, -r, 0, r); gr.addColorStop(0, '#9CF6FF'); gr.addColorStop(0.5, '#00E5FF'); gr.addColorStop(1, '#0088B8');
  g.fillStyle = gr; g.fillRect(0, -r, L + r, 2 * r);
  g.fillStyle = '#FFE600'; g.fillRect(-2, -r, 4, 2 * r); // gold band
  g.fillStyle = 'rgba(255,255,255,.75)'; g.beginPath(); g.ellipse(-L * 0.1, -r * 0.5, L * 0.75, r * 0.2, 0, 0, 7); g.fill();
  g.restore();
  g.lineWidth = 3; body(g); g.stroke();
  // halo
  const R3 = 64, halo = mk(R3 * 2, R3 * 2), hq = halo.getContext('2d'), hg = hq.createRadialGradient(R3, R3, 4, R3, R3, R3);
  hg.addColorStop(0, 'rgba(255,255,255,.95)'); hg.addColorStop(0.25, 'rgba(0,229,255,.6)'); hg.addColorStop(0.6, 'rgba(169,112,255,.35)'); hg.addColorStop(1, 'rgba(255,46,136,0)');
  hq.fillStyle = hg; hq.fillRect(0, 0, R3 * 2, R3 * 2);
  Object.assign(PILL_SPR, { cap, cw: W / S, ch: Hh / S, halo, R3, r, L, body });
  return PILL_SPR;
}
const SPARK_COLS = ['#FFE600', '#00E5FF', '#FF2E88', '#fff', '#A970FF'];
function star(x, y, s, col) { ctx.fillStyle = col; ctx.beginPath(); ctx.moveTo(x, y - s); ctx.lineTo(x + s * 0.3, y - s * 0.3); ctx.lineTo(x + s, y); ctx.lineTo(x + s * 0.3, y + s * 0.3); ctx.lineTo(x, y + s); ctx.lineTo(x - s * 0.3, y + s * 0.3); ctx.lineTo(x - s, y); ctx.lineTo(x - s * 0.3, y - s * 0.3); ctx.closePath(); ctx.fill(); }
// glitter trail (fixed pool, no per-frame allocation)
const GLIT = Array.from({ length: 40 }, () => ({ x: 0, y: 0, t: -1e9, c: 0, vx: 0 })); let glitI = 0, glitLast = 0;
function drawPill(x, y, t, alpha, scale, full) {
  const P = pillSprites();
  const bob = REDUCED || !full ? 0 : Math.sin(t / 260) * 5;
  y += bob;
  if (full && !REDUCED && t - glitLast > 45) { const g = GLIT[glitI++ % GLIT.length]; g.x = x + rand(-10, 10); g.y = y + rand(-6, 6); g.t = t; g.c = (glitI % 5); g.vx = rand(-12, 12); glitLast = t; }
  if (full && !REDUCED) for (const g of GLIT) { const age = t - g.t; if (age > 900) continue; const k = age / 900;
    ctx.globalAlpha = alpha * (1 - k); star(g.x + g.vx * k, g.y - 12 * k, 4 * (1 - k * 0.5), SPARK_COLS[g.c]); }
  ctx.save(); ctx.translate(x, y); ctx.globalAlpha = alpha;
  if (full) {
    const pulse = REDUCED ? 1 : 1 + 0.12 * Math.sin(t / 150);
    
    ctx.save(); ctx.globalCompositeOperation = 'lighter'; const hs = P.R3 * pulse; ctx.drawImage(P.halo, -hs, -hs, hs * 2, hs * 2); ctx.restore();
  }
  ctx.rotate(REDUCED ? -0.35 : Math.sin(t / 420) * 0.35 - 0.3); ctx.scale(scale * 1.45, scale * 1.45);
  ctx.drawImage(P.cap, -P.cw / 2, -P.ch / 2, P.cw, P.ch);
  if (!REDUCED) { // holographic shimmer sweep across the capsule
    const k = (t / 1100) % 1, sx = -P.L - P.r + k * (2 * P.L + 2 * P.r) * 1.4;
    ctx.save(); P.body(ctx); ctx.clip(); ctx.globalCompositeOperation = 'lighter'; ctx.globalAlpha = alpha * 0.55;
    ctx.fillStyle = `hsl(${(t / 4) % 360},100%,70%)`; ctx.beginPath(); ctx.moveTo(sx, -P.r); ctx.lineTo(sx + 9, -P.r); ctx.lineTo(sx - 1, P.r); ctx.lineTo(sx - 10, P.r); ctx.closePath(); ctx.fill(); ctx.restore();
  }
  ctx.restore(); ctx.globalAlpha = 1;
  if (full && alpha > 0.5) { ctx.lineWidth = 1.5; ctx.strokeStyle = '#111';
    for (let i = 0; i < 5; i++) { const a = (REDUCED ? 0 : t / 380) + i * 1.2566, rr = 36 + (REDUCED ? 0 : 5 * Math.sin(t / 110 + i)), s = 6 + (REDUCED ? 0 : 2 * Math.sin(t / 90 + i * 2));
      star(x + Math.cos(a) * rr, y + Math.sin(a) * rr * 0.8, s, SPARK_COLS[i]); } }
}
// grab payoff: shockwave rings + sparkle burst where the pill was swallowed
const BURSTS = [];
function drawBursts(t) {
  for (let i = BURSTS.length - 1; i >= 0; i--) {
    const b = BURSTS[i], k = (t - b.t) / 700; if (k >= 1) { BURSTS.splice(i, 1); continue; }
    ctx.save(); ctx.translate(b.x, b.y); ctx.globalAlpha = 1 - k;
    for (let j = 0; j < (REDUCED ? 1 : 2); j++) { const kk = clamp(k * 1.3 - j * 0.2, 0, 1); ctx.lineWidth = 8 * (1 - kk) + 1; ctx.strokeStyle = j ? '#FF2E88' : '#00E5FF';
      ctx.beginPath(); ctx.arc(0, 0, 20 + (REDUCED ? 60 : kk * 150), 0, 7); ctx.stroke(); }
    if (!REDUCED) for (let j = 0; j < 14; j++) { const a = j * 0.4488 + b.r, d = 20 + k * 170; star(Math.cos(a) * d, Math.sin(a) * d, 9 * (1 - k) + 2, SPARK_COLS[j % 5]); }
    ctx.restore();
  }
}
// off-screen pointer to the pill, drawn in screen space
function drawPillArrow(pill, t) {
  const sx = (pill.x - cam.x) * cam.z + VW / 2, sy = (pill.y - cam.y) * cam.z + VH / 2, m = 66;
  if (sx > 0 && sx < VW && sy > 0 && sy < VH) return;
  const cx = VW / 2, cy = VH / 2, dx = sx - cx, dy = sy - cy, sc = Math.min((VW / 2 - m) / Math.abs(dx || 1e-6), (VH / 2 - m) / Math.abs(dy || 1e-6));
  const ax = cx + dx * sc, ay = cy + dy * sc, ang = Math.atan2(dy, dx), pul = REDUCED ? 1 : 1 + 0.12 * Math.sin(t / 120);
  ctx.save(); ctx.translate(ax, ay); ctx.scale(pul, pul);
  ctx.fillStyle = 'rgba(17,17,40,.8)'; ctx.strokeStyle = '#00E5FF'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(0, 0, 28, 0, 7); ctx.fill(); ctx.stroke();
  ctx.save(); ctx.rotate(ang); ctx.lineJoin = 'round'; ctx.beginPath(); ctx.moveTo(48, 0); ctx.lineTo(24, -17); ctx.lineTo(24, 17); ctx.closePath();
  ctx.strokeStyle = '#111'; ctx.lineWidth = 7; ctx.stroke(); ctx.fillStyle = '#FF2E88'; ctx.fill(); ctx.strokeStyle = '#00E5FF'; ctx.lineWidth = 2.5; ctx.stroke(); ctx.restore();
  const P = pillSprites(); ctx.rotate(-0.3); ctx.drawImage(P.cap, -P.cw * 0.6, -P.ch * 0.6, P.cw * 1.2, P.ch * 1.2);
  ctx.restore();
}
// tank-wide alert when a new pill appears (works for host + clients, and late joiners)
let lastPillId = -1;
function pillAlert(id) {
  if (id === lastPillId) return; lastPillId = id;
  const el2 = $('#pillAlert'); el2.hidden = false; el2.classList.remove('go'); void el2.offsetWidth; el2.classList.add('go');
  const fl = $('#pillFlash'); if (!REDUCED) { fl.classList.remove('go'); void fl.offsetWidth; fl.classList.add('go'); }
  clearTimeout(pillAlert.t); pillAlert.t = setTimeout(() => { el2.hidden = true; }, 2400);
}
// chomp: cartoon red splat bursting out + a few bubbles + "CHOMP!", gone within ~1.5 s
const SPLAT_MS = 1500;
function mkSplat(x, y, size, t) {
  const n = 12, drops = [], bub = [];
  for (let i = 0; i < n; i++) { const a = (i / n) * Math.PI * 2 + rand(-0.25, 0.25); drops.push({ a, d: rand(50, 110) * Math.max(0.8, size), r: rand(5, 13) * Math.max(0.8, size) }); }
  for (let i = 0; i < 5; i++) bub.push({ dx: rand(-40, 40), r: rand(4, 9), sp: rand(60, 120), ph: rand(0, 6) });
  const bumps = []; for (let i = 0; i < 9; i++) bumps.push(rand(0.75, 1.25));
  return { x, y, s: Math.max(0.8, size), t, drops, bub, bumps };
}
function splatBlob(r, bumps) {
  ctx.beginPath(); const n = bumps.length;
  for (let i = 0; i <= n; i++) { const a = (i / n) * Math.PI * 2, rr = r * bumps[i % n], a0 = ((i - 0.5) / n) * Math.PI * 2, rc = r * 1.35 * bumps[(i + 4) % n];
    if (!i) ctx.moveTo(Math.cos(a) * rr, Math.sin(a) * rr); else ctx.quadraticCurveTo(Math.cos(a0) * rc, Math.sin(a0) * rc, Math.cos(a) * rr, Math.sin(a) * rr); }
  ctx.closePath();
}
function drawSplats(t) {
  V.splats = V.splats.filter((s) => t - s.t < SPLAT_MS);
  for (const s of V.splats) {
    const k = (t - s.t) / SPLAT_MS, fade = k < 0.55 ? 1 : 1 - (k - 0.55) / 0.45;
    const pop = REDUCED ? 1 : Math.min(1, k * 7) * (1 + 0.25 * Math.exp(-k * 10) * Math.sin(k * 40));
    ctx.save(); ctx.translate(s.x, s.y); ctx.globalAlpha = fade; ctx.lineWidth = 3; ctx.strokeStyle = '#111';
    // main splat
    ctx.save(); ctx.scale(pop, pop); splatBlob(34 * s.s, s.bumps); ctx.fillStyle = '#E8143C'; ctx.fill(); ctx.stroke();
    ctx.fillStyle = 'rgba(255,255,255,.55)'; ctx.beginPath(); ctx.ellipse(-10 * s.s, -12 * s.s, 9 * s.s, 5 * s.s, -0.5, 0, 7); ctx.fill(); ctx.restore();
    // droplets fly out (ease-out) and sag a little
    const e = REDUCED ? 0.7 : 1 - Math.pow(1 - Math.min(1, k * 1.8), 3);
    for (const d of s.drops) { const x = Math.cos(d.a) * d.d * e, y = Math.sin(d.a) * d.d * e + (REDUCED ? 0 : 40 * k * k), r = d.r * (REDUCED ? 1 : 1 - 0.4 * k);
      ctx.fillStyle = '#FF2B4F'; ctx.beginPath(); ctx.arc(x, y, r, 0, 7); ctx.fill(); ctx.lineWidth = 2; ctx.stroke();
      ctx.fillStyle = 'rgba(255,255,255,.6)'; ctx.beginPath(); ctx.arc(x - r * 0.35, y - r * 0.35, r * 0.28, 0, 7); ctx.fill(); }
    // bubbles
    if (!REDUCED) for (const b of s.bub) { const y = -b.sp * k * 1.4, x = b.dx + Math.sin(t / 150 + b.ph) * 5;
      ctx.strokeStyle = 'rgba(255,255,255,.9)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(x, y, b.r, 0, 7); ctx.stroke(); }
    ctx.restore();
    if (k < 0.45) { const tk = k / 0.45, sc = REDUCED ? 1 : 0.6 + 0.6 * Math.min(1, tk * 4);
      ctx.save(); ctx.translate(s.x, s.y - 70 * s.s - (REDUCED ? 0 : tk * 16)); ctx.scale(sc, sc); ctx.globalAlpha = 1 - Math.max(0, tk - 0.7) / 0.3;
      ctx.font = '700 34px Bungee, sans-serif'; ctx.textAlign = 'center'; ctx.lineWidth = 7; ctx.strokeStyle = '#111'; ctx.fillStyle = '#FFE600';
      ctx.strokeText('CHOMP!', 0, 0); ctx.fillText('CHOMP!', 0, 0); ctx.restore(); }
  }
}
function drawEats(players, t) {
  const byId = new Map(players.map((p) => [p.id, p]));
  V.eats = V.eats.filter((e) => t - (e.arrive || e.t) < 900);
  for (const e of V.eats) {
    const p = byId.get(e.pid); const dur = e.arrive - e.t, age = t - e.t;
    if (p && age < dur) drawSuck(e, p, age / dur, t);
    const after = t - e.arrive;
    if (p && after >= 0) { // +1 / +2 pop after the gulp
      const k = after / 900; if (k >= 1) continue; ctx.save(); ctx.globalAlpha = 1 - k; ctx.font = `700 ${e.k === 3 ? 30 : 22 + 6 * (e.k - 1)}px Bungee, Fredoka, sans-serif`; ctx.textAlign = 'center';
      ctx.lineWidth = 5; ctx.strokeStyle = '#111'; ctx.fillStyle = e.k === 3 ? '#A970FF' : e.k === 2 ? '#FFC21A' : '#FF5FA8';
      const lab = e.k === 3 ? 'POWER UP!' : '+' + e.k;
      const y = (p._my || p.y) - 20 - k * 50; ctx.strokeText(lab, p._mx || p.x, y); ctx.fillText(lab, p._mx || p.x, y); ctx.restore();
      if (e.k >= 2 && after < 400) drawSparkles(p._mx || p.x, p._my || p.y, after, t);
      if (after < 200) { ctx.save(); ctx.strokeStyle = '#fff'; ctx.lineWidth = 3; const r = 8 + after / 8; for (let i = 0; i < 6; i++) { const a = i * Math.PI / 3; ctx.beginPath(); ctx.moveTo((p._mx || p.x) + Math.cos(a) * r, (p._my || p.y) + Math.sin(a) * r); ctx.lineTo((p._mx || p.x) + Math.cos(a) * (r + 7), (p._my || p.y) + Math.sin(a) * (r + 7)); ctx.stroke(); } ctx.restore(); }
    }
  }
}
// Captured flake: curves into the live mouth, accelerating (ease-in), stretched along its path and shrinking,
// with water streaks / bubbles converging on the mouth.
function drawSuck(e, p, k, t) {
  const mx = p._mx || p.x, my = p._my || p.y;
  const ease = k * k * k;
  const dx = mx - e.x, dy = my - e.y, d = Math.hypot(dx, dy) || 1;
  const bend = Math.sin(k * Math.PI) * Math.min(18, d * 0.35) * (e.fid % 2 ? 1 : -1); // slight curve
  const x = e.x + dx * ease - dy / d * bend, y = e.y + dy * ease + dx / d * bend;
  const ang = Math.atan2(my - y, mx - x), stretch = 1 + 1.4 * k, shrink = 1 - 0.75 * k;
  if (!REDUCED) for (const s of e.parts) { // streaks and bubbles rushing in
    const q = clamp((k - s.d) / (1 - s.d), 0, 1); if (q <= 0 || q >= 1) continue;
    const a = ang + Math.PI + s.a, rr = (1 - q * q) * (28 + s.l * 2) * p.size;
    const px = mx + Math.cos(a) * rr, py = my + Math.sin(a) * rr;
    ctx.save(); ctx.globalAlpha = 0.9 * (1 - q * 0.5); ctx.strokeStyle = '#fff'; ctx.lineCap = 'round'; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(px, py); ctx.lineTo(px + Math.cos(a) * s.l * (0.5 + q), py + Math.sin(a) * s.l * (0.5 + q)); ctx.stroke();
    ctx.fillStyle = 'rgba(255,255,255,.35)'; ctx.beginPath(); ctx.arc(px, py, s.r, 0, 7); ctx.fill(); ctx.stroke();
    ctx.restore();
  }
  if (e.k === 3) { drawPill(x, y, t, 1, shrink * 1.2); return; }
  ctx.save(); ctx.translate(x, y); ctx.rotate(ang); ctx.scale(stretch * shrink, shrink / Math.sqrt(stretch));
  flakePath(FOOD_R * (e.k === 2 ? 1.15 : 1), e.fid);
  ctx.fillStyle = e.k === 2 ? '#FFC21A' : '#FF5FA8'; ctx.fill(); ctx.lineWidth = 2.5 / shrink; ctx.strokeStyle = '#111'; ctx.stroke();
  ctx.restore();
}
function drawSparkles(x, y, after, t) {
  const k = after / 400; ctx.save(); ctx.globalAlpha = 1 - k; ctx.fillStyle = '#FFE600'; ctx.strokeStyle = '#111'; ctx.lineWidth = 1.5;
  for (let i = 0; i < 5; i++) {
    const a = i / 5 * Math.PI * 2 + t / 300, r = 14 + k * 30, s = 6 * (1 - k * 0.6), cx = x + Math.cos(a) * r, cy = y + Math.sin(a) * r;
    ctx.beginPath(); ctx.moveTo(cx, cy - s); ctx.lineTo(cx + s * 0.3, cy); ctx.lineTo(cx, cy + s); ctx.lineTo(cx - s * 0.3, cy); ctx.closePath(); ctx.fill(); ctx.stroke();
  }
  ctx.restore();
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
  else if (ph === 'countdown') { phase = 'GET READY'; timer = set.mode === 'timer' ? `${set.min}:00` : `GOAL ${set.goal}`; const n = Math.ceil(V.cd / 1000); if (n > 0 && V.meta.ph === 'countdown') popCountdown(String(n)); }
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
document.querySelectorAll('#pwSeg button').forEach((b) => { b.onclick = () => hostSet({ pw: b.dataset.pw === '1' }); });
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
if (QS.has('test')) window.__finops = { V, H, cam, w2s: (x, y) => ({ x: (x - cam.x) * cam.z + VW / 2, y: (y - cam.y) * cam.z + VH / 2 }), get role() { return role; }, get myId() { return myId; }, get room() { return room; }, hostSet, hostStart, mouthOf, suckRadius, SHARK_X, CAP_MS, pred, get drawn() { return window.__drawn; }, spawnPill, PW_MS };
})();
