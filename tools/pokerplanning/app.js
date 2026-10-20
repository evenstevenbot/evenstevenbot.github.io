/* evenstevenbot planning poker
 * Real-time via PeerJS (WebRTC data channels, public PeerJS cloud broker for signalling).
 * Topology: host-as-hub. The first person in a room claims the peer id derived from the
 * room code and holds the authoritative state; everyone else connects to that id.
 * If the host leaves, the remaining players race (staggered by join order) to claim the
 * host id; the winner rebuilds state from its last snapshot and the others reconnect.
 */
(() => {
'use strict';
const PREFIX = 'evenstevenbot-poker-v1-';
const DECK = ['0','1','2','3','5','8','13','21','?'];
const PRESETS = [[30,'30s'],[60,'1m'],[120,'2m'],[300,'5m']];
const AVATAR_BASE = 'https://api.dicebear.com/10.x/notionists/svg';
const AVATAR_KEY = 'esb-poker-avatar';
const SEED_MAX = 32;
const OPT_JSON_MAX = 320;
const SEED_RE = /^[a-z0-9][a-z0-9_-]{0,31}$/;
const variantList = (n) => Array.from({ length: n }, (_, i) => 'variant' + String(i + 1).padStart(2, '0'));
// Allowlisted Notionists features. Values are copied onto the DiceBear URL ourselves;
// anything else a peer sends is dropped. Optional features omitted from `o` stay off.
const AVATAR_OPTS = {
  hair:    { param: 'hairVariant', prob: 'hairProbability', values: ['hat', ...variantList(63)] },
  glasses: { param: 'glassesVariant', prob: 'glassesProbability', values: variantList(11), optional: true },
  beard:   { param: 'beardVariant', prob: 'beardProbability', values: variantList(12), optional: true },
  eyes:    { param: 'eyesVariant', prob: 'eyesProbability', values: variantList(5) },
  brows:   { param: 'eyebrowsVariant', prob: 'eyebrowsProbability', values: variantList(13) },
  mouth:   { param: 'mouthVariant', prob: 'mouthProbability', values: variantList(30) },
  nose:    { param: 'noseVariant', prob: 'noseProbability', values: variantList(20) },
  clothes: { param: 'clothesVariant', prob: 'clothesProbability', values: variantList(25) },
  gesture: { param: 'gestureVariant', prob: 'gestureProbability', values: ['hand','handPhone','ok','okLongArm','point','pointLongArm','waveLongArm','waveLongArms','waveOkLongArms','wavePointLongArms'], optional: true },
};
const PICKER_COUNT = 12;
const TINTS = ['#FFE3EF','#D9FAFF','#FFF9C2','#FFE6D5','#ECEAE4'];
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const HEARTBEAT = 2500, TIMEOUT = 9000;
// ICE: direct (host/STUN) candidates are always tried first; TURN relay is the fallback for
// networks that block UDP (Zscaler etc.). TURN over TCP/TLS 443 needs credentials:
// set METERED_TURN_URL to a Metered credentials URL (free account; the API key is safe in front-end code), e.g.
// 'https://<app>.metered.live/api/v1/turn/credentials?apiKey=<KEY>'. Or list static servers in TURN_SERVERS.
const STUN_SERVERS = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
const TURN_SERVERS = [
  { urls: 'stun:stun.relay.metered.ca:80' },
  { urls: 'turn:global.relay.metered.ca:80', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turn:global.relay.metered.ca:80?transport=tcp', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turn:global.relay.metered.ca:443', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
  { urls: 'turns:global.relay.metered.ca:443?transport=tcp', username: '11e7f7e524b90908684f5501', credential: '/Qxb9srjvqvqJtBJ' },
];
const METERED_TURN_URL = '';
const IS_LOCAL = /^(localhost|127\.0\.0\.1)$/.test(location.hostname);
const FORCE_RELAY = IS_LOCAL && new URLSearchParams(location.search).has('relayonly');   // test hook
const BLOCKED_MSG = "Can't reach the room server. Your network may be blocking it. Ask IT to allow it, or try another network.";
let peerOpts = { debug: 0 };
async function loadIce() {
  let turn = TURN_SERVERS;
  if (METERED_TURN_URL) {
    try {
      const ac = new AbortController(); const t = setTimeout(() => ac.abort(), 4000);
      const r = await fetch(METERED_TURN_URL, { signal: ac.signal }); clearTimeout(t);
      if (r.ok) turn = turn.concat(await r.json());
    } catch {}
  }
  peerOpts = { debug: window.__pdebug || 0, config: { iceServers: STUN_SERVERS.concat(turn), iceTransportPolicy: FORCE_RELAY ? 'relay' : 'all' } };
}

const $ = (s) => document.querySelector(s);
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
const now = () => Date.now();
const uid = () => (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2) + now().toString(36)).replace(/-/g,'').slice(0,16);
const hash = (s) => { let h = 0; for (const c of s) h = (h * 31 + c.charCodeAt(0)) | 0; return Math.abs(h); };
function sanitizeSeed(raw) {
  if (typeof raw !== 'string') return '';
  const s = raw.trim().toLowerCase().slice(0, SEED_MAX);
  return SEED_RE.test(s) ? s : '';
}
function sanitizeOptions(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const keys = Object.keys(raw);
  if (keys.length > 12) return null;
  let json;
  try { json = JSON.stringify(raw); } catch { return null; }
  if (json.length > OPT_JSON_MAX) return null;
  const out = {};
  for (const key of Object.keys(AVATAR_OPTS)) {
    if (!Object.prototype.hasOwnProperty.call(raw, key)) continue;
    const val = raw[key];
    if (typeof val !== 'string') continue;
    const v = val.slice(0, 24);
    if (AVATAR_OPTS[key].values.includes(v)) out[key] = v;
  }
  return Object.keys(out).length ? out : null;
}
function sanitizeAvatar(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const seed = sanitizeSeed(raw.seed);
  if (!seed) return null;
  const o = sanitizeOptions(raw.o);
  return o ? { seed, o } : { seed };
}
function legacySeed(name) {
  const s = String(name || 'guest').trim().toLowerCase().slice(0, 24);
  return s || 'guest';
}
function avatarUrl(spec) {
  const params = new URLSearchParams();
  if (spec && spec.legacy) {
    params.set('seed', legacySeed(spec.seed));
    return AVATAR_BASE + '?' + params.toString();
  }
  const clean = sanitizeAvatar(spec) || { seed: 'guest' };
  params.set('seed', clean.seed);
  if (clean.o) {
    for (const [key, specOpt] of Object.entries(AVATAR_OPTS)) {
      const val = clean.o[key];
      if (val && specOpt.values.includes(val)) {
        params.set(specOpt.param, val);
        params.set(specOpt.prob, '100');
      } else if (specOpt.optional) {
        params.set(specOpt.prob, '0');
      }
    }
  }
  return AVATAR_BASE + '?' + params.toString();
}
function tintFor(spec) {
  return TINTS[hash(String((spec && spec.seed) || 'guest').toLowerCase()) % TINTS.length];
}
function paintAvatar(img, spec, size) {
  img.src = avatarUrl(spec);
  img.alt = '';
  img.width = size;
  img.height = size;
  img.decoding = 'async';
  img.referrerPolicy = 'no-referrer';
  img.style.background = tintFor(spec);
}
function loadStoredAvatar() {
  try {
    const raw = localStorage.getItem(AVATAR_KEY);
    if (!raw || raw.length > 600) return null;
    return sanitizeAvatar(JSON.parse(raw));
  } catch { return null; }
}
function storeAvatar(av) {
  const clean = sanitizeAvatar(av);
  if (!clean) return null;
  me.avatar = clean;
  try { localStorage.setItem(AVATAR_KEY, JSON.stringify(clean)); } catch {}
  return clean;
}
function randInt(n) { return crypto.getRandomValues(new Uint32Array(1))[0] % n; }
function randomSeed() {
  const alphabet = 'abcdefghjkmnpqrstuvwxyz23456789';
  const bytes = crypto.getRandomValues(new Uint8Array(10));
  return Array.from(bytes, (n) => alphabet[n % alphabet.length]).join('');
}
function randomOptions() {
  const o = {};
  const onAt = { glasses: 115, beard: 72, gesture: 56 };
  for (const [key, spec] of Object.entries(AVATAR_OPTS)) {
    if (spec.optional && crypto.getRandomValues(new Uint8Array(1))[0] >= (onAt[key] || 80)) continue;
    o[key] = spec.values[randInt(spec.values.length)];
  }
  return o;
}
function randomAvatar() { return sanitizeAvatar({ seed: randomSeed(), o: randomOptions() }); }
const fmt = (ms) => { const s = Math.ceil(Math.max(0, ms) / 1000); return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); };
const cleanCode = (s) => (s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
const newCode = () => Array.from(crypto.getRandomValues(new Uint32Array(6)), (n) => CODE_CHARS[n % CODE_CHARS.length]).join('');

// ---- identity -------------------------------------------------------------
const me = {
  id: sessionStorage.getItem('esb-poker-id') || uid(),
  name: localStorage.getItem('esb-poker-name') || '',
  avatar: null,
  vote: null, round: 0, joinedAt: 0,
};
me.avatar = loadStoredAvatar();
sessionStorage.setItem('esb-poker-id', me.id);

// ---- network state ----------------------------------------------------------
let room = null;
let peer = null;
let role = 'idle';          // idle | connecting | host | client
let hostConn = null;
let lastHostMsg = 0;
let lastView = null;        // latest state snapshot (client: from host; host: own)
let S = null;               // authoritative state (host only)
const conns = new Map();    // host: DataConnection -> { pid, last }
let attempts = 0;
let recovering = false;
let retryTimer = null;
let hbTimer = null;
let tickTimer = null;
let localEndsAt = 0;        // client-side countdown target
let leaving = false;

const hostPeerId = (r) => PREFIX + r;

function setStatus(kind, text) {
  const s = $('#status'); s.className = 'status ' + kind; s.lastElementChild.textContent = text;
}
function banner(text) { const b = $('#banner'); if (text) { b.textContent = text; b.hidden = false; } else b.hidden = true; }
let toastT;
function toast(msg) { const t = $('#toast'); t.textContent = msg; t.classList.add('show'); clearTimeout(toastT); toastT = setTimeout(() => t.classList.remove('show'), 1800); }

function destroyPeer() {
  clearTimeout(retryTimer);
  if (hostConn) { try { hostConn.close(); } catch {} hostConn = null; }
  for (const c of conns.keys()) { try { c.close(); } catch {} }
  conns.clear();
  if (peer) { const p = peer; peer = null; try { p.destroy(); } catch {} }
}

function retryLater(fn, why) {
  attempts++;
  const delay = Math.min(1000 * attempts, 6000) + Math.random() * 400;
  setStatus('warn', why || 'reconnecting…');
  if (attempts >= 4) { banner(BLOCKED_MSG); setStatus('err', 'can\'t connect'); }
  clearTimeout(retryTimer);
  retryTimer = setTimeout(fn, delay);
}

// Step 1: try to become host by claiming the room's peer id.
function tryClaimHost() {
  if (leaving) return;
  destroyPeer();
  role = 'connecting';
  setStatus('warn', 'connecting…');
  const p = new Peer(hostPeerId(room), peerOpts);
  peer = p;
  p.on('open', () => { if (peer === p) becomeHost(); });
  p.on('connection', (c) => { if (peer === p) onHostConnection(c); });
  p.on('disconnected', () => { if (peer === p && !p.destroyed && role === 'host') { setStatus('warn', 'reconnecting…'); try { p.reconnect(); } catch {} } });
  p.on('error', (err) => {
    if (peer !== p) return;
    if (err.type === 'unavailable-id') { connectAsClient(); }           // someone already hosts
    else if (role === 'host' && ['network','server-error','socket-error','socket-closed'].includes(err.type)) {
      setStatus('warn', 'broker hiccup…');                                  // keep serving existing peers
      setTimeout(() => { if (peer === p && p.disconnected && !p.destroyed) try { p.reconnect(); } catch {} }, 2000);
    }
    else if (role !== 'host') retryLater(tryClaimHost, 'retrying…');
  });
}

// Step 2: join as a client of whoever holds the host id.
function connectAsClient() {
  if (leaving) return;
  destroyPeer();
  role = 'connecting';
  const p = new Peer(peerOpts);
  peer = p;
  p.on('open', () => {
    if (peer !== p) return;
    const c = p.connect(hostPeerId(room), { reliable: true, serialization: 'json' });
    hostConn = c;
    const t = setTimeout(() => { if (hostConn === c && !c.open) hostLost('timeout'); }, 12000);
    c.on('open', () => {
      if (hostConn !== c) return;
      clearTimeout(t);
      role = 'client'; attempts = 0; recovering = false; lastHostMsg = now();
      banner(null); setStatus('ok', 'connected');
      sendHello();
    });
    c.on('data', (d) => { if (hostConn === c) onClientData(d); });
    c.on('close', () => { if (hostConn === c) hostLost('closed'); });
    c.on('error', () => { if (hostConn === c) hostLost('error'); });
  });
  p.on('error', (err) => {
    if (peer !== p) return;
    if (err.type === 'peer-unavailable') hostLost('unavailable');
    else retryLater(connectAsClient, 'retrying…');
  });
  p.on('disconnected', () => { if (peer === p && role === 'client' && !p.destroyed) try { p.reconnect(); } catch {} });
}

// Host is gone: stagger by join order so the longest-standing player takes over first.
function hostLost(reason) {
  if (leaving) return;
  const wasConnected = role === 'client';
  if (hostConn) { const c = hostConn; hostConn = null; try { c.close(); } catch {} }
  role = 'connecting';
  let rank = 0;
  if (lastView && wasConnected) {
    const others = lastView.players.filter((p) => p.id !== lastView.hostId).sort((a, b) => a.joinedAt - b.joinedAt);
    rank = Math.max(0, others.findIndex((p) => p.id === me.id));
    const hostName = (lastView.players.find((p) => p.id === lastView.hostId) || {}).name || 'The host';
    banner(`${hostName} (host) left. Reconnecting the room… your vote and the timer are kept.`);
  } else if (lastView) {
    banner('Lost the connection to the room host. Reconnecting…');
  }
  setStatus('warn', 'reconnecting…');
  recovering = true;
  attempts++;
  if (!wasConnected && attempts >= 4) { banner("Can't connect to the room host. Your network may be blocking peer-to-peer connections. Ask IT to allow it, or try another network."); setStatus('err', 'can\'t connect'); }
  const delay = (wasConnected ? rank * 1500 : Math.min(800 * attempts, 5000)) + Math.random() * 300;
  clearTimeout(retryTimer);
  retryTimer = setTimeout(tryClaimHost, delay);
}

// ---- host ---------------------------------------------------------------
function becomeHost() {
  const migrated = !!lastView && recovering;
  role = 'host'; attempts = 0; recovering = false;
  const prev = lastView;
  S = {
    story: prev ? prev.story : '',
    revealed: prev ? prev.revealed : false,
    round: prev ? prev.round : 1,
    autoReveal: prev ? prev.autoReveal : false,
    players: {},
    timer: { duration: 60000, remaining: 60000, running: false, endsAt: 0, expired: false },
  };
  if (prev && prev.timer) {
    const t = prev.timer;
    const rem = t.running ? Math.max(0, localEndsAt - now()) : t.remaining;
    S.timer = { duration: t.duration, remaining: rem, running: t.running && rem > 0, endsAt: now() + rem, expired: t.expired };
  }
  // keep revealed votes of people we knew about, they will re-hello shortly
  if (prev && prev.revealed) {
    for (const p of prev.players) {
      if (p.id === prev.hostId) continue;
      const row = { name: p.name, vote: p.vote, joinedAt: p.joinedAt, ghost: now() };
      const av = sanitizeAvatar(p.avatar);
      if (av) row.avatar = av;
      S.players[p.id] = row;
    }
  }
  if (me.round !== S.round) { me.vote = null; me.round = S.round; }
  const self = { name: me.name, vote: me.vote, joinedAt: me.joinedAt };
  if (me.avatar) self.avatar = me.avatar;
  S.players[me.id] = self;
  banner(null);
  setStatus('ok', 'hosting');
  if (migrated) toast('You are now hosting this room');
  hostBroadcast();
}

function onHostConnection(c) {
  conns.set(c, { pid: null, last: now() });
  c.on('data', (d) => hostHandle(c, d));
  c.on('close', () => dropConn(c));
  c.on('error', () => dropConn(c));
}
function dropConn(c) {
  const info = conns.get(c); if (!info) return;
  conns.delete(c);
  try { c.close(); } catch {}
  if (info.pid && S && ![...conns.values()].some((i) => i.pid === info.pid)) { delete S.players[info.pid]; hostBroadcast(); }
}

function hostHandle(c, d) {
  const info = conns.get(c); if (!info || !d || typeof d !== 'object') return;
  info.last = now();
  if (d.t === 'ping') return;
  if (d.t === 'hello') {
    const pid = String(d.id || '').slice(0, 32); if (!pid) return;
    for (const [oc, oi] of conns) if (oc !== c && oi.pid === pid) { conns.delete(oc); try { oc.close(); } catch {} }
    info.pid = pid;
    const existing = S.players[pid];
    const sameRound = d.round === S.round;
    let avatar = existing && existing.avatar;
    if (Object.prototype.hasOwnProperty.call(d, 'avatar')) avatar = sanitizeAvatar(d.avatar) || avatar;
    const row = {
      name: String(d.name || 'guest').slice(0, 24),
      vote: sameRound && DECK.includes(d.vote) ? d.vote : (existing ? existing.vote : null),
      joinedAt: Number(d.joinedAt) || now(),
    };
    if (avatar) row.avatar = avatar;
    S.players[pid] = row;
    hostBroadcast();
    return;
  }
  // Additive. Older hosts ignore unknown `t` values, so this does not disturb them.
  if (d.t === 'avatar') {
    if (!info.pid || !S || !S.players[info.pid]) return;
    const av = sanitizeAvatar({ seed: d.seed, o: d.o });
    if (!av) return;
    S.players[info.pid].avatar = av;
    hostBroadcast();
    return;
  }
  if (!info.pid || !S.players[info.pid]) return;
  if (d.t === 'vote') applyVote(info.pid, d.value);
  else if (d.t === 'act') applyAction(d);
}

function applyVote(pid, value) {
  if (S.revealed) return;
  if (value !== null && !DECK.includes(value)) return;
  S.players[pid].vote = value;
  hostBroadcast();
}

function applyAction(d) {
  const t = S.timer;
  switch (d.a) {
    case 'reveal': S.revealed = true; break;
    case 'reset':
      S.revealed = false; S.round++;
      for (const p of Object.values(S.players)) p.vote = null;
      t.running = false; t.remaining = t.duration; t.expired = false;
      break;
    case 'story': S.story = String(d.v || '').slice(0, 140); break;
    case 'autoReveal': S.autoReveal = !!d.v; break;
    case 'timerSet': {
      const ms = Math.max(5, Math.min(3600, Number(d.v) || 60)) * 1000;
      t.duration = ms; t.remaining = ms; t.running = false; t.expired = false; break;
    }
    case 'timerToggle':
      if (t.running) { t.remaining = Math.max(0, t.endsAt - now()); t.running = false; }
      else { if (t.remaining <= 0 || t.expired) { t.remaining = t.duration; t.expired = false; } t.endsAt = now() + t.remaining; t.running = true; }
      break;
    case 'timerReset': t.running = false; t.remaining = t.duration; t.expired = false; break;
    default: return;
  }
  hostBroadcast();
}

function makeView() {
  const t = S.timer;
  const players = Object.entries(S.players)
    .map(([id, p]) => {
      const row = { id, name: p.name, joinedAt: p.joinedAt, voted: p.vote != null, vote: S.revealed ? p.vote : null };
      const av = sanitizeAvatar(p.avatar);
      if (av) row.avatar = av;
      return row;
    })
    .sort((a, b) => a.joinedAt - b.joinedAt);
  return {
    t: 'state', hostId: me.id, story: S.story, revealed: S.revealed, round: S.round, autoReveal: S.autoReveal, players,
    timer: { duration: t.duration, remaining: t.running ? Math.max(0, t.endsAt - now()) : t.remaining, running: t.running, expired: t.expired },
  };
}
function hostBroadcast() {
  if (role !== 'host') return;
  const v = makeView();
  for (const [c, info] of conns) if (c.open && info.pid) { try { c.send(v); } catch {} }
  applyView(v);
}
function hostTick() {
  if (role !== 'host' || !S) return;
  const t = S.timer;
  if (t.running && now() >= t.endsAt) {
    t.running = false; t.remaining = 0; t.expired = true;
    if (S.autoReveal) S.revealed = true;
    hostBroadcast();
  }
}

// ---- client ---------------------------------------------------------------
function sendHello() {
  if (!(hostConn && hostConn.open)) return;
  const msg = { t: 'hello', id: me.id, name: me.name, vote: me.vote, round: me.round, joinedAt: me.joinedAt };
  if (me.avatar) msg.avatar = me.avatar;
  hostConn.send(msg);
}
function publishAvatar() {
  const av = me.avatar;
  if (!av) return;
  if (role === 'host') {
    if (S && S.players[me.id]) { S.players[me.id].avatar = av; hostBroadcast(); }
    return;
  }
  if (role === 'client' && hostConn && hostConn.open) {
    try { hostConn.send({ t: 'avatar', seed: av.seed, o: av.o }); } catch {}
  }
}
function onClientData(d) {
  lastHostMsg = now();
  if (!d || typeof d !== 'object') return;
  if (d.t === 'bye') { hostLost('bye'); return; }
  if (d.t === 'state') applyView(d);
}
function send(msg) {
  if (role === 'host') { if (msg.t === 'vote') applyVote(me.id, msg.value); else applyAction(msg); }
  else if (role === 'client' && hostConn && hostConn.open) hostConn.send(msg);
  else toast('Reconnecting… try again in a moment');
}

// heartbeat both ways + ghost cleanup
function heartbeat() {
  if (role === 'client') {
    if (hostConn && hostConn.open) try { hostConn.send({ t: 'ping' }); } catch {}
    if (now() - lastHostMsg > TIMEOUT) hostLost('silent');
  } else if (role === 'host') {
    let changed = false;
    for (const [c, info] of conns) {
      if (now() - info.last > TIMEOUT) { dropConn(c); continue; }
      if (c.open) try { c.send({ t: 'ping' }); } catch {}
    }
    for (const [id, p] of Object.entries(S.players)) {
      if (p.ghost && ![...conns.values()].some((i) => i.pid === id) && now() - p.ghost > 15000) { delete S.players[id]; changed = true; }
      else if (p.ghost && [...conns.values()].some((i) => i.pid === id)) delete p.ghost;
    }
    if (changed) hostBroadcast();
  }
}

// ---- rendering ---------------------------------------------------------------
function applyView(v) {
  lastView = v;
  if (v.round !== me.round) { me.vote = null; me.round = v.round; }
  if (v.timer) localEndsAt = now() + v.timer.remaining;
  render();
}

function render() {
  const v = lastView; if (!v) return;
  // story (don't clobber while typing)
  const si = $('#storyInput');
  if (document.activeElement !== si && si.value !== v.story) si.value = v.story;

  // players
  const ul = $('#players'); ul.textContent = '';
  const justRevealed = v.revealed && !(render.prev && render.prev.revealed && render.prev.round === v.round);
  render.prev = { revealed: v.revealed, round: v.round };
  const shown = (x) => (typeof x === 'string' && DECK.includes(x) ? x : null);
  const numeric = v.revealed ? v.players.map((p) => shown(p.vote)).filter((x) => x != null && /^\d+$/.test(x)).map(Number) : [];
  const cast = v.revealed ? v.players.map((p) => shown(p.vote)).filter((x) => x != null) : [];
  const consensus = v.revealed && cast.length >= 2 && cast.every((x) => x === cast[0]) && /^\d+$/.test(cast[0]);
  for (const p of v.players) {
    const li = el('li', 'player');
    const mine = p.id === me.id;
    const spec = avatarSpecFor(p);
    const img = el('img');
    paintAvatar(img, spec, 72);
    img.loading = 'lazy';
    const av = el('div', 'av');
    av.append(img);
    const card = el('div', 'pcard');
    const vote = shown(p.vote);
    let state;
    if (v.revealed && vote != null) { card.classList.add('shown'); if (justRevealed) card.classList.add('flip'); card.textContent = vote; if (consensus) card.classList.add('match'); state = `voted ${vote}`; }
    else if (p.voted || (mine && shown(me.vote) != null)) { card.classList.add('voted'); card.textContent = '✓'; state = 'has voted'; }
    else { card.textContent = v.revealed ? '–' : ''; state = v.revealed ? 'did not vote' : 'thinking'; }
    const name = el('div', 'name', p.name + (mine ? ' (you)' : ''));
    if (mine) {
      li.classList.add('me');
      const hint = el('span', 'seat-hint', 'change');
      hint.setAttribute('aria-hidden', 'true');
      const seat = el('button', 'seat');
      seat.type = 'button';
      seat.title = 'Change avatar';
      seat.setAttribute('aria-haspopup', 'dialog');
      seat.setAttribute('aria-label', `Change your avatar, ${p.name}`);
      seat.append(av, name, hint);
      seat.addEventListener('click', openPicker);
      li.append(card, seat);
    } else {
      li.append(card, av, name);
    }
    if (p.id === v.hostId) li.append(el('div', 'tag', 'host'));
    li.setAttribute('aria-label', `${p.name}${mine ? ' (you)' : ''}: ${state}`);
    ul.append(li);
  }
  const votedCount = v.players.filter((p) => p.voted).length;
  $('#voteHint').textContent = v.revealed ? `round ${v.round} · cards revealed` : `${votedCount} of ${v.players.length} voted`;
  $('#revealBtn').hidden = v.revealed;
  $('#revealBtn').disabled = votedCount === 0;
  $('#resetBtn').classList.toggle('pink', v.revealed);

  // results
  $('#results').hidden = !v.revealed;
  if (v.revealed) {
    $('#avg').textContent = numeric.length ? (Math.round(numeric.reduce((a, b) => a + b, 0) / numeric.length * 10) / 10).toString() : '–';
    $('#voteCount').textContent = cast.length;
    const counts = {}; for (const x of cast) counts[x] = (counts[x] || 0) + 1;
    const max = Math.max(0, ...Object.values(counts));
    const dist = $('#dist'); dist.textContent = '';
    for (const k of DECK) if (counts[k]) { const c = el('span', 'chip' + (counts[k] === max && cast.length > 1 ? ' top' : ''), k); c.append(el('small', null, '×' + counts[k])); dist.append(c); }
    const cons = $('#consensus'); cons.hidden = !consensus; cons.classList.toggle('anim', justRevealed);
  }

  // deck
  const deck = $('#deck');
  for (const b of deck.children) { b.setAttribute('aria-pressed', String(b.dataset.v === me.vote)); b.disabled = v.revealed; }
  const myPick = shown(me.vote);
  $('#deckLabel').textContent = v.revealed ? 'cards revealed · start a new round to vote again' : (myPick != null ? `you picked ${myPick} · tap again to undo` : 'pick a card');

  // timer controls
  for (const b of $('#presets').children) b.setAttribute('aria-pressed', String(Number(b.dataset.s) * 1000 === v.timer.duration));
  $('#timerStart').textContent = v.timer.running ? 'pause' : (v.timer.remaining < v.timer.duration && v.timer.remaining > 0 ? 'resume' : 'start');
  $('#autoReveal').checked = !!v.autoReveal;
  renderTimer();
}

let lastExpired = false;
function renderTimer() {
  const v = lastView; if (!v) return;
  const t = v.timer;
  const rem = t.running ? Math.max(0, localEndsAt - now()) : t.remaining;
  $('#timeText').textContent = fmt(rem);
  const frac = t.duration ? rem / t.duration : 0;
  $('#timerProg').style.strokeDashoffset = String(326.73 * (1 - frac));
  const box = $('#timer');
  box.classList.toggle('low', t.running && rem <= 10000);
  box.classList.toggle('expired', !!t.expired);
  $('#timesUp').textContent = t.expired ? "time's up!" : '';
  if (t.expired && !lastExpired) chime();
  lastExpired = !!t.expired;
}
function chime() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const o = ctx.createOscillator(), g = ctx.createGain();
    o.type = 'sine'; o.frequency.value = 660; g.gain.value = 0.0001;
    o.connect(g); g.connect(ctx.destination); o.start();
    g.gain.exponentialRampToValueAtTime(0.08, ctx.currentTime + 0.05);
    g.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.9);
    o.stop(ctx.currentTime + 1);
  } catch {}
}

function avatarSpecFor(player) {
  if (player && player.id === me.id && me.avatar) return me.avatar;
  const net = player && sanitizeAvatar(player.avatar);
  if (net) return net;
  return { seed: legacySeed(player && player.name), legacy: true };
}
function previewSpec() {
  if (me.avatar) return me.avatar;
  const typed = $('#nameInput') ? $('#nameInput').value : '';
  const name = (typed || '').trim() || me.name || 'evenstevenbot';
  return { seed: legacySeed(name), legacy: true };
}
function refreshJoinAvatar() {
  const img = $('#joinAvatar');
  if (img) paintAvatar(img, previewSpec(), 112);
}
let pickerOptions = [];
function fillPickerGrid() {
  const grid = $('#pickerGrid');
  grid.textContent = '';
  pickerOptions.forEach((opt, i) => {
    const b = el('button', 'picker-opt');
    b.type = 'button';
    b.setAttribute('role', 'option');
    b.setAttribute('aria-selected', 'false');
    b.setAttribute('aria-label', `Notionist option ${i + 1}`);
    const img = el('img');
    paintAvatar(img, opt, 96);
    b.append(img);
    b.addEventListener('click', () => chooseAvatar(opt));
    grid.append(b);
  });
}
function shufflePicker() {
  const seen = new Set([avatarUrl(previewSpec())]);
  const next = [];
  let guard = 0;
  while (next.length < PICKER_COUNT && guard < 48) {
    guard++;
    const opt = randomAvatar();
    if (!opt) continue;
    const url = avatarUrl(opt);
    if (seen.has(url)) continue;
    seen.add(url);
    next.push(opt);
  }
  pickerOptions = next;
  fillPickerGrid();
}
function openPicker() {
  const dlg = $('#avatarPicker');
  if (!dlg || dlg.open) return;
  refreshJoinAvatar();
  paintAvatar($('#pickerCurrent'), previewSpec(), 96);
  shufflePicker();
  if (typeof dlg.showModal === 'function') dlg.showModal();
}
function closePicker() {
  const dlg = $('#avatarPicker');
  if (dlg && dlg.open && typeof dlg.close === 'function') dlg.close();
}
function chooseAvatar(opt) {
  const clean = storeAvatar(opt);
  if (!clean) return;
  publishAvatar();
  refreshJoinAvatar();
  if (lastView) render();
  closePicker();
  toast('Avatar saved');
}

// ---- UI wiring ----------------------------------------------------------------
function buildStatic() {
  const deck = $('#deck');
  for (const v of DECK) {
    const b = el('button', 'card', v); b.type = 'button'; b.dataset.v = v;
    b.setAttribute('aria-label', v === '?' ? 'not sure' : `${v} points`);
    b.setAttribute('aria-pressed', 'false');
    b.addEventListener('click', () => {
      if (!lastView || lastView.revealed) return;
      me.vote = me.vote === v ? null : v;
      send({ t: 'vote', value: me.vote });
      render();
    });
    deck.append(b);
  }
  const pre = $('#presets');
  for (const [s, label] of PRESETS) {
    const b = el('button', 'preset', label); b.type = 'button'; b.dataset.s = s; b.setAttribute('aria-pressed', 'false');
    b.setAttribute('aria-label', `set timer to ${label}`);
    b.addEventListener('click', () => send({ t: 'act', a: 'timerSet', v: s }));
    pre.append(b);
  }
  $('#revealBtn').addEventListener('click', () => send({ t: 'act', a: 'reveal' }));
  $('#resetBtn').addEventListener('click', () => { me.vote = null; send({ t: 'act', a: 'reset' }); });
  $('#timerStart').addEventListener('click', () => send({ t: 'act', a: 'timerToggle' }));
  $('#timerReset').addEventListener('click', () => send({ t: 'act', a: 'timerReset' }));
  $('#autoReveal').addEventListener('change', (e) => send({ t: 'act', a: 'autoReveal', v: e.target.checked }));
  let storyT;
  $('#storyInput').addEventListener('input', (e) => { clearTimeout(storyT); storyT = setTimeout(() => send({ t: 'act', a: 'story', v: e.target.value }), 250); });
  $('#storyInput').addEventListener('blur', (e) => { clearTimeout(storyT); if (lastView && e.target.value !== lastView.story) send({ t: 'act', a: 'story', v: e.target.value }); });
  const copy = async () => {
    const url = shareUrl();
    try { await navigator.clipboard.writeText(url); toast('Room link copied'); }
    catch { const i = $('#shareUrl'); i.select(); document.execCommand && document.execCommand('copy'); toast('Room link copied'); }
  };
  $('#copyLink').addEventListener('click', copy);
  $('#copyLink2').addEventListener('click', copy);
  $('#shareUrl').addEventListener('focus', (e) => e.target.select());
  $('#leave').addEventListener('click', leaveRoom);
  $('#pickerKeep').addEventListener('click', closePicker);
  $('#pickerShuffle').addEventListener('click', shufflePicker);
  $('#avatarPicker').addEventListener('click', (e) => { if (e.target === $('#avatarPicker')) closePicker(); });
  $('#joinAvatarBtn').addEventListener('click', openPicker);
}

function shareUrl() { const u = new URL(location.href); u.search = '?room=' + room; u.hash = ''; return u.toString(); }

function initJoin() {
  const params = new URLSearchParams(location.search);
  const code = cleanCode(params.get('room'));
  const ni = $('#nameInput');
  ni.value = me.name;
  const upd = () => refreshJoinAvatar();
  let t; ni.addEventListener('input', () => { clearTimeout(t); t = setTimeout(upd, 250); });
  upd();
  if (code) {
    $('#joinBtn').textContent = `join room ${code}`;
    $('#joinSub').textContent = `You've been invited to room ${code}. Pick a name and jump in.`;
    $('#orRow').hidden = true; $('#codeRow').hidden = true;
  }
  const go = (roomCode) => {
    const name = ni.value.trim().slice(0, 24);
    if (!name) { $('#joinError').textContent = 'Please type your name first.'; ni.focus(); return; }
    me.name = name; localStorage.setItem('esb-poker-name', name);
    enterRoom(roomCode);
  };
  $('#joinForm').addEventListener('submit', (e) => { e.preventDefault(); go(code || newCode()); });
  $('#joinCodeBtn').addEventListener('click', () => {
    const c = cleanCode($('#codeInput').value);
    if (c.length < 4) { $('#joinError').textContent = 'Enter a room code (6 letters/numbers).'; $('#codeInput').focus(); return; }
    go(c);
  });
  $('#codeInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); $('#joinCodeBtn').click(); } });
  if (!me.name) ni.focus();
}

function enterRoom(code) {
  if (typeof Peer === 'undefined') { $('#joinError').textContent = 'Could not load the real-time library. Check your connection and reload.'; return; }
  room = code; leaving = false;
  me.joinedAt = Number(sessionStorage.getItem('esb-poker-joined-' + code)) || now();
  sessionStorage.setItem('esb-poker-joined-' + code, me.joinedAt);
  history.replaceState(null, '', '?room=' + code);
  document.title = `room ${code} · planning poker`;
  $('#roomCode').textContent = code;
  $('#shareUrl').value = shareUrl();
  $('#joinView').hidden = true; $('#roomView').hidden = false; $('#roomBar').hidden = false;
  // provisional local view until connected
  lastView = { hostId: null, story: '', revealed: false, round: 0, autoReveal: false, players: [{ id: me.id, name: me.name, joinedAt: me.joinedAt, voted: false, vote: null }], timer: { duration: 60000, remaining: 60000, running: false, expired: false } };
  render();
  lastView = null;
  loadIce().then(tryClaimHost);
  clearInterval(hbTimer); hbTimer = setInterval(heartbeat, HEARTBEAT);
  clearInterval(tickTimer); tickTimer = setInterval(() => { hostTick(); renderTimer(); }, 250);
}

function sayBye() {
  if (role === 'host') for (const c of conns.keys()) { try { if (c.open) c.send({ t: 'bye' }); } catch {} }
}
function leaveRoom() {
  leaving = true; sayBye();
  setTimeout(() => { destroyPeer(); location.href = location.pathname; }, 150);
}
window.addEventListener('pagehide', () => { leaving = true; sayBye(); destroyPeer(); });

// debug hook for automated tests
window.__poker = { get role() { return role; }, get view() { return lastView; }, get me() { return me; }, sanitizeAvatar, avatarUrl };

buildStatic();
initJoin();
})();
