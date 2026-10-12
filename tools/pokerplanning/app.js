/* evenstevenbot planning poker
 * Real-time via PeerJS (WebRTC data channels, public PeerJS cloud broker for signalling).
 * Topology: host-as-hub. The first person in a room claims the peer id derived from the
 * room code and holds the authoritative state; everyone else connects to that id.
 * If the host leaves, the remaining players race (staggered by join order) to claim the
 * host id; the winner rebuilds state from its last snapshot and the others reconnect.
 *
 * Relay mode (preferred, works behind corporate proxies like Zscaler): when RELAY_URL is set,
 * the room lives in a Cloudflare Durable Object (repo evenstevenbot/poker-relay). Clients talk
 * to it over WSS on 443 and fall back to plain HTTPS long-polling if WebSockets are stripped.
 * PeerJS is only used if the relay can't be reached at all on first connect.
 */
(() => {
'use strict';
const PREFIX = 'evenstevenbot-poker-v1-';
const DECK = ['0','1','2','3','5','8','13','21','?','☕'];
const PRESETS = [[30,'30s'],[60,'1m'],[120,'2m'],[300,'5m']];
const AVATAR = 'https://api.dicebear.com/10.x/notionists/svg?seed=';
const TINTS = ['#FFE3EF','#D9FAFF','#FFF9C2','#FFE6D5','#ECEAE4'];
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const HEARTBEAT = 2500, TIMEOUT = 9000;
// Room relay (Cloudflare Worker + Durable Object). Leave '' to use PeerJS only.
// e.g. 'https://poker-relay.<subdomain>.workers.dev' or 'https://poker.techirvine.com'
const RELAY_DEFAULT = '';
const IS_LOCAL = /^(localhost|127\.0\.0\.1)$/.test(location.hostname);
const RELAY_URL = ((IS_LOCAL && new URLSearchParams(location.search).get('relay')) || RELAY_DEFAULT).replace(/\/+$/, '');
const FORCE_POLL = new URLSearchParams(location.search).has('poll');   // test hook: skip WebSockets
const BLOCKED_MSG = "Can't reach the room server. Your network may be blocking it. Ask IT to allow it, or try another network.";
let useRelay = !!RELAY_URL;
let relay = null, relayEverUp = false, relayWsBroken = FORCE_POLL;

const $ = (s) => document.querySelector(s);
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
const now = () => Date.now();
const uid = () => (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2) + now().toString(36)).replace(/-/g,'').slice(0,16);
const hash = (s) => { let h = 0; for (const c of s) h = (h * 31 + c.charCodeAt(0)) | 0; return Math.abs(h); };
const avatarUrl = (name) => AVATAR + encodeURIComponent((name || 'guest').trim().toLowerCase());
const fmt = (ms) => { const s = Math.ceil(Math.max(0, ms) / 1000); return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); };
const cleanCode = (s) => (s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
const newCode = () => Array.from(crypto.getRandomValues(new Uint32Array(6)), (n) => CODE_CHARS[n % CODE_CHARS.length]).join('');

// ---- identity -------------------------------------------------------------
const me = {
  id: sessionStorage.getItem('esb-poker-id') || uid(),
  name: localStorage.getItem('esb-poker-name') || '',
  vote: null, round: 0, joinedAt: 0,
};
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
  destroyRelay();
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
  const p = new Peer(hostPeerId(room), { debug: window.__pdebug||0 });
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
  const p = new Peer({ debug: window.__pdebug||0 });
  peer = p;
  p.on('open', () => {
    if (peer !== p) return;
    const c = p.connect(hostPeerId(room), { reliable: true, serialization: 'json' });
    hostConn = c;
    const t = setTimeout(() => { if (hostConn === c && !c.open) hostLost('timeout'); }, 9000);
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
  const delay = (wasConnected ? rank * 1500 : Math.min(800 * attempts, 5000)) + Math.random() * 300;
  clearTimeout(retryTimer);
  retryTimer = setTimeout(tryClaimHost, delay);
}

// ---- relay transport --------------------------------------------------------
const helloMsg = () => ({ t: 'hello', id: me.id, name: me.name, vote: me.vote, round: me.round, joinedAt: me.joinedAt });
function destroyRelay() {
  if (!relay) return;
  const r = relay; relay = null;
  if (r.ws) { r.ws.onclose = r.ws.onmessage = r.ws.onopen = null; try { r.ws.close(); } catch {} }
  if (r.ac) try { r.ac.abort(); } catch {}
}
function relayConnect() {
  if (leaving) return;
  clearTimeout(retryTimer);
  destroyRelay();
  role = 'connecting';
  setStatus('warn', 'connecting…');
  const r = relay = { mode: null, ver: -1, ws: null, ac: new AbortController() };
  if (relayWsBroken) return startPoll(r);
  let ws, opened = false;
  try { ws = new WebSocket(RELAY_URL.replace(/^http/, 'ws') + `/rooms/${room}/ws`); } catch { relayWsBroken = true; return startPoll(r); }
  r.ws = ws;
  const t = setTimeout(() => { if (relay === r && !opened) { ws.onclose = null; try { ws.close(); } catch {} relayWsBroken = true; startPoll(r); } }, 6000);
  ws.onopen = () => {
    if (relay !== r) return;
    opened = true; clearTimeout(t); r.mode = 'ws';
    ws.send(JSON.stringify(helloMsg()));
  };
  ws.onmessage = (e) => {
    if (relay !== r) return;
    lastHostMsg = now();
    let d; try { d = JSON.parse(e.data); } catch { return; }
    if (d.t === 'state') { if (role !== 'client') relayUp(r); applyView(d); }
    else if (d.t === 'rejoin') ws.send(JSON.stringify(helloMsg()));
  };
  ws.onclose = () => {
    if (relay !== r) return;
    clearTimeout(t);
    if (!opened) { relayWsBroken = true; startPoll(r); }   // WS upgrade blocked: use HTTPS polling
    else relayLost();
  };
}
function relayUp(r) {
  role = 'client'; attempts = 0; relayEverUp = true; lastHostMsg = now();
  banner(null); setStatus('ok', r.mode === 'poll' ? 'connected · https' : 'connected');
}
async function relayPost(msg) {
  // text/plain keeps this a "simple" CORS request (no preflight)
  const res = await fetch(`${RELAY_URL}/rooms/${room}/send`, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: JSON.stringify({ ...msg, pid: me.id }), cache: 'no-store' });
  if (!res.ok) throw new Error('relay ' + res.status);
  return res.json();
}
async function startPoll(r) {
  if (relay !== r) return;
  r.mode = 'poll'; r.ws = null;
  try {
    const v = await relayPost(helloMsg());
    if (relay !== r) return;
    relayUp(r); r.ver = v.ver; applyView(v);
  } catch { if (relay === r) relayFailed(); return; }
  while (relay === r) {
    try {
      const res = await fetch(`${RELAY_URL}/rooms/${room}/poll?pid=${encodeURIComponent(me.id)}&v=${r.ver}`, { cache: 'no-store', signal: r.ac.signal });
      if (!res.ok) throw new Error('relay ' + res.status);
      const d = await res.json();
      if (relay !== r) return;
      lastHostMsg = now();
      if (d.rejoin) { const v = await relayPost(helloMsg()); if (relay !== r) return; r.ver = v.ver; applyView(v); continue; }
      r.ver = d.ver; applyView(d);
    } catch { if (relay === r) relayLost(); return; }
  }
}
function relayFailed() {
  // never reached the relay (neither WSS nor HTTPS)
  if (!relayEverUp && typeof Peer !== 'undefined') {
    destroyRelay(); useRelay = false; attempts = 0;
    toast('Room server unreachable, trying peer-to-peer…');
    tryClaimHost(); return;
  }
  relayLost();
}
function relayLost() {
  if (leaving) return;
  destroyRelay();
  role = 'connecting';
  if (lastView && relayEverUp) banner('Lost the connection to the room. Reconnecting… your vote and the timer are kept.');
  retryLater(relayConnect, 'reconnecting…');
}
function relaySend(msg) {
  const r = relay;
  if (!r || role !== 'client') { toast('Reconnecting… try again in a moment'); return; }
  if (r.mode === 'ws' && r.ws && r.ws.readyState === 1) r.ws.send(JSON.stringify(msg));
  else if (r.mode === 'poll') relayPost(msg).then((v) => { if (relay === r && v && v.ver >= (lastView && lastView.ver || 0)) applyView(v); }).catch(() => toast('Could not send, retrying connection…'));
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
  if (prev && prev.revealed) for (const p of prev.players) if (p.id !== prev.hostId) S.players[p.id] = { name: p.name, vote: p.vote, joinedAt: p.joinedAt, ghost: now() };
  if (me.round !== S.round) { me.vote = null; me.round = S.round; }
  S.players[me.id] = { name: me.name, vote: me.vote, joinedAt: me.joinedAt };
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
    S.players[pid] = {
      name: String(d.name || 'guest').slice(0, 24),
      vote: sameRound && DECK.includes(d.vote) ? d.vote : (existing ? existing.vote : null),
      joinedAt: Number(d.joinedAt) || now(),
    };
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
    .map(([id, p]) => ({ id, name: p.name, joinedAt: p.joinedAt, voted: p.vote != null, vote: S.revealed ? p.vote : null }))
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
  if (hostConn && hostConn.open) hostConn.send({ t: 'hello', id: me.id, name: me.name, vote: me.vote, round: me.round, joinedAt: me.joinedAt });
}
function onClientData(d) {
  lastHostMsg = now();
  if (!d || typeof d !== 'object') return;
  if (d.t === 'bye') { hostLost('bye'); return; }
  if (d.t === 'state') applyView(d);
}
function send(msg) {
  if (useRelay) return relaySend(msg);
  if (role === 'host') { if (msg.t === 'vote') applyVote(me.id, msg.value); else applyAction(msg); }
  else if (role === 'client' && hostConn && hostConn.open) hostConn.send(msg);
  else toast('Reconnecting… try again in a moment');
}

// heartbeat both ways + ghost cleanup
function heartbeat() {
  if (useRelay) {
    if (relay && relay.mode === 'ws' && role === 'client') {
      if (relay.ws.readyState === 1) try { relay.ws.send('{"t":"ping"}'); } catch {}
      if (now() - lastHostMsg > TIMEOUT) relayLost();
    }
    return;
  }
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
  const numeric = v.revealed ? v.players.map((p) => p.vote).filter((x) => x != null && /^\d+$/.test(x)).map(Number) : [];
  const cast = v.revealed ? v.players.map((p) => p.vote).filter((x) => x != null) : [];
  const consensus = v.revealed && cast.length >= 2 && cast.every((x) => x === cast[0]) && /^\d+$/.test(cast[0]);
  for (const p of v.players) {
    const li = el('li', 'player');
    const av = el('div', 'av');
    const img = el('img'); img.src = avatarUrl(p.name); img.alt = ''; img.width = 72; img.height = 72; img.loading = 'lazy';
    img.style.background = TINTS[hash(p.name.toLowerCase()) % TINTS.length];
    av.append(img);
    const card = el('div', 'pcard');
    let state;
    if (v.revealed && p.vote != null) { card.classList.add('shown'); if (justRevealed) card.classList.add('flip'); card.textContent = p.vote; if (consensus) card.classList.add('match'); state = `voted ${p.vote === '☕' ? 'coffee break' : p.vote}`; }
    else if (p.voted || (p.id === me.id && me.vote != null)) { card.classList.add('voted'); card.textContent = '✓'; state = 'has voted'; }
    else { card.textContent = v.revealed ? '–' : ''; state = v.revealed ? 'did not vote' : 'thinking'; }
    const name = el('div', 'name', p.name + (p.id === me.id ? ' (you)' : ''));
    li.append(card, av, name);
    if (p.id === v.hostId) li.append(el('div', 'tag', 'host'));
    li.setAttribute('aria-label', `${p.name}${p.id === me.id ? ' (you)' : ''}: ${state}`);
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
  $('#deckLabel').textContent = v.revealed ? 'cards revealed · start a new round to vote again' : (me.vote != null ? `you picked ${me.vote === '☕' ? 'coffee' : me.vote} · tap again to undo` : 'pick a card');

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

// ---- UI wiring ----------------------------------------------------------------
function buildStatic() {
  const deck = $('#deck');
  for (const v of DECK) {
    const b = el('button', 'card', v); b.type = 'button'; b.dataset.v = v;
    b.setAttribute('aria-label', v === '☕' ? 'coffee break' : v === '?' ? 'not sure' : `${v} points`);
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
}

function shareUrl() { const u = new URL(location.href); u.search = '?room=' + room; u.hash = ''; return u.toString(); }

function initJoin() {
  const params = new URLSearchParams(location.search);
  const code = cleanCode(params.get('room'));
  const ni = $('#nameInput'), prev = $('#joinAvatar');
  ni.value = me.name;
  const upd = () => { prev.src = avatarUrl(ni.value || 'evenstevenbot'); prev.style.background = TINTS[hash((ni.value || 'evenstevenbot').toLowerCase()) % TINTS.length]; };
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
  if (!useRelay && typeof Peer === 'undefined') { $('#joinError').textContent = 'Could not load the real-time library. Check your connection and reload.'; return; }
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
  if (useRelay) relayConnect(); else tryClaimHost();
  clearInterval(hbTimer); hbTimer = setInterval(heartbeat, HEARTBEAT);
  clearInterval(tickTimer); tickTimer = setInterval(() => { hostTick(); renderTimer(); }, 250);
}

function sayBye() {
  if (useRelay && relay) {
    const m = JSON.stringify({ t: 'bye', pid: me.id });
    if (relay.mode === 'ws' && relay.ws && relay.ws.readyState === 1) try { relay.ws.send(m); } catch {}
    else try { navigator.sendBeacon(`${RELAY_URL}/rooms/${room}/send`, new Blob([m], { type: 'text/plain' })); } catch {}
    return;
  }
  if (role === 'host') for (const c of conns.keys()) { try { if (c.open) c.send({ t: 'bye' }); } catch {} }
}
function leaveRoom() {
  leaving = true; sayBye();
  setTimeout(() => { destroyPeer(); location.href = location.pathname; }, 150);
}
window.addEventListener('pagehide', () => { leaving = true; sayBye(); destroyPeer(); });

// debug hook for automated tests
window.__poker = { get transport() { return useRelay ? (relay && relay.mode) : 'peer'; }, get role() { return role; }, get view() { return lastView; }, get me() { return me; } };

buildStatic();
initJoin();
})();
