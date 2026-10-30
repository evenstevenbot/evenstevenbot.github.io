/* Fin Ops fish catalog: Kaws Bot's six species (assets/fish/*.svg) plus four extra species
 * drawn in the same sticker style, each recolorable into the shared palette. */
(() => {
'use strict';
const COLORS = [
  { id: 'pink',   name: 'Pink',   hex: '#FF2E88' },
  { id: 'cyan',   name: 'Cyan',   hex: '#00E5FF' },
  { id: 'yellow', name: 'Yellow', hex: '#FFE600' },
  { id: 'orange', name: 'Orange', hex: '#FF6A00' },
  { id: 'white',  name: 'Pearl',  hex: '#F7F4EF' },
  { id: 'ink',    name: 'Ink',    hex: '#2a2a2a' },
  { id: 'lime',   name: 'Lime',   hex: '#7CF25A' },
  { id: 'purple', name: 'Grape',  hex: '#A970FF' },
];
// mouth: point in SVG coords (viewBox -14 -14 268 208), body: original body hex to recolor.
const SPECIES = [
  { id: 'maguro', name: 'Maguro', jp: 'マグロ', file: 'maguro-pink.svg',   body: '#FF2E88', mouth: [207, 96] },
  { id: 'fugu',   name: 'Fugu',   jp: 'フグ',   file: 'fugu-yellow.svg',   body: '#FFE600', mouth: [199, 106] },
  { id: 'hirame', name: 'Hirame', jp: 'ヒラメ', file: 'hirame-cyan.svg',   body: '#00E5FF', mouth: [205, 100] },
  { id: 'ryukin', name: 'Ryukin', jp: 'リュウキン', file: 'ryukin-orange.svg', body: '#FF6A00', mouth: [197, 102] },
  { id: 'koi',    name: 'Koi',    jp: 'コイ',   file: 'koi-white.svg',     body: '#F7F4EF', mouth: [213, 94] },
  { id: 'betta',  name: 'Betta',  jp: 'ベタ',   file: 'betta-ink.svg',     body: '#2a2a2a', mouth: [205, 96] },
  { id: 'angel',  name: 'Angel',  jp: 'エンゼル', body: '#A970FF', mouth: [201, 94], gen: 'angel' },
  { id: 'clown',  name: 'Clown',  jp: 'クマノミ', body: '#FF6A00', mouth: [205, 98], gen: 'clown' },
  { id: 'same',   name: 'Shark',  jp: 'サメ',   body: '#00E5FF', mouth: [217, 104], gen: 'shark' },
  { id: 'hagi',   name: 'Tang',   jp: 'ハギ',   body: '#7CF25A', mouth: [203, 96], gen: 'tang' },
];

function eye(cx, cy, r, iris) {
  return `<ellipse cx="${cx}" cy="${cy}" rx="${r}" ry="${r*1.15}" fill="#fff" stroke="#111111" stroke-width="4.5"/>` +
    `<ellipse cx="${cx+2}" cy="${cy+2}" rx="${r*.72}" ry="${r*.88}" fill="${iris}"/>` +
    `<ellipse cx="${cx+2}" cy="${cy+3}" rx="${r*.38}" ry="${r*.48}" fill="#111111"/>` +
    `<circle cx="${cx-r*.38}" cy="${cy-r*.35}" r="${r*.28}" fill="#fff"/><circle cx="${cx+r*.35}" cy="${cy+r*.4}" r="${r*.13}" fill="#fff"/>` +
    `<ellipse cx="${cx-r*.4}" cy="${cy+r*1.3}" rx="${r*.45}" ry="${r*.22}" fill="#FF2E88" opacity=".75"/>`;
}
function sticker(id, body, fins, decor, eyeSvg, smile) {
  const both = fins + `<path d="${body}" fill="BODYHEX"/>`;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="-14 -14 268 208" width="240" height="186"><defs>` +
    `<pattern id="d${id}" width="9" height="9" patternUnits="userSpaceOnUse"><circle cx="4.5" cy="4.5" r="1.8" fill="#111111" opacity=".22"/></pattern>` +
    `<clipPath id="c${id}"><path d="${body}"/></clipPath></defs>` +
    `<g stroke="#fff" stroke-width="22" stroke-linejoin="round" fill="#fff" style="filter:drop-shadow(3px 5px 0 rgba(0,0,0,.35))">${both}</g>` +
    `<g stroke="#111111" stroke-width="5.5" stroke-linejoin="round">${both}</g>` +
    `<g clip-path="url(#c${id})">${decor}<rect x="0" y="100" width="260" height="90" fill="url(#d${id})"/>` +
    `<path d="M40,78 C90,48 150,46 196,66" stroke="#fff" stroke-width="7" fill="none" opacity=".7" stroke-linecap="round"/></g>` +
    `<path d="${body}" fill="none" stroke="#111111" stroke-width="5.5"/>${eyeSvg}` +
    `<path d="${smile}" fill="#FF2E88" stroke="#111111" stroke-width="4" stroke-linejoin="round"/></svg>`;
}
const GEN = {
  angel: () => sticker('angel', 'M58,92 C88,40 150,34 200,88 C150,146 88,144 58,92Z',
    '<path d="M96,60 L118,-6 L160,50Z" fill="#FFE600"/><path d="M96,124 L114,192 L156,134Z" fill="#FFE600"/><path d="M64,92 L22,60 L32,92 L22,124Z" fill="#00E5FF"/>',
    '<path d="M112,30 L112,160 M146,30 L146,160" stroke="#111111" stroke-width="9" opacity=".8"/>',
    eye(170, 84, 16, '#00E5FF'), 'M189,96 Q195,106 201,96'),
  clown: () => sticker('clown', 'M40,96 C60,50 170,46 208,92 C176,140 64,142 40,96Z',
    '<path d="M46,96 L10,62 L20,96 L10,130Z" fill="#FFE600"/><path d="M88,60 C108,28 150,32 162,56Z" fill="#FFE600"/><path d="M118,126 L110,152 L140,128Z" fill="#FFE600"/>',
    '<path d="M86,40 C74,80 74,116 88,150 M146,40 C134,80 134,116 148,150" stroke="#111111" stroke-width="22" fill="none"/><path d="M86,40 C74,80 74,116 88,150 M146,40 C134,80 134,116 148,150" stroke="#fff" stroke-width="13" fill="none"/>',
    eye(172, 84, 15, '#FF2E88'), 'M193,98 Q199,108 205,98'),
  shark: () => sticker('shark', 'M18,100 C60,58 172,54 222,96 C180,130 70,136 18,100Z',
    '<path d="M98,68 L122,14 L148,64Z" fill="#FF2E88"/><path d="M28,100 L-2,48 L14,100 L-2,152Z" fill="#FF2E88"/><path d="M128,120 L112,158 L152,124Z" fill="#FF2E88"/>',
    '<path d="M30,112 C80,136 170,132 222,98 L230,170 L20,170Z" fill="#fff" opacity=".85"/><path d="M168,86 l-5,22 M156,86 l-5,22 M144,86 l-5,22" stroke="#111111" stroke-width="4" stroke-linecap="round"/>',
    eye(188, 86, 11, '#FFE600'), 'M205,106 Q211,114 217,104'),
  tang: () => sticker('tang', 'M50,94 C60,42 170,38 208,90 C180,140 70,146 50,94Z',
    '<path d="M56,94 L18,62 L28,94 L18,126Z" fill="#FFE600"/><path d="M70,64 C104,24 168,34 192,68Z" fill="#111111"/><path d="M76,124 C110,150 160,146 180,124Z" fill="#111111"/>',
    '<path d="M76,78 C106,56 156,64 148,98 C140,122 104,118 112,96" stroke="#111111" stroke-width="11" fill="none" stroke-linecap="round"/>',
    eye(174, 82, 15, '#FF6A00'), 'M191,96 Q197,106 203,96'),
};

const raw = {};
const spriteCache = {};
const urlCache = {};
async function loadAll(base) {
  await Promise.all(SPECIES.map(async (s) => {
    if (s.gen) { raw[s.id] = GEN[s.gen]().replace(/BODYHEX/g, s.body); return; }
    const r = await fetch(base + s.file); raw[s.id] = await r.text();
  }));
}
function svgFor(sp, colorId) {
  const s = SPECIES.find((x) => x.id === sp) || SPECIES[0];
  const c = COLORS.find((x) => x.id === colorId) || COLORS[0];
  const src = raw[s.id] || '';
  return src.split(s.body).join(c.hex).split(s.body.toLowerCase()).join(c.hex);
}
function urlFor(sp, colorId) {
  const k = sp + ':' + colorId;
  if (!urlCache[k]) urlCache[k] = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgFor(sp, colorId));
  return urlCache[k];
}
// Returns a canvas (or null while loading). Sprite is 402x312 px = viewBox * 1.5.
function sprite(sp, colorId) {
  const k = sp + ':' + colorId;
  const e = spriteCache[k];
  if (e) return e.ready ? e.cv : null;
  const ent = spriteCache[k] = { ready: false, cv: document.createElement('canvas') };
  ent.cv.width = 402; ent.cv.height = 312;
  const img = new Image();
  img.onload = () => { const g = ent.cv.getContext('2d'); g.drawImage(img, 0, 0, 402, 312); ent.ready = true; };
  img.src = urlFor(sp, colorId);
  return null;
}
window.FinFish = { COLORS, SPECIES, loadAll, urlFor, sprite };
})();
