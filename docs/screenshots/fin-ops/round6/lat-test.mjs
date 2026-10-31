import { chromium } from '/usr/local/lib/pnpm/5/.pnpm/playwright@1.63.0-alpha-2026-08-31/node_modules/playwright/index.mjs';
const [,, PORT='4432', NS='2', LOSS='0', DUR='12000'] = process.argv;
const BASE=`http://localhost:${PORT}/games/finops/`, Q='&local=1&test=1'+(+LOSS?'&loss='+LOSS:'');
const phases=NS.split(',').map(Number);
const b=await chromium.launch({executablePath:'/usr/bin/google-chrome',args:['--disable-background-timer-throttling','--disable-renderer-backgrounding','--disable-backgrounding-occluded-windows']});
const ctx=await b.newContext({viewport:{width:800,height:600}});
const W=(p,ms)=>p.waitForTimeout(ms); const errs=[];
const watch=(p,n)=>{p.on('pageerror',e=>errs.push(n+': '+e.message));p.on('console',m=>{if(m.type()==='error')errs.push(n+' console: '+m.text());});};
const host=await ctx.newPage(); watch(host,'host'); await host.goto(BASE+'?x=1'+Q); await W(host,800); await host.click('#createBtn');
await host.waitForSelector('#pick:not([hidden])',{timeout:20000}); const code=await host.evaluate(()=>__finops.room);
await host.fill('#nameInput','Host'); await host.click('#diveBtn'); await host.waitForSelector('#hud:not([hidden])');
const gs=[];
async function addG(){const i=gs.length+1;const g=await ctx.newPage(); watch(g,'G'+i); await g.goto(BASE+'?room='+code+Q); await g.waitForSelector('#pick:not([hidden])',{timeout:30000}); await g.fill('#nameInput','G'+i); await g.click('#diveBtn'); await g.waitForSelector('#hud:not([hidden])'); 
  await g.evaluate(()=>{const s={last:-1,at:performance.now(),gaps:[],n:0};window.__st=s;setInterval(()=>{const V=__finops.V;if(V.seq!==s.last){const t=performance.now();if(s.last>=0)s.gaps.push(t-s.at);s.last=V.seq;s.at=t;s.n++;}},4);
   // keyboard-ish steering through the real input path
   let k=0;setInterval(()=>{k++;const keys=['ArrowRight','ArrowDown','ArrowLeft','ArrowUp'];const d=keys[k%4];for(const x of keys)window.dispatchEvent(new KeyboardEvent('keyup',{key:x}));window.dispatchEvent(new KeyboardEvent('keydown',{key:d}));},900);});
  gs.push(g);}
async function close1(){const g=gs.pop(); await g.close();}
await host.evaluate(()=>__finops.hostStart({min:10})).catch(()=>{});
const out=[];
for(const N of phases){
  while(gs.length+1<N) await addG(); while(gs.length+1>N) await close1();
  if(out.length===0){await host.evaluate(()=>__finops.hostStart()); }
  await W(host,3000);
  for(const g of gs) await g.evaluate(()=>{const s=__st;s.gaps=[];s.n=0;s.t0=performance.now();s.id=[];const V=__finops.V;s.iv=setInterval(()=>s.id.push(V.idelay),100);});
  const h0=await host.evaluate(()=>({sent:__finops.H.sent||0,tick:__finops.H.tick,t:performance.now()}));
  const bytes=await host.evaluate((D)=>new Promise(res=>{const s=[];const iv=setInterval(()=>s.push(__finops.H.bytesWire),33);setTimeout(()=>{clearInterval(iv);res(Math.round(s.reduce((a,b)=>a+b)/s.length));},D);}),+DUR);
  const h1=await host.evaluate(()=>({sent:__finops.H.sent||0,tick:__finops.H.tick,t:performance.now(),se:__finops.H.se,players:__finops.H.players.size}));
  const G=[];
  const sample=async(g)=>{const r=await g.evaluate(()=>{const V=__finops.V;return {fN:V.fN,fs:[...(V.fsim||new Map()).entries()].map(([id,f])=>[id,f.x,f.y])};});const hf=await host.evaluate((n)=>__finops.H.fpos&&__finops.H.fpos.get(n)||null,r.fN);const hm=new Set((hf||[]).map(q=>q[0])),gm=new Set(r.fs.map(q=>q[0]));return {miss:[...hm].filter(i=>!gm.has(i)),ghost:[...gm].filter(i=>!hm.has(i))};};
  const persist={miss:0,ghost:0};for(const g of gs){const S=[];for(let k=0;k<3;k++){S.push(await sample(g));await W(g,500);}const inAll=(key)=>S[0][key].filter(i=>S.every(s=>s[key].includes(i))).length;persist.miss=Math.max(persist.miss,inAll('miss'));persist.ghost=Math.max(persist.ghost,inAll('ghost'));}
  for(const g of gs){const r=await g.evaluate(()=>{const s=__st;clearInterval(s.iv);const dt=(performance.now()-s.t0)/1000;const gg=s.gaps.slice().sort((a,b)=>a-b);const V=__finops.V;
     return {rate:+(s.n/dt).toFixed(1),gapMax:Math.round(gg[gg.length-1]||0),gapP99:Math.round(gg[Math.floor(gg.length*.99)]||0),freezes200:gg.filter(x=>x>200).length,idelayAvg:Math.round(s.id.reduce((a,b)=>a+b,0)/s.id.length),rtt:Math.round(V.rtt),fN:V.fN,fs:[...(V.fsim||new Map()).entries()].map(([id,f])=>[id,f.x,f.y])};});
    const hf=await host.evaluate((n)=>{const a=__finops.H.fpos&&__finops.H.fpos.get(n);return a||null;},r.fN);
    let missing=0,ghost=0,err=[];if(hf){const hm=new Map(hf.map(q=>[q[0],q]));const gm=new Map(r.fs.map(q=>[q[0],q]));for(const id of hm.keys())if(!gm.has(id))missing++;for(const [id,q] of gm){const h=hm.get(id);if(!h)ghost++;else err.push(Math.hypot(h[1]-q[1],h[2]-q[2]));}}
    err.sort((a,b)=>a-b);G.push({...r,fs:undefined,fN:undefined,hostFood:hf?hf.length:null,missing,ghost,posErrMax:+(err[err.length-1]||0).toFixed(1)});}
  const dt=(h1.t-h0.t)/1000, sps=(h1.sent-h0.sent)/dt;
  const agg=(k,f=Math.max)=>f(...G.map(g=>g[k]));
  out.push({players:h1.players,se:h1.se,hostSnapsPerSec:h0.sent!=null&&h1.sent?+sps.toFixed(1):null,tickRate:+((h1.tick-h0.tick)/dt).toFixed(1),wireAvgB:bytes,
    estUploadKBps:h1.sent?+((sps*(bytes+90)*(h1.players-1))/1024).toFixed(1):null,
    guestRateMin:agg('rate',Math.min),guestRateMax:agg('rate'),gapMax:agg('gapMax'),gapP99:agg('gapP99'),freezes200:agg('freezes200'),idelayAvg:Math.round(G.reduce((a,g)=>a+g.idelayAvg,0)/G.length),idelayMin:agg('idelayAvg',Math.min),
    foodMissing:agg('missing'),foodMissingPersist1s:persist.miss,foodGhostPersist1s:persist.ghost,foodGhost:agg('ghost'),foodPosErrMax:agg('posErrMax'),hostFood:G[0].hostFood,why:await host.evaluate(()=>__finops.H.why)});
}
console.log(JSON.stringify({port:PORT,loss:+LOSS,phases:out,errs:errs.slice(0,10),nErr:errs.length},null,1)); await b.close();
