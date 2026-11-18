import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {pathToFileURL} from 'node:url';
import {LIMIT,AUTHORS,normalize,day,dueAt,gate,reserve,decision,op,failure,domFailure,tweets,updateCache,success,cachedProfile,dispatchGuard,DOM_EXPRESSION} from './policy.mjs';
import {RequestLifecycle,resolveInterception} from './request-lifecycle.mjs';
const ROOT=path.resolve(process.env.X_COLLECTOR_HOME||path.join(process.cwd(),'.state')),STATE=path.join(ROOT,'state.json'),LOCK=path.join(ROOT,'collector.lock'),STATUS=path.join(ROOT,'status.json'),STOP=path.join(ROOT,'STOP');
function read(p,f={}){return fs.existsSync(p)?JSON.parse(fs.readFileSync(p,'utf8')):f;}
function atomic(p,v){fs.writeFileSync(p+'.tmp',JSON.stringify(v));fs.renameSync(p+'.tmp',p);}
const fileFor=a=>path.join(ROOT,'cache-'+a+'.json');
function bootstrap(s){
 const previous=read(path.join(ROOT,'last-run.json'),null);
 if(previous?.ok&&AUTHORS.includes(previous.author)&&!fs.existsSync(fileFor(previous.author))){
 const c={};for(const r of previous.receipts||[])updateCache(c,r.body,previous.author,r.acquired_at);
 atomic(fileFor(previous.author),c);
 const at=Date.parse(c.lastAcquiredAt);s.streams[previous.author]={lastSuccess:at,intervalMs:LIMIT.minRunMs,emptyStreak:0,baseline:true};
 s.preV2LastRun=s.lastRun;s.lastRun=Math.max(s.lastRun||0,at);
 }s.migratedAt??=new Date().toISOString();return s;
}
export function protocolError(method,value={}){
 const known=['Execution context was destroyed','Cannot find context with specified id','Cannot find default execution context','Inspected target navigated or closed','Invalid InterceptionId','Invalid interceptionId','Invalid RequestId','No resource with given identifier found','No data found for resource with given identifier','Session with given id not found','No target with given id found','Target closed','Not attached to an active page','Internal error','Invalid parameters','Failed to deserialize params'];
 const raw=String(value.message||'');const message=known.find(x=>raw.includes(x))||'unclassified_protocol_error';
 const e=Error('cdp_error');e.cdp={method:/^[A-Za-z]+\.[A-Za-z]+$/.test(method||'')?method:'unknown_method',code:Number.isInteger(value.code)?value.code:null,message};return e;
}
export class CDP{
 constructor(){this.pending=new Map();this.seq=0;this.connected=false;this.id=crypto.randomUUID();}
 async connect(){const p=fs.readFileSync(process.env.CDP_ACTIVE_PORT_FILE||path.join(process.env.LOCALAPPDATA||'.','Google','Chrome','User Data','DevToolsActivePort'),'utf8').trim().split(/\r?\n/);
 this.ws=new WebSocket('ws://127.0.0.1:'+p[0]+p[1]);
 this.ws.onmessage=e=>{const m=JSON.parse(e.data),p=this.pending.get(m.id);if(p){this.pending.delete(m.id);clearTimeout(p.timer);m.error?p.reject(protocolError(p.method,m.error)):p.resolve(m.result);}else this.onEvent?.(m);};
 this.ws.onclose=()=>{this.connected=false;for(const p of this.pending.values()){clearTimeout(p.timer);p.reject(Error('browser_disconnected'));}this.pending.clear();this.onDisconnect?.();};
 await new Promise((resolve,reject)=>{let timer=setTimeout(()=>reject(Error('chrome_permission_timeout')),45000);this.ws.onopen=()=>{clearTimeout(timer);this.connected=true;resolve();};this.ws.onerror=()=>{clearTimeout(timer);reject(Error('browser_connect_error'));};});
 }
 call(method,params={},sessionId){if(!this.connected)return Promise.reject(Error('browser_disconnected'));return new Promise((resolve,reject)=>{const id=++this.seq;const timer=setTimeout(()=>{this.pending.delete(id);reject(Error('cdp_timeout'));},10000);this.pending.set(id,{resolve,reject,timer,method});this.ws.send(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})}));});}
 close(){this.onDisconnect=null;this.ws?.close();}
}
export function profileAuthor(url){try{return JSON.parse(new URL(url).searchParams.get('variables')||'{}').screen_name||null;}catch{return null;}}
function safeCode(e){return String(e?.message||e).replace(/[^a-z_]/gi,'').slice(0,64)||'unknown_error';}
export async function runOnce(cdp,s,author,writeState,signal,untilSession=Infinity){
 const error=gate(s,author,Date.now());if(error)return {ok:false,error,network:0};
 const lifecycle=new RequestLifecycle();
 const started=Date.now(),cache=read(fileFor(author),{}),r={author,reads:0,total:0,denied:0,cacheHits:0,receipts:[],diagnostics:[],responses:new Map(),jobs:new Set(),operations:new Set(),queue:Promise.resolve(),lastRead:0,stopped:null,sessionId:null,targetId:null};
 s.lastRun=started;s.runReads=0;writeState();
 function stop(code){r.stopped??=code;s.blocked??=code;writeState();}
 function guard(){const code=dispatchGuard(s,r,{until:untilSession,stopFile:fs.existsSync(STOP),aborted:signal?.aborted});if(code)stop(code);return code;}
 function log(v){if(r.diagnostics.length<180)r.diagnostics.push(v);}
 function stopError(e){if(e?.cdp)r.diagnostics.push({event:'cdp_error',...e.cdp,...(e.requestLifecycle?{requestLifecycle:e.requestLifecycle}:{})});stop(safeCode(e));}
 const call=(method,params)=>cdp.call(method,params,r.sessionId);
 async function resolveRequest(p,method,params){const out=await resolveInterception(lifecycle,p.requestId,method,params,call);if(out.skipped)log({op:op(p.request.url),type:p.resourceType,result:out.reason});return out;}
 function canceledOrFinal(p){const reason=lifecycle.reason(p.requestId);if(reason){lifecycle.stats.skipped++;log({op:op(p.request.url),type:p.resourceType,result:reason});return true;}return false;}
 async function paused(p){
 const req={url:p.request.url,method:p.request.method,type:p.resourceType};const d=decision(req,author);
 let deny=r.finishing?'cycle_finished':guard()||d.deny;
 if(d.stop)stop(d.deny);
 if(canceledOrFinal(p))return;
 if(!deny&&d.kind==='data'){
 if(d.operation==='UserByScreenName'){
 if(profileAuthor(req.url)?.toLowerCase()!==author.toLowerCase()){deny='wrong_author_request';stop(deny);}
 else if(cachedProfile(cache,Date.now())){s.cacheStats.hits++;r.cacheHits++;writeState();log({op:d.operation,result:'profile_cache_hit'});await resolveRequest(p,'Fetch.fulfillRequest',{requestId:p.requestId,responseCode:200,responseHeaders:[{name:'Content-Type',value:'application/json'}],body:Buffer.from(JSON.stringify(cache.profile.body)).toString('base64')});return;}
 }
 if(!deny&&r.operations.has(d.operation)){deny='duplicate_read_blocked';}
 if(!deny){r.operations.add(d.operation);try{reserve(s,r,'data',Date.now());s.runReads=r.reads;writeState();await sleep(Math.max(0,LIMIT.minRequestMs-(Date.now()-r.lastRead)),signal);r.lastRead=Date.now();}catch(e){deny=safeCode(e);r.stopped??=deny;stop(deny);}}
 }else if(!deny){try{reserve(s,r,'static',Date.now());writeState();}catch(e){deny=safeCode(e);stop(deny);}}
 deny=(r.finishing?'cycle_finished':guard())||deny;
 if(canceledOrFinal(p))return;
 log({op:op(req.url),type:req.type,result:deny||'dispatch'});
 if(deny){r.denied++;await resolveRequest(p,'Fetch.failRequest',{requestId:p.requestId,errorReason:'BlockedByClient'});}else await resolveRequest(p,'Fetch.continueRequest',{requestId:p.requestId});
 }
 async function event(m){
 if(m.sessionId!==r.sessionId||!r.sessionId)return;
 if(m.method==='Network.loadingFailed'){lifecycle.loadingFailed(m.params);return;}
 if(m.method==='Fetch.requestPaused'){if(!lifecycle.add(m.params)){log({op:op(m.params.request.url),type:m.params.resourceType,result:'duplicate_pause'});return;}r.queue=r.queue.then(()=>paused(m.params)).catch(e=>{if(!r.stopped)stopError(e);});return;}
 if(m.method==='Network.responseReceived'){
 const p=m.params,name=op(p.response.url),isData=['UserByScreenName','UserTweets','UserOriginalsTimeline'].includes(name),doc=p.type==='Document';
 if(isData||doc||['x.com','api.x.com','abs.twimg.com'].includes(new URL(p.response.url).hostname)){const code=failure(p.response.status,p.response.url);log({op:name,status:p.response.status});if(code)stop(code);}
 if(isData&&p.response.status===200)r.responses.set(p.requestId,{operation:name,status:200});
 }else if(m.method==='Network.loadingFinished'&&r.responses.has(m.params.requestId)){
 const id=m.params.requestId,meta=r.responses.get(id);r.responses.delete(id);
 const job=(async()=>{const response=await call('Network.getResponseBody',{requestId:id});const raw=response.base64Encoded?Buffer.from(response.body,'base64').toString():response.body;
 if(Buffer.byteLength(raw)>LIMIT.maxBody)throw Error('body_cap');const body=JSON.parse(raw);const code=failure(200,'',body);if(code){stop(code);return;}
 const at=new Date().toISOString();
 if(meta.operation==='UserByScreenName'){
 if(!cache.profile||!cachedProfile(cache,Date.now())){const u=body.data?.user?.result;if(!u?.rest_id)throw Error('profile_schema');cache.profile={body,acquiredAt:Date.now(),originalAcquiredAt:at};atomic(fileFor(author),cache);}
 }else{if(!tweets(body).length)throw Error('timeline_schema');r.receipts.push({...meta,acquired_at:at,body});}
 })().catch(e=>stopError(e));r.jobs.add(job);job.finally(()=>r.jobs.delete(job));
 }}
 const onEvent=m=>event(m).catch(e=>stopError(e));cdp.onEvent=onEvent;
 try{
 r.targetId=(await cdp.call('Target.createTarget',{url:'about:blank',background:true})).targetId;
 r.sessionId=(await cdp.call('Target.attachToTarget',{targetId:r.targetId,flatten:true})).sessionId;
 await call('Network.enable',{maxTotalBufferSize:4194304,maxResourceBufferSize:2097152});
 await call('Network.setBypassServiceWorker',{bypass:true});
 await call('Fetch.enable',{patterns:[{urlPattern:'*',requestStage:'Request'}]});
 await call('Page.navigate',{url:'https://x.com/'+author});
 const until=Math.min(started+LIMIT.timeoutMs,untilSession);
 let nextDom=0;
 while(!guard()&&!r.receipts.length&&Date.now()<until){
  if(Date.now()>=nextDom){
   const v=await call('Runtime.evaluate',{expression:DOM_EXPRESSION,returnByValue:true});
   const code=domFailure(JSON.parse(v.result.value));if(code)stop(code);
   nextDom=Date.now()+1000;
  }
  await sleep(200,signal);
 }
 if(Date.now()>=untilSession&&!r.stopped)stop('bounded_session_complete');
 await Promise.allSettled([...r.jobs]);
 if(!r.receipts.length&&!r.stopped)stop('no_timeline_receipt');
 const dom=await call('Runtime.evaluate',{expression:DOM_EXPRESSION,returnByValue:true});
 const code=domFailure(JSON.parse(dom.result.value));if(code)stop(code);
 }catch(e){stopError(e);}
 finally{r.finishing=true;if(cdp.onEvent===onEvent)cdp.onEvent=null;if(r.sessionId)try{await call('Page.stopLoading',{});}catch{}await r.queue;await Promise.allSettled([...r.jobs]);if(r.targetId)try{await cdp.call('Target.closeTarget',{targetId:r.targetId});}catch{}if(cdp.onEvent===onEvent)cdp.onEvent=null;}
 let delta=null;
 if(!r.stopped&&r.receipts.length){try{for(const receipt of r.receipts)delta=updateCache(cache,receipt.body,author,receipt.acquired_at);atomic(fileFor(author),cache);success(s,author,delta,Date.parse(r.receipts.at(-1).acquired_at));writeState();}catch(e){stopError(e);}}
 const out={schema:2,source:'windows_chrome_x',author,started_at:new Date(started).toISOString(),finished_at:new Date().toISOString(),ok:!r.stopped&&r.receipts.length>0,error:r.stopped,read_requests:r.reads,total_reserved_requests:r.total,denied_requests:r.denied,cache_hits:r.cacheHits,daily_reserved:s.used,daily_total_reserved:s.traffic.total,traffic_unknown_before_v2:s.traffic.unknownBeforeV2,lifecycle:lifecycle.summary(),delta,limits:LIMIT,diagnostics:r.diagnostics,receipts:r.receipts,coverage:'bounded_first_page_with_known_id_overlap',gaps:['pagination_not_collected','media_not_downloaded','not_24h_accepted']};
 const stamp=out.started_at.replace(/[:.]/g,'-');const dir=path.join(ROOT,'outbox');fs.mkdirSync(dir,{recursive:true});atomic(path.join(dir,stamp+'.json'),out);atomic(path.join(ROOT,'last-run.json'),out);return {...out,receipts:out.receipts.map(x=>({operation:x.operation,acquired_at:x.acquired_at}))};
}
function sleep(ms,signal){if(signal?.aborted)return Promise.reject(Error('aborted'));return new Promise((resolve,reject)=>{const stop=()=>{clearTimeout(timer);reject(Error('aborted'));};const timer=setTimeout(()=>{signal?.removeEventListener('abort',stop);resolve();},ms);signal?.addEventListener('abort',stop,{once:true});});}
export async function main(args=process.argv.slice(2)){
 if(args.includes('--help')){console.log('X Browser Collector: --check | --author=HANDLE | --serve --shadow-hours=H. Set X_AUTHORS, X_COLLECTOR_HOME and CDP_ACTIVE_PORT_FILE.');return;}
 if(!args.includes('--check')&&!process.env.X_AUTHORS)throw Error('set_X_AUTHORS_before_collection');
 fs.mkdirSync(ROOT,{recursive:true});
 if(args.includes('--check')){const s=normalize(read(STATE));console.log(JSON.stringify({ok:true,host:process.env.COMPUTERNAME,state:s,status:read(STATUS,null),cache:AUTHORS.map(a=>({author:a,posts:Object.keys(read(fileFor(a),{}).posts||{}).length})),network:0}));return;}
 try{fs.writeFileSync(LOCK,JSON.stringify({pid:process.pid,started_at:new Date().toISOString(),version:2}),{flag:'wx'});}catch{console.log(JSON.stringify({ok:false,error:'busy',network:0}));process.exitCode=3;return;}
 let cdp,stopped=null;const controller=new AbortController();const signal=controller.signal;const serve=args.includes('--serve'),bootstrapOnly=args.includes('--bootstrap');
 const hours=Number(args.find(x=>x.startsWith('--shadow-hours='))?.split('=')[1]||24);
 let state=normalize(read(STATE)),healthChecks=0,cycles=0;const requestedEnd=args.find(x=>x.startsWith('--shadow-until='))?.slice('--shadow-until='.length);
 const end=Math.min(Date.now()+Math.min(24,Math.max(0.05,hours))*3600000,requestedEnd?Date.parse(requestedEnd):Infinity);
 if(!Number.isFinite(end))throw Error('invalid_shadow_deadline');
 const writeState=()=>atomic(STATE,state);
 const status=(phase,extra={})=>atomic(STATUS,{version:2,pid:process.pid,phase,updated_at:new Date().toISOString(),connectionId:cdp?.id||null,connected:!!cdp?.connected,connectionChecks:healthChecks,cycles,expires_at:new Date(end).toISOString(),daily_reserved:state.used||0,daily_total_reserved:state.traffic.total,traffic_unknown_before_v2:state.traffic.unknownBeforeV2,...extra});
 const stop=reason=>{stopped??=reason;controller.abort();};
 const onSignal=()=>stop('owner_stop');process.on('SIGTERM',onSignal);process.on('SIGINT',onSignal);
 try{
 if(fs.existsSync(STOP))throw Error('owner_stop_file');
 bootstrap(state);writeState();
 if(bootstrapOnly){console.log(JSON.stringify({ok:true,bootstrap:true,used:state.used,cache:AUTHORS.map(a=>({author:a,posts:Object.keys(read(fileFor(a),{}).posts||{}).length})),network:0}));return;}
 const author=args.find(x=>x.startsWith('--author='))?.split('=')[1]||AUTHORS[0];
 if(!serve){const reason=gate(state,author,Date.now());if(reason){console.log(JSON.stringify({ok:false,error:reason,used:state.used,next_due:new Date(dueAt(state,author)).toISOString(),network:0}));process.exitCode=3;return;}}
 if(state.blocked)throw Error('blocked_manual_check');
 if(Date.now()>=end)throw Error('bounded_session_complete');
 cdp=new CDP();status('waiting_chrome_permission');await cdp.connect();cdp.onDisconnect=()=>stop('browser_disconnected');
 await cdp.call('Browser.getVersion');healthChecks++;await cdp.call('Browser.getVersion');healthChecks++;
 const tabs=await cdp.call('Target.getTargets');if(!tabs.targetInfos.some(t=>t.type==='page'&&/^https:\/\/x\.com\//.test(t.url)))throw Error('no_existing_x_session_tab');
 do{
 if(stopped||fs.existsSync(STOP)){stopped??='owner_stop_file';break;}
 if(state.blocked){stopped='blocked_manual_check';break;}
 const choice=serve?AUTHORS.map(a=>({author:a,due:dueAt(state,a)})).sort((a,b)=>a.due-b.due)[0]:{author,due:dueAt(state,author)};
 if(!gate(state,choice.author,Date.now())){
 status('collecting',{author:choice.author});const result=await runOnce(cdp,state,choice.author,writeState,signal,end);cycles++;console.log(JSON.stringify({event:'cycle',...result}));if(!result.ok){stopped=result.error||'collection_error';break;}
 }else if(!serve)break;
 status('connected_idle',{next_author:choice.author,next_due:new Date(dueAt(state,choice.author)).toISOString()});
 if(!serve)break;
 await sleep(Math.max(1000,Math.min(60000,dueAt(state,choice.author)-Date.now(),end-Date.now())),signal);
 if(Date.now()<end&&!stopped){await cdp.call('Browser.getVersion');healthChecks++;}
 }while(Date.now()<end);
 }catch(e){stopped??=safeCode(e);if(!/owner_stop|abort/.test(stopped)){state.blocked??=stopped;writeState();}console.log(JSON.stringify({ok:false,error:stopped,...(e?.cdp?{cdp:e.cdp}:{})}));process.exitCode=2;}
 finally{status('stopped',{reason:stopped||'bounded_session_complete'});cdp?.close();try{const l=read(LOCK);if(l.pid===process.pid)fs.unlinkSync(LOCK);}catch{}process.off('SIGTERM',onSignal);process.off('SIGINT',onSignal);}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href)await main();
