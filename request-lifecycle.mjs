const TERMINAL=new Set(['resolving','resolved','canceled','failed']);
export class RequestLifecycle{
 constructor(cap=1024){this.cap=cap;this.requests=new Map();this.canceledNetworks=new Set();this.stats={duplicates:0,skipped:0,staleCanceled:0};}
 add(p){if(this.requests.has(p.requestId)){this.stats.duplicates++;return false;}if(this.requests.size>=this.cap)throw Error('interception_lifecycle_cap');
 this.requests.set(p.requestId,{networkId:p.networkId||null,type:p.resourceType,operation:(()=>{try{return new URL(p.request.url).pathname.split('/').at(-1);}catch{return 'unknown';}})(),created:Date.now(),state:'queued',canceled:!!p.networkId&&this.canceledNetworks.has(p.networkId)});return true;}
 loadingFailed(p){if(p.canceled!==true)return;if(this.canceledNetworks.size>=this.cap&&!this.canceledNetworks.has(p.requestId))throw Error('interception_lifecycle_cap');this.canceledNetworks.add(p.requestId);for(const r of this.requests.values())if(r.networkId===p.requestId)r.canceled=true;}
 reason(id){const r=this.requests.get(id);if(!r)throw Error('untracked_interception');if(r.canceled)return 'canceled_request';if(TERMINAL.has(r.state))return 'already_resolved';return null;}
 canceled(id){return this.requests.get(id)?.canceled===true;}
 info(id){const r=this.requests.get(id);return r?{type:r.type,operation:r.operation,state:r.state,canceled:r.canceled,hasNetworkId:!!r.networkId,queueAgeMs:Math.max(0,Date.now()-r.created)}:{state:'untracked'};}
 summary(){return {...this.stats,tracked:this.requests.size,canceled:[...this.requests.values()].filter(r=>r.canceled).length};}
}
export async function resolveInterception(life,id,method,params,send,settle=()=>new Promise(r=>setTimeout(r,50))){
 const reason=life.reason(id);if(reason){life.stats.skipped++;return {skipped:true,reason};}
 const entry=life.requests.get(id);entry.state='resolving';
 try{const value=await send(method,params);entry.state='resolved';return {value};}
 catch(e){
 const stale=['Fetch.continueRequest','Fetch.failRequest','Fetch.fulfillRequest'].includes(method)&&e?.cdp?.method===method&&e.cdp.code===-32602&&e.cdp.message==='Invalid InterceptionId';
 if(stale){await settle();if(life.canceled(id)){entry.state='canceled';life.stats.staleCanceled++;return {skipped:true,reason:'canceled_interception_expired'};}}
 entry.state='failed';e.requestLifecycle=life.info(id);throw e;
 }
}
