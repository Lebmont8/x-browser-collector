import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {pathToFileURL} from 'node:url';
export const TTL=48*3600000;
function read(p,f={}){return fs.existsSync(p)?JSON.parse(fs.readFileSync(p,'utf8')):f;}
function atomic(p,v){const t=p+'.retention-'+process.pid;fs.writeFileSync(t,JSON.stringify(v));fs.renameSync(t,p);}
export function ident(v){if(!v||typeof v!=='object')return null;if(v.rest_id&&typeof v.legacy?.full_text==='string')return String(v.rest_id);if(v.id&&typeof v.text==='string'&&v.author)return String(v.id);return null;}
export function scrub(v,expired){const id=ident(v);if(id&&expired.has(id))return {retention_expired:true,id};if(Array.isArray(v))return v.filter(x=>!expired.has(ident(x))).map(x=>scrub(x,expired));if(v&&typeof v==='object')return Object.fromEntries(Object.entries(v).map(([k,x])=>[k,scrub(x,expired)]));return v;}
export function clean(root=path.resolve(process.env.X_COLLECTOR_HOME||path.join(process.cwd(),'.state')),now=Date.now()){
 const indexFile=path.join(root,'post-retention-index.json'),idx=read(indexFile,{posts:{},files:{}}),summary={checked_at:new Date().toISOString(),ttl_hours:48,deleted_posts:0,scrubbed_files:0,deleted_files:0,network:0};
 const remember=(id,at)=>{const t=Date.parse(at);if(id&&Number.isFinite(t))idx.posts[id]=Math.min(idx.posts[id]??Infinity,t);};
 const files=fs.readdirSync(root).filter(n=>/^cache-.*\.json$/.test(n));
 for(const n of files){const c=read(path.join(root,n));for(const [id,t] of Object.entries(c.posts||{}))remember(id,t.firstAcquiredAt||t.lastAcquiredAt||c.lastAcquiredAt);}
 const payloads=['last-run.json',...fs.readdirSync(root).filter(n=>/^diagnostic-.*-result\.json$/.test(n))];
 const outbox=path.join(root,'outbox');if(fs.existsSync(outbox))payloads.push(...fs.readdirSync(outbox).filter(n=>n.endsWith('.json')).map(n=>'outbox/'+n));
 function walk(v,at){const id=ident(v);if(id)remember(id,at);if(v&&typeof v==='object')for(const x of Object.values(v))if(x&&typeof x==='object')walk(x,at);}
 for(const n of payloads){const p=path.join(root,n);if(!fs.existsSync(p))continue;const v=read(p);for(const r of v.receipts||[])walk(r.body,r.acquired_at||v.finished_at);}
 const expired=new Set(Object.entries(idx.posts).filter(([id,first])=>first+TTL<=now).map(([id])=>id));
 for(const n of files){const p=path.join(root,n),c=read(p),before=JSON.stringify(c);
 for(const [id,t] of Object.entries(c.posts||{})){if(expired.has(id)){if(t.text!==undefined)summary.deleted_posts++;c.posts[id]={id,firstAcquiredAt:new Date(idx.posts[id]).toISOString(),retention_expired:true};}}
 if(JSON.stringify(c)!==before)atomic(p,c);
 }
 for(const n of payloads){const p=path.join(root,n);if(!fs.existsSync(p))continue;const v=read(p);const first=Date.parse(v.receipts?.[0]?.acquired_at||v.finished_at||v.started_at)||fs.statSync(p).mtimeMs;idx.files[n]=Math.min(idx.files[n]??Infinity,first);
 if(idx.files[n]+TTL<=now){fs.unlinkSync(p);summary.deleted_files++;continue;}
 const out=scrub(v,expired);if(JSON.stringify(out)!==JSON.stringify(v)){atomic(p,out);summary.scrubbed_files++;}}
 atomic(indexFile,idx);summary.expired_ids=expired.size;atomic(path.join(root,'post-retention-status.json'),summary);return summary;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href)console.log(JSON.stringify(clean()));
