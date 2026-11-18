import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import assert from 'node:assert/strict';import {clean,TTL} from './retention.mjs';
const root=fs.mkdtempSync(path.join(os.tmpdir(),'cryptoscout-retention-test-')),now=Date.now(),old=new Date(now-TTL-1).toISOString(),fresh=new Date(now-3600000).toISOString();
const write=(n,v)=>fs.writeFileSync(path.join(root,n),JSON.stringify(v));
try{
 fs.mkdirSync(path.join(root,'outbox'));
 const t=(id,text)=>({id,author:'test',text,firstAcquiredAt:id==='1'?old:fresh,lastAcquiredAt:fresh});
 const raw=(id,text)=>({rest_id:id,legacy:{full_text:text}});
 write('cache-test.json',{posts:{1:t('1','expired source'),2:t('2','fresh source')}});
 write('outbox/mixed.json',{source:'windows_chrome_x',finished_at:fresh,receipts:[{acquired_at:fresh,body:{tweets:[raw('1','expired source'),raw('2','fresh source')]}}]});
 write('outbox/old.json',{source:'windows_chrome_x',finished_at:old,receipts:[{acquired_at:old,body:{tweets:[raw('1','expired source')]}}]});
 write('state.json',{used:4,traffic:{total:380},budgetHistory:{unchanged:true}});
 const state=fs.readFileSync(path.join(root,'state.json'),'utf8');
 const r=clean(root,now);
 let cache=JSON.parse(fs.readFileSync(path.join(root,'cache-test.json')));
 assert.equal(cache.posts['1'].retention_expired,true);assert.equal(cache.posts['1'].text,undefined);assert.equal(cache.posts['2'].text,'fresh source');
 assert.equal(fs.existsSync(path.join(root,'outbox/old.json')),false);
 assert.equal(fs.readFileSync(path.join(root,'outbox/mixed.json'),'utf8').includes('expired source'),false);
 assert.equal(fs.readFileSync(path.join(root,'state.json'),'utf8'),state);
 // Emulate the already-loaded collector re-observing an expired post: first time must not move.
 cache.posts['1']={...t('1','resurrection attempt'),lastAcquiredAt:fresh};write('cache-test.json',cache);clean(root,now);
 cache=JSON.parse(fs.readFileSync(path.join(root,'cache-test.json')));assert.equal(cache.posts['1'].text,undefined);
 const idx=JSON.parse(fs.readFileSync(path.join(root,'post-retention-index.json')));assert.equal(idx.posts['1'],Date.parse(old));
 assert.equal(r.network,0);console.log(JSON.stringify({ok:true,checks:10,network:0}));
}finally{fs.rmSync(root,{recursive:true,force:true});}
