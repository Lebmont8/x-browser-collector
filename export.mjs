import {AUTHORS} from './policy.mjs';
import fs from 'node:fs';import path from 'node:path';import zlib from 'node:zlib';import crypto from 'node:crypto';
const root=path.resolve(process.env.X_COLLECTOR_HOME||path.join(process.cwd(),'.state')),dir=path.join(root,'outbox');
if(process.argv.includes('--list')){
 const files=fs.existsSync(dir)?fs.readdirSync(dir).filter(n=>/^[0-9TZ-]+\.json$/.test(n)).map(name=>{const p=path.join(dir,name),b=fs.readFileSync(p),r=JSON.parse(b);if(r.source!=='windows_chrome_x')throw Error('source');return {name,ok:r.ok,error:r.error,author:r.author,acquired_at:r.receipts?.map(x=>x.acquired_at)||[],bytes:b.length,sha256:crypto.createHash('sha256').update(b).digest('hex')};}):[];
 const caches=AUTHORS.map(a=>{const p=path.join(root,'cache-'+a+'.json');const c=fs.existsSync(p)?JSON.parse(fs.readFileSync(p)):{};const posts=Object.values(c.posts||{});return {author:a,total_cached_objects:posts.length,seed_author_posts:posts.filter(x=>x.author.toLowerCase()===a.toLowerCase()).length,highWaterId:c.highWaterId,lastAcquiredAt:c.lastAcquiredAt};});
 console.log(JSON.stringify({files,caches,network:0}));
}else{
 const name=process.argv.find(x=>x.startsWith('--file='))?.slice(7);
 if(!/^[0-9TZ-]+\.json$/.test(name||''))throw Error('file_name');
 const p=path.join(dir,name),b=fs.readFileSync(p);if(b.length>8388608)throw Error('export_size');
 const v=JSON.parse(b);if(v.source!=='windows_chrome_x')throw Error('source');
 console.log('GZIP:'+zlib.gzipSync(b).toString('base64'));
}
