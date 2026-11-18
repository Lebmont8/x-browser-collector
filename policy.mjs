import crypto from 'node:crypto';
export const LIMIT={perRun:4,perDay:24,totalPerDay:720,minRunMs:3600000,minRequestMs:2000,maxIntervalMs:21600000,profileTtlMs:86400000,timeoutMs:45000,maxBody:2097152};
export const AUTHORS=(process.env.X_AUTHORS||'example_author,example_writer').split(',').map(x=>x.trim()).filter(Boolean);
if(!AUTHORS.length||AUTHORS.some(x=>!/^\w{1,15}$/.test(x)))throw Error('invalid_author_configuration');
export function normalize(s={}){
 s.streams??={};s.cacheStats??={hits:0};s.traffic??={day:s.day,total:s.used||0,unknownBeforeV2:true};s.version=2;return s;
}
export function day(t){return new Date(t).toISOString().slice(0,10);}
export function roll(s,t){if(s.day!==day(t)){(s.budgetHistory??={})[s.day||'unknown']={used:s.used||0,traffic:s.traffic};s.day=day(t);s.used=0;s.traffic={day:s.day,total:0,unknownBeforeV2:false};}return s;}
export function dueAt(s,author,at=Date.now()){normalize(s);const a=s.streams[author]||{};let due=Math.max((s.lastRun||0)+LIMIT.minRunMs,(a.lastSuccess||0)+(a.intervalMs||LIMIT.minRunMs));if(s.day===day(at)&&(s.used>=LIMIT.perDay||s.traffic.total>=LIMIT.totalPerDay))due=Math.max(due,Date.parse(day(at)+'T00:00:00Z')+86400000);return due;}
export function gate(s,author,t){normalize(s);if(s.blocked)return 'blocked_manual_check';if(!AUTHORS.includes(author))return 'author_not_allowed';if(s.day===day(t)&&(s.used>=LIMIT.perDay||s.traffic.total>=LIMIT.totalPerDay))return 'daily_cap';if(t<dueAt(s,author,t))return 'min_run_interval';return null;}
export function reserve(s,run,kind,t){normalize(s);roll(s,t);if(s.blocked)throw Error('blocked_manual_check');if(s.traffic.total>=LIMIT.totalPerDay)throw Error('traffic_cap');if(kind==='data'&&(s.used>=LIMIT.perDay||run.reads>=LIMIT.perRun))throw Error(s.used>=LIMIT.perDay?'daily_cap':'run_cap');s.traffic.total++;run.total++;if(kind==='data'){s.used=(s.used||0)+1;run.reads++;}return s;}
export function op(url){try{return new URL(url).pathname.split('/').at(-1);}catch{return '';}}
export function decision(r,author){
 const u=new URL(r.url),name=op(r.url),x=['x.com','api.x.com'].includes(u.hostname);
 if(/\/i\/flow\/login|\/account\/access|\/i\/jf\/onboarding/.test(u.pathname))return {deny:'auth_redirect',stop:true};
 if(r.method!=='GET')return {deny:'non_get'};
 if(['Image','Media','Font'].includes(r.type))return {deny:'media'};
 if(r.type==='Document')return r.url==='https://x.com/'+author?{kind:'static'}:{deny:'unexpected_redirect',stop:true};
 if(x&&u.pathname.includes('/graphql/')){
  if(['UserByScreenName','UserTweets','UserOriginalsTimeline'].includes(name))return {kind:'data',operation:name};
  return {deny:'api_not_allowed'};
 }
 if(x&&['XHR','Fetch','EventSource','WebSocket'].includes(r.type))return {deny:'unrelated_xhr'};
 if(!['x.com','abs.twimg.com'].includes(u.hostname))return {deny:'unrelated_host'};
 return {kind:'static'};
}
export function failure(status,url='',body=null){
 if(status===429)return 'rate_limited';
 if(status===401||status===403)return 'auth_or_access_denied';
 if(/\/i\/flow\/login|\/account\/access|\/i\/jf\/onboarding/.test(url))return 'auth_redirect';
 if(status>=300&&status<400)return 'unexpected_redirect';
 if(status>=400)return 'http_error';
 if(body?.errors?.length)return body.errors.some(e=>[32,89,326].includes(e.code))?'auth_or_challenge':'graphql_errors';
 return null;
}
export function domFailure(d){if(d.challenge||d.login||/\/i\/flow\/login|\/account\/access|\/i\/jf\/onboarding/.test(d.url||''))return 'auth_or_challenge';return null;}
export function tweets(body){
 const found=new Map();function walk(v){if(!v||typeof v!=='object')return;if(v.rest_id&&v.legacy&&typeof v.legacy.full_text==='string'){
 const user=v.core?.user_results?.result||{},author=user.core?.screen_name||user.legacy?.screen_name||'';
 const text=v.note_tweet?.note_tweet_results?.result?.text||v.legacy.full_text;
 const id=String(v.rest_id);if(/^\d+$/.test(id)){const stable={id,text,author,reply:v.legacy.in_reply_to_status_id_str||null,quote:v.legacy.quoted_status_id_str||null,article:v.article?.article_results?.result?.content_state?.blocks||null};found.set(id,{id,author,text,hash:crypto.createHash('sha256').update(JSON.stringify(stable)).digest('hex')});}
 }for(const c of Object.values(v))if(c&&typeof c==='object')walk(c);}
 walk(body);return [...found.values()];
}
export function updateCache(cache,body,author,acquiredAt){
 cache.posts??={};const result={newIds:[],changedIds:[],knownIds:[],authorPosts:0,overlapReached:false};
 for(const t of tweets(body)){const old=cache.posts[t.id];if(!old)result.newIds.push(t.id);else if(old.hash!==t.hash)result.changedIds.push(t.id);else result.knownIds.push(t.id);
 if(old&&t.author.toLowerCase()===author.toLowerCase())result.overlapReached=true;
 cache.posts[t.id]={...t,firstAcquiredAt:old?.firstAcquiredAt||acquiredAt,lastAcquiredAt:acquiredAt};
 if(t.author.toLowerCase()===author.toLowerCase()){result.authorPosts++;if(!cache.highWaterId||BigInt(t.id)>BigInt(cache.highWaterId))cache.highWaterId=t.id;}
 }if(!result.authorPosts)throw Error('timeline_schema_or_wrong_author');
 cache.lastAcquiredAt=acquiredAt;cache.author=author;return result;
}
export function success(s,author,result,at){const a=s.streams[author]??={};a.emptyStreak=(result.newIds.length||result.changedIds.length)?0:(a.emptyStreak||0)+1;a.intervalMs=Math.min(LIMIT.maxIntervalMs,LIMIT.minRunMs*2**Math.min(a.emptyStreak,8));a.lastSuccess=at;a.lastDelta={new:result.newIds.length,changed:result.changedIds.length,overlap:result.overlapReached};return a;}
export function cachedProfile(cache,t){return !!(cache.profile&&t-cache.profile.acquiredAt<LIMIT.profileTtlMs);}

export function dispatchGuard(s,run,{now=Date.now(),until=Infinity,stopFile=false,aborted=false}={}){
 if(run?.stopped)return run.stopped;
 if(stopFile||aborted)return 'owner_stop_file';
 if(now>=until)return 'bounded_session_complete';
 if(s.blocked)return 'blocked_manual_check';
 return null;
}
export const DOM_EXPRESSION="JSON.stringify({url:location.href,challenge:!!document.querySelector('iframe[src*=captcha],iframe[src*=arkoselabs],iframe[src*=hcaptcha],input[name*=verification],input[autocomplete=one-time-code]')||Array.from(document.querySelectorAll('h1,h2,[role=dialog],[data-testid=error-detail]')).some(e=>/captcha|verify (your|you are)|unusual activity|temporarily (limited|restricted)|account (locked|suspended)|confirm your identity|провер.{0,12}(личност|человек)|подозрительн.{0,12}активност|аккаунт.{0,12}(заблокирован|ограничен)/i.test(e.innerText||'')),login:!!document.querySelector('input[name=text],input[name=password]')})";
