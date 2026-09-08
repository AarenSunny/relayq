export const dashboardHtml = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RelayQ Control Room</title>
  <style>
    :root { color-scheme: dark; --bg:#0a0c10; --panel:#11151c; --line:#242b36; --muted:#8f9bad; --text:#f2f5f8; --blue:#62a8ff; --green:#44d19d; --red:#ff6b7a; --amber:#f5bd61; }
    * { box-sizing:border-box } body { margin:0; background:radial-gradient(circle at 15% -10%,#17243b 0,transparent 38%),var(--bg); color:var(--text); font:14px/1.5 ui-sans-serif,system-ui,sans-serif; }
    main { width:min(1180px,calc(100% - 32px)); margin:0 auto; padding:38px 0 64px; }
    header { display:flex; align-items:flex-end; justify-content:space-between; margin-bottom:28px; }
    h1 { margin:0; font-size:32px; letter-spacing:-1px; } h1 span { color:var(--blue) } .eyebrow { color:var(--muted); text-transform:uppercase; letter-spacing:2px; font-size:11px; font-weight:700; }
    .connection { display:flex; gap:8px; align-items:center; color:var(--muted); } .dot { width:9px;height:9px;border-radius:50%;background:var(--amber);box-shadow:0 0 12px currentColor; }
    .connection.live .dot { background:var(--green) } .connection.offline .dot { background:var(--red) }
    .stats { display:grid; grid-template-columns:repeat(6,1fr); gap:10px; margin-bottom:18px; }
    .card,.panel { background:color-mix(in srgb,var(--panel) 94%,transparent); border:1px solid var(--line); border-radius:14px; box-shadow:0 18px 50px #0004; }
    .card { padding:16px; } .card b { display:block;font-size:26px;letter-spacing:-1px; } .card span { color:var(--muted);text-transform:uppercase;font-size:10px;letter-spacing:1.2px; }
    .grid { display:grid; grid-template-columns:minmax(0,1.65fr) minmax(290px,.85fr); gap:18px; }
    .panel { overflow:hidden; } .panel-head { display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid var(--line); } h2 { margin:0;font-size:15px; }
    table { width:100%;border-collapse:collapse; } th,td { padding:12px 16px;text-align:left;border-bottom:1px solid var(--line); } th { color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:1px; } td:first-child { font-family:ui-monospace,monospace;color:#c9d5e6; }
    .pill { display:inline-block;padding:3px 8px;border-radius:999px;background:#28303b;color:#c8d0da;font-size:11px; } .pill.succeeded { color:var(--green);background:#18352d } .pill.running { color:var(--blue);background:#182e49 } .pill.failed { color:var(--red);background:#3b1d24 } .pill.queued { color:var(--amber);background:#382d1c }
    form { padding:16px 18px;display:grid;gap:10px; } label { color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:1px; } input,textarea,button { width:100%;border-radius:9px;border:1px solid var(--line);background:#0c1016;color:var(--text);padding:10px 12px;font:inherit; } textarea { min-height:82px;resize:vertical;font-family:ui-monospace,monospace; } button { cursor:pointer;background:var(--blue);border-color:var(--blue);color:#07101c;font-weight:750; }
    .activity { max-height:460px;overflow:auto;padding:6px 18px 18px; } .event { display:grid;grid-template-columns:8px 1fr;gap:10px;padding:11px 0;border-bottom:1px solid var(--line); } .event-dot { width:7px;height:7px;border-radius:50%;background:var(--blue);margin-top:7px; } .event small { color:var(--muted);display:block; } .empty { color:var(--muted);padding:26px 18px;text-align:center; }
    @media (max-width:850px) { .stats{grid-template-columns:repeat(3,1fr)} .grid{grid-template-columns:1fr} header{align-items:flex-start;gap:12px;flex-direction:column} }
  </style>
</head>
<body><main>
  <header><div><div class="eyebrow">Distributed job infrastructure</div><h1>Relay<span>Q</span> Control Room</h1></div><div id="connection" class="connection"><i class="dot"></i><span>Connecting</span></div></header>
  <section id="stats" class="stats"></section>
  <div class="grid"><section class="panel"><div class="panel-head"><h2>Recent jobs</h2><span class="eyebrow">priority queue</span></div><div id="jobs"></div></section>
  <aside><section class="panel"><div class="panel-head"><h2>Submit work</h2></div><form id="submit"><label>Task type</label><input name="type" value="sum" required><label>JSON payload</label><textarea name="payload">[20, 22]</textarea><label>Priority</label><input name="priority" type="number" value="0"><button>Enqueue job</button></form></section>
  <section class="panel" style="margin-top:18px"><div class="panel-head"><h2>Live activity</h2><span class="eyebrow">event stream</span></div><div id="activity" class="activity"></div></section></aside></div>
</main><script>
  const statuses=['queued','running','succeeded','failed','cancelled','total'];
  const stats=document.querySelector('#stats'), jobs=document.querySelector('#jobs'), activity=document.querySelector('#activity'), connection=document.querySelector('#connection');
  const short=id=>id.slice(0,8); const time=value=>new Date(value).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});
  async function refresh(){
    const [counts,list]=await Promise.all([fetch('/stats').then(r=>r.json()),fetch('/jobs?limit=20').then(r=>r.json())]);
    stats.replaceChildren(...statuses.map(status=>{const el=document.createElement('div');el.className='card';el.innerHTML='<b></b><span></span>';el.querySelector('b').textContent=counts[status];el.querySelector('span').textContent=status;return el;}));
    if(!list.jobs.length){jobs.innerHTML='<div class="empty">No jobs have been submitted.</div>';return;}
    const table=document.createElement('table');table.innerHTML='<thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Attempt</th><th>Priority</th></tr></thead><tbody></tbody>';
    for(const job of list.jobs){const row=table.tBodies[0].insertRow();[short(job.id),job.type,'',job.attempts+'/'+job.maxAttempts,job.priority].forEach(v=>{const cell=row.insertCell();cell.textContent=v;});const pill=document.createElement('span');pill.className='pill '+job.status;pill.textContent=job.status;row.cells[2].append(pill);}
    jobs.replaceChildren(table);
  }
  function showEvent(event){const el=document.createElement('div');el.className='event';const dot=document.createElement('i');dot.className='event-dot';const body=document.createElement('div');const title=document.createElement('div');title.textContent=event.type.replaceAll('_',' ')+' · '+short(event.jobId);const meta=document.createElement('small');meta.textContent=time(event.createdAt)+(event.workerId?' · '+event.workerId:'');body.append(title,meta);el.append(dot,body);activity.prepend(el);while(activity.children.length>30)activity.lastChild.remove();}
  fetch('/events?limit=20').then(r=>r.json()).then(({events})=>events.forEach(showEvent));
  const stream=new EventSource('/events/stream');stream.onopen=()=>{connection.className='connection live';connection.querySelector('span').textContent='Live';};stream.onerror=()=>{connection.className='connection offline';connection.querySelector('span').textContent='Reconnecting';};stream.onmessage=message=>{showEvent(JSON.parse(message.data));refresh();};
  document.querySelector('#submit').addEventListener('submit',async event=>{event.preventDefault();const data=new FormData(event.currentTarget);let payload;try{payload=JSON.parse(data.get('payload'));}catch{alert('Payload must be valid JSON');return;}const response=await fetch('/jobs',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({type:data.get('type'),payload,priority:Number(data.get('priority'))})});if(!response.ok)alert((await response.json()).error);});
  refresh(); setInterval(refresh,10000);
</script></body></html>`;
