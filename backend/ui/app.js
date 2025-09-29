const qs = (s) => document.querySelector(s);
const qsa = (s) => Array.from(document.querySelectorAll(s));

function setActiveTab(name){
  qsa('.tab').forEach(b=>b.classList.toggle('active', b.dataset.tab===name));
  qsa('.tab-panel').forEach(p=>p.classList.toggle('active', p.id===`tab-${name}`));
}

qsa('.tab').forEach(b=>b.addEventListener('click',()=>setActiveTab(b.dataset.tab)));

const fileInput = qs('#fileInput');
const btnPreflight = qs('#btnPreflight');
const btnValidate = qs('#btnValidate');
const btnRender = qs('#btnRender');
const runIdEl = qs('#runId');
const durationEl = qs('#duration');
const tablesIndexLink = qs('#tablesIndexLink');
const messagesBody = qs('#messagesBody');
const tplFrame = qs('#tplFrame');
const hlNs = qs('#hlNs');
const hlLn = qs('#hlLn');
const hlCx = qs('#hlCx');
const btnApplyHighlight = qs('#btnApplyHighlight');
const filterSeverity = qs('#filterSeverity');
const filterCategory = qs('#filterCategory');
const btnApplyFilter = qs('#btnApplyFilter');
const pfPassed = qs('#pfPassed');
const pfFailed = qs('#pfFailed');
const pfItemsCount = qs('#pfItemsCount');
const preflightBody = qs('#preflightBody');
const tplTabBar = qs('#tplTabBar');
const bottomProgress = qs('#bottomProgress');
const provBlocked = qs('#provBlocked');
const provMappings = qs('#provMappings');
const provDecisions = qs('#provDecisions');
const provOffline = qs('#provOffline');
const provMapBody = qs('#provMapBody');
const provNetBody = qs('#provNetBody');

function setTaskPercent(task, percent, label){
  if(!bottomProgress) return;
  const item = bottomProgress.querySelector(`.bp-item[data-task="${task}"]`);
  if(!item) return;
  const fill = item.querySelector('.bp-fill');
  const stateEl = item.querySelector('.bp-state');
  const pct = Math.max(0, Math.min(100, Math.round(percent||0)));
  if(fill){ fill.style.width = pct + '%'; }
  if(stateEl){ stateEl.textContent = (label || '') ? `${pct}% ${label}` : `${pct}%`; }
}

async function pollProgress(jobId, task){
  if(!jobId) return;
  try{
    const r = await fetch(`/api/v1/progress?job_id=${encodeURIComponent(jobId)}`);
    if(!r.ok) return;
    const j = await r.json();
    if(j && j.task === task){
      if(j.status === 'running'){
        setTaskPercent(task, j.percent || 0, j.message || '');
      } else if (j.status === 'success'){
        setTaskPercent(task, 100, 'done');
        setTaskState(task, 'success');
        return; // stop polling
      } else if (j.status === 'error'){
        setTaskPercent(task, j.percent || 0, 'error');
        setTaskState(task, 'error');
        return; // stop polling
      }
    }
  }catch(_e){}
  setTimeout(()=>pollProgress(jobId, task), 600);
}

function setTaskState(task, state){
  if(!bottomProgress) return;
  const item = bottomProgress.querySelector(`.bp-item[data-task="${task}"]`);
  if(!item) return;
  item.classList.remove('running','success','error');
  const stateEl = item.querySelector('.bp-state');
  if(state==='running'){
    item.classList.add('running');
    if(stateEl) stateEl.textContent = 'running';
    const fill = item.querySelector('.bp-fill');
    if(fill){ fill.style.width = '40%'; }
  } else if(state==='success'){
    item.classList.add('success');
    if(stateEl) stateEl.textContent = 'done';
    const fill = item.querySelector('.bp-fill');
    if(fill){ fill.style.width = '100%'; }
  } else if(state==='error'){
    item.classList.add('error');
    if(stateEl) stateEl.textContent = 'error';
    const fill = item.querySelector('.bp-fill');
    if(fill){ fill.style.width = '100%'; }
  } else {
    if(stateEl) stateEl.textContent = 'idle';
    const fill = item.querySelector('.bp-fill');
    if(fill){ fill.style.width = '0'; }
  }
}

let currentRunId = '';
let currentTablesBase = '';

function populateCategoryFilter(messages){
  if(!filterCategory) return;
  const selected = filterCategory.value;
  const categories = new Set();
  for(const m of messages){
    const c = (m && m.category) ? String(m.category) : '';
    if(c) categories.add(c);
  }
  const options = ['']
    .concat(Array.from(categories).sort((a,b)=>a.localeCompare(b)));
  filterCategory.innerHTML = options.map(v=>{
    const label = v || '(all)';
    const safe = v.replace(/"/g,'&quot;');
    return `<option value="${safe}">${label}</option>`;
  }).join('');
  if(options.includes(selected)){
    filterCategory.value = selected;
  }
}

async function postForm(url, form){
  const res = await fetch(url,{ method:'POST', body:form });
  if(!res.ok) throw new Error(await res.text());
  return res.json();
}

function selectedFile(){ return (fileInput && fileInput.files && fileInput.files[0]) || null; }

async function runPreflight(){
  const f = selectedFile(); if(!f) return alert('Select a file first');
  const {token} = await chunkedUpload(f);
  const form = new FormData(); form.append('upload_token', token); form.append('light','true');
  const clientRunId = crypto.randomUUID(); form.append('client_run_id', clientRunId);
  const t0 = performance.now();
  try{
    setTaskState('preflight','running');
    pollProgress(clientRunId, 'preflight');
    const data = await postForm('/api/v1/preflight', form);
    const dur = Math.round(performance.now()-t0);
    runIdEl.textContent = '(n/a)'; durationEl.textContent = `${dur} ms`;
    setActiveTab('summary');
    pfPassed.textContent = data.passed;
    pfFailed.textContent = data.failed;
    const items = data.items || [];
    pfItemsCount.textContent = items.length;
    preflightBody.innerHTML='';
    for(const it of items){
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${it.status||''}</td><td>${it.id||''}</td><td>${it.message||''}</td>`;
      preflightBody.appendChild(tr);
    }
    setActiveTab('preflight');
    setTaskState('preflight','success');
  }catch(e){
    setTaskState('preflight','error');
    throw e;
  }
}

async function runValidate(){
  const f = selectedFile(); if(!f) return alert('Select a file first');
  const {token} = await chunkedUpload(f);
  const form = new FormData(); form.append('upload_token', token); form.append('profile','full');
  const clientRunId = crypto.randomUUID(); form.append('client_run_id', clientRunId);
  const t0 = performance.now();
  try{
    setTaskState('validate','running');
    pollProgress(clientRunId, 'validate');
    const data = await postForm('/api/v1/validate', form);
    const dur = Math.round(performance.now()-t0);
    currentRunId = data.run_id || '';
    runIdEl.textContent = currentRunId || '-';
    durationEl.textContent = `${dur} ms`;
    tablesIndexLink.textContent = 'Tables Index';
    tablesIndexLink.href = data.tables_index_url || '#';
    currentTablesBase = (data.tables_index_url || '').replace('/index.html','');
    qs('#status').textContent = data.status;
    qs('#factsCount').textContent = data.facts_count;
    const cats = (data.metrics && data.metrics.category_counts) || {};
    qs('#cntXbrl21').textContent = cats.xbrl21 || 0;
    qs('#cntDim').textContent = cats.dimensions || 0;
    qs('#cntCalc').textContent = cats.calculation || 0;
    qs('#cntForm').textContent = cats.formulas || 0;
    qs('#cntEba').textContent = cats.eba_filing || 0;
    // Populate messages
    messagesBody.innerHTML='';
    const errs = (data.errors||[]).map(e=>({...e, severity:'error'}));
    const warns = (data.warnings||[]).map(e=>({...e, severity:'warning'}));
    const all = [...errs, ...warns];
    window.__allMessages = all;
    populateCategoryFilter(all);
    for(const m of all){
      const tr = document.createElement('tr');
      const rule = m.rule_id || m.code || '';
      const msg = m.readable_message || m.message || '';
      tr.innerHTML = `<td>${m.severity||''}</td><td>${m.category||''}</td><td>${rule}</td>`;
      tr.appendChild((()=>{ const td=document.createElement('td'); td.innerHTML=msg; return td; })());
      tr.addEventListener('click',async()=>{
        if(!currentTablesBase) return;
        const params = new URLSearchParams();
        if(m.conceptNs) params.set('conceptNs', m.conceptNs);
        if(m.conceptLn) params.set('conceptLn', m.conceptLn);
        if(m.contextRef) params.set('contextRef', m.contextRef);
        if(m.message) params.set('errorText', m.message);
        if(m.rowCode) params.set('rowCode', String(m.rowCode));
        if(m.colCode) params.set('colCode', String(m.colCode));
        let target = '';
        if(m.table_id){
          target = `${currentTablesBase}/${m.table_id}.html`;
        } else if (m.conceptNs && m.conceptLn) {
          // Ask backend for candidate tables
          try{
            const q = new URLSearchParams({run_id: currentRunId, conceptNs: m.conceptNs, conceptLn: m.conceptLn});
            if(m.contextRef) q.set('contextRef', m.contextRef);
            if(m.id) q.set('messageId', m.id);
            const r = await fetch(`/api/v1/render/for-error?${q.toString()}`);
            if(r.ok){
              const j = await r.json();
              const cand = (j.candidates||[])[0];
              if(cand && cand.url){ target = cand.url; }
            }
          }catch(_e){}
          if(!target){ target = `${currentTablesBase}/eba_tC_00.01.html`; }
        } else {
          target = `${currentTablesBase}/eba_tC_00.01.html`;
        }
        const hasQ = target.includes('?');
        const qp = params.toString();
        if(qp) target = target + (hasQ? '&' : '?') + qp;
        tplFrame.src = target; setActiveTab('templates');
      });
      messagesBody.appendChild(tr);
    }
    // Provenance summary
    try{
      const offline = (data.metrics && data.metrics.offline_attempted_urls) || [];
      const prov = (data.metrics && data.metrics.provenance) || {};
      const offlineMode = (data.metrics && data.metrics.offline_mode) || (data.dts_evidence && data.dts_evidence.offline_mode);
      if(provBlocked) provBlocked.textContent = String(offline.length||0);
      if(provMappings) provMappings.textContent = String(prov.url_mappings_count||0);
      if(provDecisions) provDecisions.textContent = String(prov.network_decisions_count||0);
      if(provOffline) provOffline.textContent = (offlineMode===false)? 'no' : 'yes';
      if(provMapBody){
        provMapBody.innerHTML='';
        for(const it of (prov.url_mappings_sample||[])){
          const tr = document.createElement('tr');
          tr.innerHTML = `<td>${(it.requested||'')}</td><td>${(it.mapped||'')}</td><td>${(it.source||'')}</td>`;
          provMapBody.appendChild(tr);
        }
      }
      if(provNetBody){
        provNetBody.innerHTML='';
        for(const it of (prov.network_decisions_sample||[])){
          const tr = document.createElement('tr');
          tr.innerHTML = `<td>${(it.decision||'')}</td><td>${(it.rule||'')}</td><td>${(it.url||'')}</td>`;
          provNetBody.appendChild(tr);
        }
      }
    }catch(_e){}
    setActiveTab('summary');
    setTaskState('validate','success');
  }catch(e){
    setTaskState('validate','error');
    throw e;
  }
}

async function runRender(){
  const f = selectedFile(); if(!f) return alert('Select a file first');
  const form = new FormData(); form.append('file', f); form.append('lang','en');
  const clientRunId = crypto.randomUUID(); form.append('client_run_id', clientRunId);
  try{
    setTaskState('render','running');
    pollProgress(clientRunId, 'render');
    const data = await postForm('/api/v1/render/tableset', form);
    tablesIndexLink.textContent = 'Tables Index';
    tablesIndexLink.href = data.index_url || '#';
    currentTablesBase = (data.index_url || '').replace('/index.html','');
    setActiveTab('templates');
    // Load first table for convenience
    if(data.index_url){ tplFrame.src = data.index_url.replace('/index.html','/eba_tC_00.01.html'); }
    // Load tables.json and render tabs
    try{
      const t = await fetch(currentTablesBase + '/tables.json');
      if(t.ok){
        const list = await t.json();
        tplTabBar.innerHTML='';
        for(const it of list){
          const b = document.createElement('button');
          b.textContent = it.tableLabel || it.tableId;
          b.addEventListener('click',()=>{
            // lazy-load iframe by switching src
            tplFrame.src = `${currentTablesBase}/${it.tableId}.html`;
            qsa('.tpl-tabs button').forEach(x=>x.classList.remove('active'));
            b.classList.add('active');
          });
          tplTabBar.appendChild(b);
        }
        const first = tplTabBar.querySelector('button'); if(first) first.classList.add('active');
      }
    }catch(_e){}
    setTaskState('render','success');
  }catch(e){
    setTaskState('render','error');
    throw e;
  }
}

btnPreflight.addEventListener('click',()=>runPreflight().catch(e=>alert(e)));
btnValidate.addEventListener('click',()=>runValidate().catch(e=>alert(e)));

// -------- Chunked upload client (simple) --------
async function chunkedUpload(file){
  const init = new FormData();
  init.append('filename', file.name);
  init.append('total_bytes', String(file.size||0));
  const r0 = await fetch('/api/v1/upload/init',{method:'POST', body:init});
  if(!r0.ok) throw new Error(await r0.text());
  const j0 = await r0.json();
  const token = j0.upload_token;
  const chunkSize = 5*1024*1024;
  let index = 0;
  for(let offset=0; offset<file.size; offset+=chunkSize){
    const blob = file.slice(offset, Math.min(offset+chunkSize, file.size));
    const form = new FormData();
    form.append('upload_token', token);
    form.append('index', String(index++));
    form.append('chunk', new File([blob], file.name));
    const r = await fetch('/api/v1/upload/chunk',{method:'POST', body:form});
    if(!r.ok) throw new Error(await r.text());
  }
  const done = new FormData(); done.append('upload_token', token);
  const r1 = await fetch('/api/v1/upload/complete',{method:'POST', body:done});
  if(!r1.ok) throw new Error(await r1.text());
  return { token };
}
btnRender.addEventListener('click',()=>runRender().catch(e=>alert(e)));

btnApplyHighlight.addEventListener('click',()=>{
  if(!tplFrame.src) return;
  const url = new URL(tplFrame.src);
  const ns = hlNs.value.trim(); const ln = hlLn.value.trim(); const cx = hlCx.value.trim();
  if(ns) url.searchParams.set('conceptNs', ns); else url.searchParams.delete('conceptNs');
  if(ln) url.searchParams.set('conceptLn', ln); else url.searchParams.delete('conceptLn');
  if(cx) url.searchParams.set('contextRef', cx); else url.searchParams.delete('contextRef');
  tplFrame.src = url.toString();
});

btnApplyFilter.addEventListener('click',()=>{
  const sev = filterSeverity.value.trim();
  const cat = filterCategory.value.trim();
  const source = window.__allMessages || [];
  messagesBody.innerHTML='';
  for(const m of source){
    if(sev && (m.severity!==sev)) continue;
    if(cat && (String(m.category||'')!==cat)) continue;
    const tr = document.createElement('tr');
    const rule = m.rule_id || m.code || '';
    const msg = m.readable_message || m.message || '';
    tr.innerHTML = `<td>${m.severity||''}</td><td>${m.category||''}</td><td>${rule}</td>`;
    tr.appendChild((()=>{ const td=document.createElement('td'); td.innerHTML=msg; return td; })());
    tr.addEventListener('click',async()=>{
      if(!currentTablesBase) return;
      const params = new URLSearchParams();
      if(m.conceptNs) params.set('conceptNs', m.conceptNs);
      if(m.conceptLn) params.set('conceptLn', m.conceptLn);
      if(m.contextRef) params.set('contextRef', m.contextRef);
      if(m.message) params.set('errorText', m.message);
      let target = '';
      if(m.table_id){ target = `${currentTablesBase}/${m.table_id}.html`; }
      else { target = `${currentTablesBase}/eba_tC_00.01.html`; }
      const hasQ = target.includes('?');
      const qp = params.toString();
      if(qp) target = target + (hasQ? '&' : '?') + qp;
      tplFrame.src = target; setActiveTab('templates');
    });
    messagesBody.appendChild(tr);
  }
});


