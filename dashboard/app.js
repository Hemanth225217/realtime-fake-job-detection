let isStreaming = false;
let currentTps = 2.0;
let pollTimer = null;
let cachedJobs = {};

document.addEventListener('DOMContentLoaded', () => {
  if (window.lucide) {
    lucide.createIcons();
  }
  checkHealth();
  loadBenchmarks();
  startMetricsPolling();

  const boostSlider = document.getElementById('fraudBoostSlider');
  if (boostSlider) {
    boostSlider.addEventListener('input', (e) => {
      document.getElementById('boostVal').innerText = `${e.target.value}%`;
    });
  }
});

async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    const data = await res.json();
    const statusText = document.getElementById('statusText');
    const statusBadge = document.getElementById('statusBadge');
    const dbBackend = document.getElementById('dbBackend');

    if (data.status === 'healthy') {
      statusText.innerText = data.streaming_active ? 'Streaming Active' : 'System Ready';
      statusBadge.querySelector('span:first-child').className = data.streaming_active ? 'w-2.5 h-2.5 rounded-full bg-emerald-400 pulse-fast' : 'w-2.5 h-2.5 rounded-full bg-emerald-400';
      dbBackend.innerText = data.database_backend.toUpperCase();
      isStreaming = data.streaming_active;
      updateStreamButtonUI();
    }
  } catch (e) {
    console.error('Health check failed', e);
    document.getElementById('statusText').innerText = 'Offline';
  }
}

function setTps(val) {
  currentTps = val;
  document.querySelectorAll('.tps-btn').forEach(btn => {
    btn.classList.remove('bg-emerald-500/20', 'border-emerald-500', 'text-emerald-300');
    if (btn.innerText.includes(`${val} TPS`)) {
      btn.classList.add('bg-emerald-500/20', 'border-emerald-500', 'text-emerald-300');
    }
  });
}

async function toggleStreaming() {
  const boost = document.getElementById('fraudBoostSlider').value / 100.0;
  if (!isStreaming) {
    try {
      const res = await fetch('/api/stream/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tps: currentTps, fraud_boost: boost })
      });
      const data = await res.json();
      isStreaming = true;
      updateStreamButtonUI();
      document.getElementById('statusText').innerText = 'Streaming Active';
      document.getElementById('statusBadge').querySelector('span:first-child').className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 pulse-fast';
    } catch (e) {
      alert('Failed to start streaming: ' + e);
    }
  } else {
    try {
      await fetch('/api/stream/stop', { method: 'POST' });
      isStreaming = false;
      updateStreamButtonUI();
      document.getElementById('statusText').innerText = 'Stream Paused';
      document.getElementById('statusBadge').querySelector('span:first-child').className = 'w-2.5 h-2.5 rounded-full bg-amber-400';
    } catch (e) {
      alert('Failed to stop streaming: ' + e);
    }
  }
}

function updateStreamButtonUI() {
  const btn = document.getElementById('btnStartStream');
  const label = document.getElementById('streamBtnLabel');
  if (isStreaming) {
    btn.className = "flex-1 py-3 px-4 rounded-xl font-semibold text-xs tracking-wider uppercase transition flex items-center justify-center space-x-2 bg-rose-600 text-white shadow-lg shadow-rose-600/25 hover:bg-rose-500";
    label.innerText = "Pause Live Stream";
  } else {
    btn.className = "flex-1 py-3 px-4 rounded-xl font-semibold text-xs tracking-wider uppercase transition flex items-center justify-center space-x-2 bg-gradient-to-r from-emerald-500 to-teal-600 text-black shadow-lg shadow-emerald-500/25 hover:brightness-110";
    label.innerText = "Launch Real-Time Stream";
  }
  if (window.lucide) lucide.createIcons();
}

function startMetricsPolling() {
  pollTimer = setInterval(async () => {
    await fetchMetrics();
    await fetchRecentJobs();
  }, 1000);
}

async function fetchMetrics() {
  try {
    const res = await fetch('/api/stream/metrics');
    const data = await res.json();
    const rt = data.realtime_telemetry;
    const db = data.database_totals;

    document.getElementById('kpiTps').innerText = rt.current_throughput_tps.toFixed(1);
    document.getElementById('kpiTotalProcessed').innerText = db.total_jobs_processed.toLocaleString();
    document.getElementById('kpiFraudRate').innerText = `${rt.recent_fraud_rate_pct.toFixed(1)}%`;
    document.getElementById('kpiFraudCount').innerText = db.fraudulent_detected.toLocaleString();
    document.getElementById('kpiLatency').innerText = rt.avg_latency_ms.toFixed(2);
    document.getElementById('kpiBatchCount').innerText = rt.batch_count;
    document.getElementById('kpiAlertCount').innerText = db.fraudulent_detected;
  } catch (e) {
    console.error('Error fetching metrics', e);
  }
}

async function fetchRecentJobs() {
  try {
    const res = await fetch('/api/stream/recent?limit=15');
    const jobs = await res.json();
    if (!jobs || jobs.length === 0) return;

    const tbody = document.getElementById('streamFeedBody');
    tbody.innerHTML = '';

    jobs.forEach(job => {
      cachedJobs[job.job_id] = job;
      const isFraud = job.prediction === 'FRAUDULENT';
      const tr = document.createElement('tr');
      tr.className = "hover:bg-slate-800/40 transition cursor-pointer";
      tr.onclick = () => openXaiModal(job.job_id);

      const statusBadge = isFraud
        ? `<span class="px-2.5 py-1 rounded-full bg-red-950/80 border border-red-500/40 text-red-300 font-bold text-[11px] inline-flex items-center gap-1.5"><span class="w-1.5 h-1.5 rounded-full bg-red-400 pulse-fast"></span>FRAUD</span>`
        : `<span class="px-2.5 py-1 rounded-full bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-bold text-[11px] inline-flex items-center gap-1.5"><span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span>REAL</span>`;

      const riskPill = getRiskPill(job.risk_level);

      tr.innerHTML = `
        <td class="py-3 px-4 font-mono text-slate-400 text-[11px]">${job.timestamp ? job.timestamp.split(' ')[1] : '--'}</td>
        <td class="py-3 px-4 font-semibold text-white max-w-[220px] truncate" title="${job.title}">
          ${job.title}
          <div class="text-[10px] text-slate-500 font-mono">ID: ${job.job_id}</div>
        </td>
        <td class="py-3 px-4 text-slate-300 max-w-[150px] truncate">${job.company_name}</td>
        <td class="py-3 px-4">${statusBadge}</td>
        <td class="py-3 px-4 font-mono text-slate-300">${(job.confidence_score * 100).toFixed(1)}%</td>
        <td class="py-3 px-4">${riskPill}</td>
        <td class="py-3 px-4 font-mono text-cyan-400">${job.latency_ms.toFixed(1)} ms</td>
        <td class="py-3 px-4 text-right">
          <button class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-[11px] border border-slate-700">Audit XAI</button>
        </td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    console.error('Error fetching stream feed', e);
  }
}

function getRiskPill(risk) {
  if (risk === 'CRITICAL') return `<span class="px-2 py-0.5 rounded bg-red-900/60 text-red-300 font-bold text-[10px] border border-red-700">CRITICAL</span>`;
  if (risk === 'HIGH') return `<span class="px-2 py-0.5 rounded bg-amber-900/60 text-amber-300 font-bold text-[10px] border border-amber-700">HIGH</span>`;
  if (risk === 'MEDIUM') return `<span class="px-2 py-0.5 rounded bg-yellow-900/60 text-yellow-300 font-bold text-[10px] border border-yellow-700">MEDIUM</span>`;
  return `<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-300 font-medium text-[10px]">LOW</span>`;
}

function openXaiModal(jobId) {
  const job = cachedJobs[jobId];
  if (!job) return;

  const card = job.explanation_card || {};
  document.getElementById('modalTitle').innerText = job.title;
  document.getElementById('modalSubtitle').innerText = `Job ID: ${job.job_id} | Location: ${job.location} | Latency: ${job.latency_ms} ms`;

  // Risk Icon
  const iconContainer = document.getElementById('modalRiskIcon');
  if (job.prediction === 'FRAUDULENT') {
    iconContainer.className = "w-9 h-9 rounded-xl bg-red-500/20 text-red-400 flex items-center justify-center border border-red-500/40";
    iconContainer.innerHTML = `<i data-lucide="alert-triangle" class="w-5 h-5"></i>`;
  } else {
    iconContainer.className = "w-9 h-9 rounded-xl bg-emerald-500/20 text-emerald-400 flex items-center justify-center border border-emerald-500/40";
    iconContainer.innerHTML = `<i data-lucide="check-circle" class="w-5 h-5"></i>`;
  }

  // Recommendation Banner
  const banner = document.getElementById('modalRecommendationBanner');
  banner.innerText = card.recommendation || job.explanation;
  if (job.risk_level === 'CRITICAL' || job.risk_level === 'HIGH') {
    banner.className = "p-4 rounded-xl border border-red-500/50 bg-red-950/40 text-red-200 text-xs leading-relaxed font-medium";
  } else {
    banner.className = "p-4 rounded-xl border border-emerald-500/50 bg-emerald-950/40 text-emerald-200 text-xs leading-relaxed font-medium";
  }

  // Bullets
  const bulletsUl = document.getElementById('modalBullets');
  bulletsUl.innerHTML = '';
  const bullets = card.explanation_bullets || [job.explanation];
  bullets.forEach(b => {
    const li = document.createElement('li');
    li.innerText = b;
    bulletsUl.appendChild(li);
  });

  // Tokens
  const fraudTokensDiv = document.getElementById('modalFraudTokens');
  const legitTokensDiv = document.getElementById('modalLegitTokens');
  fraudTokensDiv.innerHTML = '';
  legitTokensDiv.innerHTML = '';

  const attributions = card.token_attributions || job.token_attributions || {};
  const fraudDrivers = attributions.fraud_drivers || [];
  const legitDrivers = attributions.legitimacy_drivers || [];

  if (fraudDrivers.length === 0) {
    fraudTokensDiv.innerHTML = '<span class="text-slate-500 italic text-[11px]">No significant positive fraud weights.</span>';
  } else {
    fraudDrivers.forEach(t => {
      const span = document.createElement('span');
      span.className = 'token-fraud text-xs';
      span.innerText = `${t.token} (+${t.weight.toFixed(3)})`;
      fraudTokensDiv.appendChild(span);
    });
  }

  if (legitDrivers.length === 0) {
    legitTokensDiv.innerHTML = '<span class="text-slate-500 italic text-[11px]">No significant negative legitimacy weights.</span>';
  } else {
    legitDrivers.forEach(t => {
      const span = document.createElement('span');
      span.className = 'token-legit text-xs';
      span.innerText = `${t.token} (${t.weight.toFixed(3)})`;
      legitTokensDiv.appendChild(span);
    });
  }

  // Meta indicators
  const breakdown = card.forensic_breakdown || {};
  const brand = breakdown.identity_and_brand || {};
  const fin = breakdown.financial_risks || {};
  const comm = breakdown.communication_redirection || {};

  document.getElementById('modalMetaBrand').innerText = brand.has_company_logo ? 'Verified Logo' : 'No Logo Uploaded';
  document.getElementById('modalMetaPayment').innerText = fin.payment_scam_flags > 0 ? `${fin.payment_scam_flags} Suspicious Triggers` : 'None';
  document.getElementById('modalMetaOffPlatform').innerText = comm.off_platform_flags > 0 ? `${comm.off_platform_flags} External Handles` : 'Compliant';
  document.getElementById('modalMetaEmail').innerText = fin.free_email_domain ? 'Public / Unverified' : 'Corporate Domain';

  document.getElementById('xaiModal').classList.remove('hidden');
  if (window.lucide) lucide.createIcons();
}

function closeModal() {
  document.getElementById('xaiModal').classList.add('hidden');
}

async function evaluateSandbox(e) {
  e.preventDefault();
  const btn = document.getElementById('btnAnalyzeSandbox');
  btn.disabled = true;
  btn.innerHTML = `<i data-lucide="loader" class="w-4 h-4 animate-spin"></i><span>Analyzing...</span>`;
  if (window.lucide) lucide.createIcons();

  const payload = {
    job_id: 'SANDBOX_' + Math.floor(Math.random() * 90000 + 10000),
    title: document.getElementById('sbTitle').value,
    company_profile: document.getElementById('sbCompany').value,
    description: document.getElementById('sbDescription').value,
    requirements: '',
    benefits: '',
    location: 'Remote',
    has_company_logo: document.getElementById('sbHasLogo').checked ? 1 : 0,
    has_questions: document.getElementById('sbHasQuestions').checked ? 1 : 0,
    telecommuting: document.getElementById('sbTelecommuting').checked ? 1 : 0
  };

  try {
    const res = await fetch('/api/predict', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const result = await res.json();
    cachedJobs[result.job_id] = result;
    openXaiModal(result.job_id);
    fetchMetrics();
  } catch (err) {
    alert('Prediction failed: ' + err);
  } finally {
    btn.disabled = false;
    btn.innerHTML = `<i data-lucide="sparkles" class="w-4 h-4"></i><span>Run Glass-Box Forensic Audit</span>`;
    if (window.lucide) lucide.createIcons();
  }
}

function loadSampleScam() {
  document.getElementById('sbTitle').value = "Urgent Data Entry Clerk - Immediate Start Work From Home";
  document.getElementById('sbCompany').value = "";
  document.getElementById('sbDescription').value = "We are seeking motivated individuals to earn $4,500 weekly with simple data entry tasks. No previous experience or formal interview required. All work is done from home. We will wire transfer funds or issue an upfront cashier's check to cover your home office equipment and laptop registration fee. Contact our hiring director immediately on Telegram @recruiter_fastwork or send resume to jobhr2026@gmail.com.";
  document.getElementById('sbHasLogo').checked = false;
  document.getElementById('sbHasQuestions').checked = false;
  document.getElementById('sbTelecommuting').checked = true;
}

function loadSampleReal() {
  document.getElementById('sbTitle').value = "Senior Distributed Systems Engineer (Kafka / Spark)";
  document.getElementById('sbCompany').value = "Enterprise Data Technologies Inc. - Founded in 2012, we build mission-critical stream processing and distributed ML infrastructure for Fortune 500 financial institutions.";
  document.getElementById('sbDescription').value = "We are looking for a Senior Distributed Systems Engineer to design scalable real-time streaming architectures using Apache Kafka, Apache Spark, and Kubernetes. The role requires at least 5 years of experience in Java or Python, solid background in low-latency pipeline optimization, CI/CD, and robust system monitoring. Competitive salary ($140,000 - $175,000) with 401(k) matching and health coverage. Apply via our official careers portal.";
  document.getElementById('sbHasLogo').checked = true;
  document.getElementById('sbHasQuestions').checked = true;
  document.getElementById('sbTelecommuting').checked = true;
}

async function loadBenchmarks() {
  try {
    const res = await fetch('/api/benchmark');
    const data = await res.json();
    const tbody = document.getElementById('benchmarkTableBody');
    tbody.innerHTML = '';

    for (const [name, m] of Object.entries(data)) {
      const tr = document.createElement('tr');
      const isProposed = name.startsWith('Proposed');
      tr.className = isProposed ? "bg-emerald-950/20 font-semibold text-emerald-300" : "text-slate-300";

      const badge = isProposed
        ? `<span class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 text-[10px] uppercase font-bold mr-1.5 border border-emerald-500/30">Proposed</span>`
        : `<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px] uppercase font-medium mr-1.5">Baseline</span>`;

      tr.innerHTML = `
        <td class="py-3 px-4 font-mono text-white">${badge} ${name.replace(/_/g, ' ')}</td>
        <td class="py-3 px-4 font-mono">${(m.Accuracy * 100).toFixed(2)}%</td>
        <td class="py-3 px-4 font-mono text-amber-400">${(m.Precision * 100).toFixed(2)}%</td>
        <td class="py-3 px-4 font-mono text-cyan-400">${(m.Recall * 100).toFixed(2)}%</td>
        <td class="py-3 px-4 font-mono font-bold ${isProposed ? 'text-emerald-400 text-sm' : 'text-slate-300'}">${(m.F1_Score * 100).toFixed(2)}%</td>
        <td class="py-3 px-4 font-mono">${m.ROC_AUC.toFixed(4)}</td>
        <td class="py-3 px-4 font-mono">${m.PR_AUC.toFixed(4)}</td>
        <td class="py-3 px-4 font-mono text-cyan-300">${m.Inference_Latency_ms.toFixed(3)} ms</td>
      `;
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error('Benchmark loading error', e);
  }
}
