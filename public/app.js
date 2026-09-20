// EventBus-Broker v2.0.0 Interactive Cluster Controller

let topicsCache = [];
let eventSource = null;

document.addEventListener('DOMContentLoaded', () => {
  setupSSE();
  fetchTopics();
  setupEventListeners();
});

// Setup Server-Sent Events (SSE)
function setupSSE() {
  const badge = document.getElementById('sseBadge');
  if (eventSource) eventSource.close();

  eventSource = new EventSource('/api/events/stream');

  eventSource.onopen = () => {
    badge.textContent = 'SSE: CONNECTED';
    badge.className = 'badge badge-active';
  };

  eventSource.onerror = () => {
    badge.textContent = 'SSE: DISCONNECTED';
    badge.className = 'badge badge-danger';
  };

  eventSource.addEventListener('message_published', () => {
    fetchTopics();
    inspectLag();
  });

  eventSource.addEventListener('topic_created', () => {
    fetchTopics();
  });
}

// Fetch Topics
async function fetchTopics() {
  try {
    const res = await fetch('/api/topics');
    const data = await res.json();
    if (data.success && data.topics) {
      topicsCache = data.topics;
      renderTopicDropdowns(topicsCache);
      updateTopMetrics(topicsCache);
    }
  } catch (err) {
    console.error('Failed to fetch topics:', err);
  }
}

// Render Dropdowns
function renderTopicDropdowns(topics) {
  const pubSelect = document.getElementById('selectTopic');
  const browseSelect = document.getElementById('browseTopicSelect');

  const curPubVal = pubSelect.value;
  const curBrowseVal = browseSelect.value;

  const options = topics.map(t => `<option value="${escapeHtml(t.name)}">${escapeHtml(t.name)} (${t.partitionCount}P)</option>`).join('');

  pubSelect.innerHTML = options;
  browseSelect.innerHTML = options;

  if (curPubVal && topics.some(t => t.name === curPubVal)) pubSelect.value = curPubVal;
  if (curBrowseVal && topics.some(t => t.name === curBrowseVal)) browseSelect.value = curBrowseVal;

  updatePartitionDropdown();
}

function updatePartitionDropdown() {
  const browseTopic = document.getElementById('browseTopicSelect').value;
  const t = topicsCache.find(x => x.name === browseTopic);
  const partSelect = document.getElementById('browsePartitionSelect');

  if (t) {
    let opts = '';
    for (let i = 0; i < t.partitionCount; i++) {
      opts += `<option value="${i}">Partition ${i}</option>`;
    }
    partSelect.innerHTML = opts;
  }
}

// Update Top Metrics
function updateTopMetrics(topics) {
  const totalTopics = topics.length;
  let totalPartitions = 0;
  let totalMessages = 0;

  for (const t of topics) {
    totalPartitions += t.partitionCount;
    totalMessages += t.totalMessages;
  }

  document.getElementById('valTopicsCount').textContent = totalTopics;
  document.getElementById('valPartitionsCount').textContent = totalPartitions;
  document.getElementById('valMessagesCount').textContent = totalMessages;
  document.getElementById('valGroupsCount').textContent = '1+';
}

// Inspect Consumer Group Lag
async function inspectLag() {
  const groupId = document.getElementById('inputGroupId').value.trim();
  const topic = document.getElementById('selectTopic').value;
  const container = document.getElementById('lagContainer');

  if (!groupId || !topic) return;

  try {
    const res = await fetch(`/api/groups/lag?groupId=${encodeURIComponent(groupId)}&topic=${encodeURIComponent(topic)}`);
    const data = await res.json();

    if (data.success && data.lagInfo) {
      const info = data.lagInfo;
      let html = `<div style="font-weight:700; font-size:0.85rem;">Group: <code>${escapeHtml(info.groupId)}</code> | Total Lag: <span class="badge ${info.totalLag > 0 ? 'badge-danger' : 'badge-active'}">${info.totalLag} msgs</span></div>`;

      html += info.partitions.map(p => {
        const pct = p.highWatermark > 0 ? Math.min(100, Math.floor((p.lag / p.highWatermark) * 100)) : 0;
        return `
          <div class="lag-card">
            <div class="lag-card-head">
              <span>Partition ${p.partitionId}</span>
              <span>Lag: ${p.lag} (HWM: ${p.highWatermark} | Commit: ${p.committedOffset})</span>
            </div>
            <div class="lag-bar-wrapper">
              <div class="lag-bar-fill" style="width: ${pct}%"></div>
            </div>
          </div>
        `;
      }).join('');

      container.innerHTML = html;
    }
  } catch (err) {
    container.innerHTML = `<span class="danger">Lag inspection failed: ${escapeHtml(err.message)}</span>`;
  }
}

// Fetch Messages for Log Browser
async function fetchMessages() {
  const topic = document.getElementById('browseTopicSelect').value;
  const partition = document.getElementById('browsePartitionSelect').value;
  const tbody = document.getElementById('messagesTableBody');

  tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Loading partition messages...</td></tr>';

  try {
    const res = await fetch(`/api/fetch?topic=${encodeURIComponent(topic)}&partition=${partition}&offset=0&limit=50`);
    const data = await res.json();

    if (data.success && data.messages) {
      if (data.messages.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Partition log is empty.</td></tr>';
        return;
      }

      tbody.innerHTML = data.messages.map(m => {
        const valStr = typeof m.value === 'object' ? JSON.stringify(m.value) : String(m.value);
        const headersStr = JSON.stringify(m.headers || {});

        return `
          <tr>
            <td><strong><code>#${m.offset}</code></strong></td>
            <td><code>${escapeHtml(m.key || 'null')}</code></td>
            <td><small class="text-muted">${new Date(m.timestamp).toLocaleTimeString()}</small></td>
            <td><code>${escapeHtml(valStr)}</code></td>
            <td><small class="text-muted">${escapeHtml(headersStr)}</small></td>
          </tr>
        `;
      }).join('');
    }
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="5" class="danger">Error: ${escapeHtml(err.message)}</td></tr>`;
  }
}

// Event Listeners
function setupEventListeners() {
  // Publish Form
  document.getElementById('publishForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const topic = document.getElementById('selectTopic').value;
    const key = document.getElementById('inputKey').value.trim();
    const payloadRaw = document.getElementById('inputPayload').value.trim();
    const resultBox = document.getElementById('publishResult');

    let value = payloadRaw;
    try { value = JSON.parse(payloadRaw); } catch (err) {}

    try {
      const res = await fetch('/api/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic, key, value, headers: { client: 'web-console' } })
      });
      const data = await res.json();

      if (data.success && data.result) {
        const r = data.result;
        resultBox.innerHTML = `
          <div><strong>✓ Published to <code>${escapeHtml(r.topic)}</code></strong></div>
          <div style="margin-top:4px;">Partition: <strong>${r.partitionId}</strong> | Offset: <strong>#${r.offset}</strong></div>
          <div style="font-size:0.75rem; color:var(--text-muted); margin-top:2px;">Timestamp: ${new Date(r.timestamp).toISOString()}</div>
        `;
        fetchTopics();
        inspectLag();
      } else {
        resultBox.innerHTML = `<span class="danger">Error: ${escapeHtml(data.error)}</span>`;
      }
    } catch (err) {
      resultBox.innerHTML = `<span class="danger">Network Error: ${escapeHtml(err.message)}</span>`;
    }
  });

  // Inspect Lag Button
  document.getElementById('btnInspectGroup').addEventListener('click', inspectLag);
  document.getElementById('btnRefreshLag').addEventListener('click', inspectLag);

  // Browse Topic Change
  document.getElementById('browseTopicSelect').addEventListener('change', () => {
    updatePartitionDropdown();
    fetchMessages();
  });

  document.getElementById('browsePartitionSelect').addEventListener('change', fetchMessages);
  document.getElementById('btnFetchMessages').addEventListener('click', fetchMessages);

  // New Topic Modal
  const modal = document.getElementById('topicModal');
  document.getElementById('btnNewTopicModal').addEventListener('click', () => modal.classList.remove('hidden'));
  document.getElementById('btnCloseTopicModal').addEventListener('click', () => modal.classList.add('hidden'));

  document.getElementById('createTopicForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = document.getElementById('inputNewTopicName').value.trim();
    const partitions = parseInt(document.getElementById('inputNewTopicPartitions').value, 10);

    try {
      const res = await fetch('/api/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, partitions })
      });
      const data = await res.json();
      if (data.success) {
        modal.classList.add('hidden');
        document.getElementById('createTopicForm').reset();
        fetchTopics();
      } else {
        alert('Failed to create topic: ' + data.error);
      }
    } catch (err) {
      alert('Error creating topic: ' + err.message);
    }
  });
}

function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
