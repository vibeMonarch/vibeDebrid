/**
 * duplicates.js — logic for the Duplicates page.
 *
 * Depends on VD.formatBytes and VD.escapeHtml from utils.js,
 * and on csrfFetch / showToast provided by base.html.
 */
(function () {
  'use strict';

  // ─── State ─────────────────────────────────────────────────────────────────

  // groupData[imdb_id] = { group object from API, resolved: bool }
  const groupData = {};

  // ─── Utilities ─────────────────────────────────────────────────────────────

  function resBadgeClass(resolution) {
    if (!resolution) return 'badge-res-other';
    const r = resolution.toLowerCase();
    if (r.includes('2160') || r.includes('4k')) return 'badge-res-2160p';
    if (r.includes('1080'))                       return 'badge-res-1080p';
    if (r.includes('720'))                        return 'badge-res-720p';
    if (r.includes('480'))                        return 'badge-res-480p';
    return 'badge-res-other';
  }

  function groupKey(group) {
    // Unique key per media item; use imdb_id + season + episode for shows
    return [
      group.imdb_id || 'unknown',
      group.season  !== null && group.season  !== undefined ? 'S' + String(group.season).padStart(2, '0')  : '',
      group.episode !== null && group.episode !== undefined ? 'E' + String(group.episode).padStart(2, '0') : '',
    ].filter(Boolean).join('-');
  }

  function formatEpisodeLabel(group) {
    if (group.season === null && group.episode === null) return null;
    let label = '';
    if (group.season  !== null && group.season  !== undefined) label += 'S' + String(group.season).padStart(2, '0');
    if (group.episode !== null && group.episode !== undefined) label += 'E' + String(group.episode).padStart(2, '0');
    return label || null;
  }

  // ─── UI state helpers ───────────────────────────────────────────────────────

  function showOnly(id) {
    ['loading-state', 'empty-state', 'error-state'].forEach(function(sid) {
      document.getElementById(sid).classList.add('hidden');
    });
    if (id) document.getElementById(id).classList.remove('hidden');
  }

  function setScanBusy(busy) {
    const btn  = document.getElementById('btn-scan');
    const icon = document.getElementById('scan-icon');
    btn.disabled = busy;
    btn.classList.toggle('opacity-50', busy);
    btn.classList.toggle('cursor-not-allowed', busy);
    if (busy) {
      icon.classList.add('spinner');
      btn.setAttribute('aria-busy', 'true');
    } else {
      icon.classList.remove('spinner');
      btn.removeAttribute('aria-busy');
    }
  }

  // ─── Render ─────────────────────────────────────────────────────────────────

  function renderGroups(duplicates) {
    const container = document.getElementById('groups-container');
    container.innerHTML = '';

    duplicates.forEach(function(group, groupIndex) {
      const key  = groupKey(group);
      const card = buildGroupCard(group, key, groupIndex);
      container.appendChild(card);
    });
  }

  function buildGroupCard(group, key, groupIndex) {
    const epLabel  = formatEpisodeLabel(group);
    const torrents = group.torrents || [];

    // Sort: keep largest first as the default "keep" selection
    const sorted = torrents.slice().sort(function(a, b) {
      return (b.filesize || 0) - (a.filesize || 0);
    });

    const card = document.createElement('div');
    card.id    = 'group-' + key;
    card.className = 'bg-vd-card border border-vd-border rounded-xl overflow-hidden';
    card.setAttribute('data-group-key', key);

    // ── Card header ──────────────────────────────────────────────────────────
    const header = document.createElement('div');
    header.className = 'flex flex-col sm:flex-row sm:items-center justify-between gap-2 px-5 py-4 border-b border-vd-border';

    const headerLeft = document.createElement('div');
    headerLeft.className = 'flex items-center gap-3 min-w-0';

    // Duplicate icon
    const dupIcon = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    dupIcon.setAttribute('class', 'w-4 h-4 text-vd-muted flex-shrink-0');
    dupIcon.setAttribute('fill', 'none');
    dupIcon.setAttribute('stroke', 'currentColor');
    dupIcon.setAttribute('viewBox', '0 0 24 24');
    dupIcon.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/>';

    const titleEl = document.createElement('h3');
    titleEl.className = 'text-white font-semibold text-sm truncate';
    titleEl.textContent = group.title || group.imdb_id || 'Unknown title';

    headerLeft.appendChild(dupIcon);
    headerLeft.appendChild(titleEl);

    // Pills: episode label + IMDB ID
    const pillRow = document.createElement('div');
    pillRow.className = 'flex items-center gap-2 flex-wrap mt-1 sm:mt-0';

    if (epLabel) {
      const epPill = document.createElement('span');
      epPill.className = 'inline-flex items-center px-2 py-0.5 text-xs rounded-full bg-white/10 text-gray-300 font-mono';
      epPill.textContent = epLabel;
      pillRow.appendChild(epPill);
    }

    if (group.imdb_id) {
      const imdbPill = document.createElement('a');
      imdbPill.className = 'inline-flex items-center px-2 py-0.5 text-xs rounded-full bg-yellow-900/40 text-yellow-400 hover:bg-yellow-800/60 transition-colors font-mono';
      imdbPill.href      = 'https://www.imdb.com/title/' + group.imdb_id + '/';
      imdbPill.target    = '_blank';
      imdbPill.rel       = 'noopener noreferrer';
      imdbPill.textContent = group.imdb_id;
      pillRow.appendChild(imdbPill);
    }

    const countPill = document.createElement('span');
    countPill.className = 'inline-flex items-center px-2 py-0.5 text-xs rounded-full bg-vd-danger/20 text-red-400';
    countPill.textContent = torrents.length + ' copies';
    pillRow.appendChild(countPill);

    // Resolve button (per-group)
    const resolveBtn = document.createElement('button');
    resolveBtn.className = 'flex-shrink-0 flex items-center gap-1.5 px-3 py-1.5 text-xs bg-vd-danger/20 hover:bg-vd-danger/40 text-red-400 hover:text-red-300 border border-red-900/40 hover:border-red-700/60 rounded-lg transition-colors font-medium';
    resolveBtn.setAttribute('data-group-key', key);
    resolveBtn.setAttribute('aria-label', 'Resolve duplicates for ' + (group.title || group.imdb_id));
    resolveBtn.innerHTML = '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg> Resolve';
    resolveBtn.addEventListener('click', function() {
      promptResolve(key, group.title || group.imdb_id);
    });

    const headerRight = document.createElement('div');
    headerRight.className = 'flex items-center gap-2';
    headerRight.appendChild(pillRow);
    headerRight.appendChild(resolveBtn);

    header.appendChild(headerLeft);
    header.appendChild(headerRight);
    card.appendChild(header);

    // ── Torrent table ─────────────────────────────────────────────────────────
    const tableWrapper = document.createElement('div');
    tableWrapper.className = 'overflow-x-auto';

    const table = document.createElement('table');
    table.className = 'w-full text-sm';

    // Table head
    const thead = document.createElement('thead');
    thead.innerHTML = [
      '<tr class="border-b border-vd-border text-xs text-vd-muted uppercase tracking-wider">',
      '  <th class="px-5 py-3 text-left w-8" aria-label="Keep selection"></th>',
      '  <th class="px-2 py-3 text-left">Filename</th>',
      '  <th class="px-2 py-3 text-left w-24">Resolution</th>',
      '  <th class="px-2 py-3 text-right w-28">Size</th>',
      '  <th class="px-2 py-3 text-left w-24 hidden sm:table-cell">Status</th>',
      '</tr>'
    ].join('');
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    tbody.id = 'tbody-' + key;

    sorted.forEach(function(torrent, idx) {
      const row = buildTorrentRow(torrent, key, idx, groupIndex);
      tbody.appendChild(row);
    });

    table.appendChild(tbody);
    tableWrapper.appendChild(table);
    card.appendChild(tableWrapper);

    // Store group data for later use by resolve functions
    groupData[key] = { group: group, sorted: sorted };

    return card;
  }

  function buildTorrentRow(torrent, groupKey, idx, groupIndex) {
    const isDefault = idx === 0; // largest torrent is kept by default
    const radioId   = 'radio-' + groupKey + '-' + torrent.rd_id;

    const tr = document.createElement('tr');
    tr.className = 'border-b border-vd-border/50 last:border-0 hover:bg-white/5 transition-colors';
    tr.setAttribute('data-rd-id', torrent.rd_id);

    // Radio cell
    const tdRadio = document.createElement('td');
    tdRadio.className = 'px-5 py-3';

    const radio = document.createElement('input');
    radio.type      = 'radio';
    radio.name      = 'keep-' + groupKey;
    radio.value     = torrent.rd_id;
    radio.id        = radioId;
    radio.checked   = isDefault;
    radio.className = 'keeper-radio';
    radio.setAttribute('aria-label', 'Keep ' + (torrent.filename || torrent.rd_id));

    // Highlight row when this radio is selected
    radio.addEventListener('change', function() {
      const tbody = document.getElementById('tbody-' + groupKey);
      if (!tbody) return;
      tbody.querySelectorAll('tr').forEach(function(r) {
        r.classList.toggle(
          'bg-indigo-900/10',
          r.getAttribute('data-rd-id') === radio.value
        );
      });
    });

    tdRadio.appendChild(radio);

    // Trigger initial highlight
    if (isDefault) {
      tr.classList.add('bg-indigo-900/10');
    }

    // Filename cell
    const tdFile = document.createElement('td');
    tdFile.className = 'px-2 py-3';

    const filenameWrapper = document.createElement('label');
    filenameWrapper.htmlFor = radioId;
    filenameWrapper.className = 'cursor-pointer';

    const filename = document.createElement('span');
    filename.className = 'block text-gray-200 text-xs font-mono break-all leading-relaxed';
    filename.textContent = torrent.filename || '\u2014';

    const hashEl = document.createElement('span');
    hashEl.className = 'block text-vd-muted text-xs mt-0.5 font-mono truncate';
    hashEl.textContent = torrent.info_hash ? torrent.info_hash.substring(0, 16) + '...' : '';
    hashEl.title = torrent.info_hash || '';

    filenameWrapper.appendChild(filename);
    if (torrent.info_hash) filenameWrapper.appendChild(hashEl);
    tdFile.appendChild(filenameWrapper);

    // Resolution badge cell
    const tdRes = document.createElement('td');
    tdRes.className = 'px-2 py-3';

    const resBadge = document.createElement('span');
    resBadge.className = 'inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold ' + resBadgeClass(torrent.resolution);
    resBadge.textContent = torrent.resolution || '\u2014';
    tdRes.appendChild(resBadge);

    // Size cell
    const tdSize = document.createElement('td');
    tdSize.className = 'px-2 py-3 text-right text-gray-300 font-mono text-xs whitespace-nowrap';
    tdSize.textContent = VD.formatBytes(torrent.filesize);

    // Status cell
    const tdStatus = document.createElement('td');
    tdStatus.className = 'px-2 py-3 hidden sm:table-cell';

    const statusEl = document.createElement('span');
    const statusLower = (torrent.status || '').toLowerCase();
    const statusColors = {
      active:      'text-green-400',
      downloaded:  'text-green-400',
      waiting:     'text-yellow-400',
      queued:      'text-yellow-400',
      error:       'text-red-400',
      dead:        'text-gray-500',
    };
    statusEl.className = 'text-xs font-medium ' + (statusColors[statusLower] || 'text-gray-400');
    statusEl.textContent = torrent.status || '\u2014';
    tdStatus.appendChild(statusEl);

    tr.appendChild(tdRadio);
    tr.appendChild(tdFile);
    tr.appendChild(tdRes);
    tr.appendChild(tdSize);
    tr.appendChild(tdStatus);

    return tr;
  }

  // ─── Scan ───────────────────────────────────────────────────────────────────

  window.scanDuplicates = function() {
    // Clear previous results
    document.getElementById('groups-container').innerHTML = '';
    document.getElementById('summary-bar').classList.add('hidden');
    document.getElementById('btn-resolve-all').classList.add('hidden');
    document.getElementById('btn-resolve-all').classList.remove('flex');
    document.getElementById('page-subtitle').textContent = 'Scanning your Real-Debrid account...';

    Object.keys(groupData).forEach(function(k) { delete groupData[k]; });

    setScanBusy(true);
    showOnly('loading-state');

    fetch('/api/duplicates')
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status + ': ' + res.statusText);
        return res.json();
      })
      .then(function(data) {
        setScanBusy(false);
        showOnly(null);

        const duplicates = data.duplicates || [];

        if (duplicates.length === 0) {
          showOnly('empty-state');
          document.getElementById('page-subtitle').textContent = 'No duplicates found in your Real-Debrid account.';
          return;
        }

        // Compute summary stats
        let totalExtras  = 0;
        let wastedBytes  = 0;
        duplicates.forEach(function(group) {
          const sorted = (group.torrents || []).slice().sort(function(a, b) {
            return (b.filesize || 0) - (a.filesize || 0);
          });
          totalExtras += sorted.length - 1;
          sorted.slice(1).forEach(function(t) { wastedBytes += t.filesize || 0; });
        });

        document.getElementById('summary-groups').textContent = data.total_groups ?? duplicates.length;
        document.getElementById('summary-extras').textContent = totalExtras;
        document.getElementById('summary-wasted').textContent = VD.formatBytes(wastedBytes);
        document.getElementById('summary-bar').classList.remove('hidden');

        const resolveAllBtn = document.getElementById('btn-resolve-all');
        resolveAllBtn.classList.remove('hidden');
        resolveAllBtn.classList.add('flex');

        document.getElementById('page-subtitle').textContent =
          (data.total_groups ?? duplicates.length) + ' duplicate group' +
          ((data.total_groups ?? duplicates.length) !== 1 ? 's' : '') + ' found';

        renderGroups(duplicates);
      })
      .catch(function(err) {
        setScanBusy(false);
        showOnly('error-state');
        document.getElementById('error-message').textContent = err.message || 'An unexpected error occurred.';
        document.getElementById('page-subtitle').textContent = 'Scan failed';
        console.error('Duplicate scan failed:', err);
      });
  };

  // ─── Modal ──────────────────────────────────────────────────────────────────

  let pendingResolveCallback = null;

  function openModal(title, body, onConfirm) {
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-body').textContent  = body;
    document.getElementById('modal-confirm').onclick   = function() {
      closeModal();
      onConfirm();
    };
    document.getElementById('confirm-modal').classList.remove('hidden');
    document.getElementById('modal-confirm').focus();
  }

  window.closeModal = function() {
    document.getElementById('confirm-modal').classList.add('hidden');
    pendingResolveCallback = null;
  };

  // Close modal on backdrop click
  document.getElementById('confirm-modal').addEventListener('click', function(e) {
    if (e.target === this) window.closeModal();
  });

  // Close modal on Escape key
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') window.closeModal();
  });

  // ─── Resolve ────────────────────────────────────────────────────────────────

  function getSelectedKeepId(key) {
    const radio = document.querySelector('input[name="keep-' + key + '"]:checked');
    return radio ? radio.value : null;
  }

  function promptResolve(key, label) {
    const data    = groupData[key];
    if (!data) return;

    const keepId  = getSelectedKeepId(key);
    if (!keepId) {
      showToast('No torrent selected to keep.', 'warning');
      return;
    }

    const extras  = data.sorted.filter(function(t) { return t.rd_id !== keepId; });
    const count   = extras.length;
    const keepTorrent = data.sorted.find(function(t) { return t.rd_id === keepId; });

    openModal(
      'Resolve duplicates for "' + label + '"',
      'Keep: ' + (keepTorrent ? keepTorrent.filename : keepId) + '. ' +
      count + ' torrent' + (count !== 1 ? 's' : '') + ' will be removed from Real-Debrid. This cannot be undone.',
      function() { executeResolve(key, keepId); }
    );
  }

  function executeResolve(key, keepId) {
    const data   = groupData[key];
    if (!data) return;

    const removeIds = data.sorted
      .filter(function(t) { return t.rd_id !== keepId; })
      .map(function(t) { return t.rd_id; });

    if (removeIds.length === 0) {
      showToast('Nothing to remove.', 'info');
      return;
    }

    const card = document.getElementById('group-' + key);
    if (card) card.classList.add('group-resolving');

    csrfFetch('/api/duplicates/resolve', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ keep_rd_id: keepId, remove_rd_ids: removeIds }),
    })
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function(result) {
        const removed = result.removed_count || 0;
        const errors  = result.errors || [];

        if (errors.length > 0) {
          console.warn('Resolve errors:', errors);
          showToast('Resolved with ' + errors.length + ' error(s). Check console for details.', 'warning');
        } else {
          showToast('Removed ' + removed + ' duplicate' + (removed !== 1 ? 's' : '') + '.', 'success');
        }

        // Remove the card after animation completes
        setTimeout(function() {
          if (card) card.remove();
          delete groupData[key];
          updateSummaryAfterResolve();
        }, 550);
      })
      .catch(function(err) {
        if (card) card.classList.remove('group-resolving');
        showToast('Failed to resolve: ' + err.message, 'error');
        console.error('Resolve failed for group', key, err);
      });
  }

  function updateSummaryAfterResolve() {
    const remaining = Object.keys(groupData).length;

    if (remaining === 0) {
      document.getElementById('summary-bar').classList.add('hidden');
      document.getElementById('btn-resolve-all').classList.add('hidden');
      document.getElementById('btn-resolve-all').classList.remove('flex');
      document.getElementById('page-subtitle').textContent = 'All duplicates resolved.';

      // Show empty state
      const container = document.getElementById('groups-container');
      if (container.children.length === 0) {
        showOnly('empty-state');
        document.getElementById('empty-state').querySelector('p.text-white').textContent = 'All duplicates resolved';
        document.getElementById('empty-state').querySelector('p.text-sm').textContent =
          'No remaining duplicates in your Real-Debrid account.';
      }
      return;
    }

    // Recalculate wasted space and extras from remaining groups
    let totalExtras = 0;
    let wastedBytes = 0;
    Object.values(groupData).forEach(function(d) {
      const extras = d.sorted.slice(1);
      totalExtras += extras.length;
      extras.forEach(function(t) { wastedBytes += t.filesize || 0; });
    });

    document.getElementById('summary-groups').textContent = remaining;
    document.getElementById('summary-extras').textContent = totalExtras;
    document.getElementById('summary-wasted').textContent = VD.formatBytes(wastedBytes);
    document.getElementById('page-subtitle').textContent =
      remaining + ' duplicate group' + (remaining !== 1 ? 's' : '') + ' remaining';
  }

  // ─── Resolve All ────────────────────────────────────────────────────────────

  window.resolveAll = function() {
    const keys = Object.keys(groupData);
    if (keys.length === 0) {
      showToast('No groups to resolve.', 'info');
      return;
    }

    // Build a summary for the modal
    let totalRemove = 0;
    keys.forEach(function(key) {
      const d      = groupData[key];
      const keepId = getSelectedKeepId(key) || (d.sorted[0] && d.sorted[0].rd_id);
      totalRemove  += d.sorted.filter(function(t) { return t.rd_id !== keepId; }).length;
    });

    openModal(
      'Resolve All Duplicate Groups',
      'This will remove ' + totalRemove + ' torrent' + (totalRemove !== 1 ? 's' : '') +
      ' across ' + keys.length + ' group' + (keys.length !== 1 ? 's' : '') +
      ', keeping the selected version in each group. This cannot be undone.',
      function() {
        // Execute each resolve sequentially to avoid hammering the API
        let chain = Promise.resolve();
        keys.forEach(function(key) {
          chain = chain.then(function() {
            return resolveGroupPromise(key);
          });
        });
        chain.then(function() {
          showToast('All groups resolved.', 'success');
        }).catch(function(err) {
          console.error('Resolve All error:', err);
        });
      }
    );
  };

  function resolveGroupPromise(key) {
    const data   = groupData[key];
    if (!data) return Promise.resolve();

    const keepId = getSelectedKeepId(key) || (data.sorted[0] && data.sorted[0].rd_id);
    if (!keepId) return Promise.resolve();

    const removeIds = data.sorted
      .filter(function(t) { return t.rd_id !== keepId; })
      .map(function(t) { return t.rd_id; });

    if (removeIds.length === 0) return Promise.resolve();

    const card = document.getElementById('group-' + key);
    if (card) card.classList.add('group-resolving');

    return csrfFetch('/api/duplicates/resolve', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ keep_rd_id: keepId, remove_rd_ids: removeIds }),
    })
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function() {
        setTimeout(function() {
          if (card) card.remove();
          delete groupData[key];
          updateSummaryAfterResolve();
        }, 550);
      })
      .catch(function(err) {
        if (card) card.classList.remove('group-resolving');
        console.error('Failed to resolve group', key, err);
        showToast('Failed to resolve a group: ' + err.message, 'error');
      });
  }
})();
