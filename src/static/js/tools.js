(function() {

  // ---------------------------------------------------------------------------
  // Stored preview data (needed to build the confirmation message)
  // ---------------------------------------------------------------------------
  var _preview = null;

  // ---------------------------------------------------------------------------
  // Section toggle (card collapse)
  // ---------------------------------------------------------------------------
  window.toggleSection = function(id) {
    var body = document.getElementById('body-' + id);
    var chevron = document.getElementById('chevron-' + id);
    if (!body) return;
    body.classList.toggle('collapsed');
    if (chevron) chevron.classList.toggle('rotated');
  };

  window.toggleTableSection = function(id) {
    var body = document.getElementById('body-' + id);
    var chevron = document.getElementById('chevron-' + id);
    if (!body) return;
    body.classList.toggle('collapsed');
    if (chevron) chevron.classList.toggle('rotated');
  };

  // ---------------------------------------------------------------------------
  // HTML escaping
  // ---------------------------------------------------------------------------
  function esc(s) { return VD.escapeHtml(s); }

  // ---------------------------------------------------------------------------
  // Summary stat card helper
  // ---------------------------------------------------------------------------
  function statCard(label, value, colorClass) {
    return '<div class="bg-vd-card border border-vd-border rounded-lg px-4 py-3">' +
      '<p class="text-xs text-vd-muted uppercase tracking-wider mb-1">' + esc(label) + '</p>' +
      '<p class="text-2xl font-bold ' + colorClass + '">' + esc(String(value)) + '</p>' +
      '</div>';
  }

  // ---------------------------------------------------------------------------
  // Scan & Preview
  // ---------------------------------------------------------------------------
  window.runPreview = function() {
    var moviesPath = document.getElementById('movies-path').value.trim();
    var showsPath = document.getElementById('shows-path').value.trim();

    if (!moviesPath || !showsPath) {
      showToast('Please enter both paths before scanning.', 'warning');
      return;
    }

    setScanLoading(true);
    document.getElementById('preview-section').classList.add('hidden');
    document.getElementById('result-section').classList.add('hidden');
    _preview = null;

    csrfFetch('/api/tools/migration/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ movies_path: moviesPath, shows_path: showsPath })
    })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      _preview = data;
      renderPreview(data);
      setScanLoading(false);
    })
    .catch(function(err) {
      setScanLoading(false);
      showToast('Scan failed: ' + err.message, 'error');
    });
  };

  function setScanLoading(loading) {
    var btn = document.getElementById('scan-btn');
    var icon = document.getElementById('scan-icon');
    var spinner = document.getElementById('scan-spinner');
    var label = document.getElementById('scan-btn-label');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Scanning library...' : 'Scan & Preview';
  }

  // ---------------------------------------------------------------------------
  // Render preview data
  // ---------------------------------------------------------------------------
  function renderPreview(data) {
    var s = data.summary || {};

    // Summary cards
    var totalFound = (s.found_movies || 0) + (s.found_episodes || 0);
    document.getElementById('preview-summary').innerHTML =
      statCard('Found', totalFound, 'text-white') +
      statCard('To Import', s.to_import_count || 0, 'text-vd-accent') +
      statCard('Duplicates', s.duplicate_count || 0, 'text-yellow-400') +
      statCard('To Move', s.to_move_count || 0, 'text-blue-400');

    // Found items table (cap at 50 rows, show "X more" note)
    renderFoundItems(data.found_items || []);

    // Duplicates
    renderDuplicates(data.duplicates || []);

    // Items to move
    renderToMove(data.to_move || []);

    // Errors
    renderErrors(data.errors || []);

    document.getElementById('preview-section').classList.remove('hidden');
  }

  var TABLE_CAP = 50;

  function renderFoundItems(items) {
    var tbody = document.getElementById('found-tbody');
    var moreEl = document.getElementById('found-more');
    var badge = document.getElementById('found-count-badge');
    var displayed = items.slice(0, TABLE_CAP);
    var overflow = items.length - displayed.length;

    badge.textContent = items.length;

    var rows = displayed.map(function(fi, idx) {
      var seInfo = fi.season != null ? 'S' + pad2(fi.season) + (fi.episode != null ? 'E' + pad2(fi.episode) : '') : '';
      var symlinkBadge = fi.is_symlink
        ? '<span class="text-xs text-vd-accent">symlink</span>'
        : '<span class="text-xs text-vd-muted">file</span>';
      var imdbLink = fi.imdb_id
        ? '<a href="https://www.imdb.com/title/' + esc(fi.imdb_id) + '" target="_blank" rel="noopener" class="text-vd-accent hover:underline font-mono text-xs">' + esc(fi.imdb_id) + '</a>'
        : '<span class="text-vd-muted">—</span>';
      var rowClass = idx % 2 === 1 ? 'bg-vd-bg/30' : '';
      return '<tr class="' + rowClass + ' border-t border-vd-border/50">' +
        '<td class="px-4 py-1.5 text-gray-200">' + esc(fi.title) + '</td>' +
        '<td class="px-4 py-1.5 text-vd-muted">' + (fi.year || '—') + '</td>' +
        '<td class="px-4 py-1.5"><span class="text-xs uppercase tracking-wide ' + (fi.media_type === 'movie' ? 'text-purple-400' : 'text-teal-400') + '">' + esc(fi.media_type) + '</span></td>' +
        '<td class="px-4 py-1.5 font-mono text-xs text-vd-muted">' + (seInfo || '—') + '</td>' +
        '<td class="px-4 py-1.5">' + imdbLink + '</td>' +
        '<td class="px-4 py-1.5">' + symlinkBadge + '</td>' +
        '<td class="px-4 py-1.5 text-xs text-vd-muted">' + (fi.resolution || '—') + '</td>' +
        '</tr>';
    });

    tbody.innerHTML = rows.join('');

    if (overflow > 0) {
      moreEl.textContent = '... and ' + overflow + ' more item' + (overflow === 1 ? '' : 's') + ' (not shown)';
      moreEl.classList.remove('hidden');
    } else {
      moreEl.classList.add('hidden');
    }
  }

  function renderDuplicates(dupes) {
    var section = document.getElementById('duplicates-section');
    var tbody = document.getElementById('duplicates-tbody');
    var badge = document.getElementById('duplicates-count-badge');

    if (!dupes.length) {
      section.classList.add('hidden');
      return;
    }
    section.classList.remove('hidden');
    badge.textContent = dupes.length;

    var rows = dupes.map(function(d, idx) {
      var fi = d.found_item || {};
      var seInfo = fi.season != null ? ' S' + pad2(fi.season) + (fi.episode != null ? 'E' + pad2(fi.episode) : '') : '';
      var foundLabel = esc(fi.title || '') + (fi.year ? ' (' + fi.year + ')' : '') + seInfo;
      var existingLabel = esc(d.existing_title || '') + ' <span class="text-vd-muted text-xs">#' + esc(String(d.existing_id)) + '</span>';
      var reasonMap = {
        'same_source_path': 'Same source file',
        'title_year_match': 'Title + year match'
      };
      var reason = reasonMap[d.match_reason] || esc(d.match_reason);
      var rowClass = idx % 2 === 1 ? 'bg-vd-bg/30' : '';
      return '<tr class="' + rowClass + ' border-t border-vd-border/50">' +
        '<td class="px-4 py-1.5 text-gray-200">' + foundLabel + '</td>' +
        '<td class="px-4 py-1.5 text-gray-200">' + existingLabel + '</td>' +
        '<td class="px-4 py-1.5 text-xs text-yellow-400">' + reason + '</td>' +
        '</tr>';
    });
    tbody.innerHTML = rows.join('');
  }

  function renderToMove(items) {
    var section = document.getElementById('tomove-section');
    var tbody = document.getElementById('tomove-tbody');
    var badge = document.getElementById('tomove-count-badge');

    if (!items.length) {
      section.classList.add('hidden');
      return;
    }
    section.classList.remove('hidden');
    badge.textContent = items.length;

    var rows = items.map(function(item, idx) {
      var seInfo = item.season != null ? 'S' + pad2(item.season) + (item.episode != null ? 'E' + pad2(item.episode) : '') : '—';
      var rowClass = idx % 2 === 1 ? 'bg-vd-bg/30' : '';
      return '<tr class="' + rowClass + ' border-t border-vd-border/50">' +
        '<td class="px-4 py-1.5 text-gray-200">' + esc(item.title) + '</td>' +
        '<td class="px-4 py-1.5 text-vd-muted">' + (item.year || '—') + '</td>' +
        '<td class="px-4 py-1.5"><span class="text-xs uppercase tracking-wide ' + (item.media_type === 'movie' ? 'text-purple-400' : 'text-teal-400') + '">' + esc(item.media_type) + '</span></td>' +
        '<td class="px-4 py-1.5 font-mono text-xs text-vd-muted">' + seInfo + '</td>' +
        '<td class="px-4 py-1.5"><span class="badge-' + esc(item.state) + ' text-white text-xs px-2 py-0.5 rounded-full">' + esc(item.state) + '</span></td>' +
        '</tr>';
    });
    tbody.innerHTML = rows.join('');
  }

  function renderErrors(errors) {
    var section = document.getElementById('errors-section');
    var list = document.getElementById('errors-list');
    var badge = document.getElementById('errors-count-badge');

    if (!errors.length) {
      section.classList.add('hidden');
      return;
    }
    section.classList.remove('hidden');
    badge.textContent = errors.length;

    list.innerHTML = errors.map(function(e) {
      return '<li class="text-xs text-red-300 font-mono">' + esc(e) + '</li>';
    }).join('');
  }

  // ---------------------------------------------------------------------------
  // Execute Migration
  // ---------------------------------------------------------------------------
  window.runExecute = function() {
    if (!_preview) return;

    var s = _preview.summary || {};
    var toImport = s.to_import_count || 0;
    var toMove = s.to_move_count || 0;
    var toDedupe = s.duplicate_count || 0;

    var parts = [];
    if (toImport > 0) parts.push('import ' + toImport + ' item' + (toImport === 1 ? '' : 's'));
    if (toMove > 0) parts.push('move ' + toMove + ' item' + (toMove === 1 ? '' : 's'));
    if (toDedupe > 0) parts.push('remove ' + toDedupe + ' duplicate' + (toDedupe === 1 ? '' : 's'));
    parts.push('update config paths');

    var msg = 'This will ' + parts.join(', ') + '. Continue?';
    if (!confirm(msg)) return;

    var moviesPath = document.getElementById('movies-path').value.trim();
    var showsPath = document.getElementById('shows-path').value.trim();

    setExecuteLoading(true);

    csrfFetch('/api/tools/migration/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ movies_path: moviesPath, shows_path: showsPath })
    })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setExecuteLoading(false);
      renderResult(data);
      showToast('Migration complete' + (data.config_updated ? ' — config paths updated!' : ''), 'success');
    })
    .catch(function(err) {
      setExecuteLoading(false);
      showToast('Migration failed: ' + err.message, 'error');
    });
  };

  function setExecuteLoading(loading) {
    var btn = document.getElementById('execute-btn');
    var icon = document.getElementById('execute-icon');
    var spinner = document.getElementById('execute-spinner');
    var label = document.getElementById('execute-btn-label');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Running migration...' : 'Execute Migration';
  }

  function renderResult(data) {
    var statsEl = document.getElementById('result-stats');
    statsEl.innerHTML =
      statCard('Imported', data.imported || 0, 'text-green-400') +
      statCard('Moved', data.moved || 0, 'text-blue-400') +
      statCard('Deduped', data.duplicates_removed || 0, 'text-yellow-400') +
      statCard('Config', data.config_updated ? 'Updated' : 'Unchanged', data.config_updated ? 'text-green-400' : 'text-vd-muted');

    var errorsWrap = document.getElementById('result-errors-wrap');
    var errorsList = document.getElementById('result-errors-list');
    if (data.errors && data.errors.length) {
      errorsList.innerHTML = data.errors.map(function(e) {
        return '<li>' + esc(e) + '</li>';
      }).join('');
      errorsWrap.classList.remove('hidden');
    } else {
      errorsWrap.classList.add('hidden');
    }

    document.getElementById('result-section').classList.remove('hidden');
    document.getElementById('preview-section').classList.add('hidden');
  }

  // ---------------------------------------------------------------------------
  // Utility
  // ---------------------------------------------------------------------------
  function pad2(n) {
    return n < 10 ? '0' + n : String(n);
  }

})();

// ---------------------------------------------------------------------------
// Data Cleanup — Backfill TMDB IDs + Bridge RD Torrents + Smart Cleanup
// ---------------------------------------------------------------------------
(function() {

  var _smartPreview = null;

  // ---------------------------------------------------------------------------
  // Shared helpers
  // ---------------------------------------------------------------------------
  function esc2(s) {
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function pad2(n) {
    return n < 10 ? '0' + n : String(n);
  }

  function statCard2(label, value, colorClass) {
    return '<div class="bg-vd-card border border-vd-border rounded-lg px-4 py-3">' +
      '<p class="text-xs text-vd-muted uppercase tracking-wider mb-1">' + esc2(label) + '</p>' +
      '<p class="text-2xl font-bold ' + colorClass + '">' + esc2(String(value)) + '</p>' +
      '</div>';
  }

  function livenessBadge(liveness) {
    if (liveness === 'live') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-900/50 text-green-300 border border-green-700">LIVE</span>';
    }
    if (liveness === 'bridged') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-900/50 text-yellow-300 border border-yellow-700">BRIDGED</span>';
    }
    return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-900/50 text-red-300 border border-red-700">DEAD</span>';
  }

  // ---------------------------------------------------------------------------
  // Backfill
  // ---------------------------------------------------------------------------
  window.runBackfill = function() {
    setBackfillLoading(true);
    document.getElementById('backfill-result').classList.add('hidden');

    csrfFetch('/api/tools/backfill', { method: 'POST' })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setBackfillLoading(false);
      renderBackfillResult(data);
    })
    .catch(function(err) {
      setBackfillLoading(false);
      showToast('Backfill failed: ' + err.message, 'error');
    });
  };

  function setBackfillLoading(loading) {
    var btn = document.getElementById('backfill-btn');
    var icon = document.getElementById('backfill-icon');
    var spinner = document.getElementById('backfill-spinner');
    var label = document.getElementById('backfill-btn-label');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Running backfill...' : 'Backfill TMDB IDs';
  }

  function renderBackfillResult(data) {
    var wrap = document.getElementById('backfill-result');
    var content = document.getElementById('backfill-result-content');

    var html = '';
    if (data.total === 0) {
      html = '<p class="text-sm text-vd-muted">Nothing to backfill — all items already have TMDB IDs.</p>';
    } else {
      var color = data.resolved > 0 ? 'text-green-400' : 'text-vd-muted';
      html =
        '<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">' +
        statCard2('Total', data.total, 'text-white') +
        statCard2('Resolved', data.resolved, 'text-green-400') +
        statCard2('Failed', data.failed, data.failed > 0 ? 'text-red-400' : 'text-vd-muted') +
        statCard2('Rows Updated', data.updated_rows, color) +
        '</div>';
    }
    content.innerHTML = html;
    wrap.classList.remove('hidden');
  }

  // ---------------------------------------------------------------------------
  // Bridge RD Torrents
  // ---------------------------------------------------------------------------
  window.runBridgeRd = function() {
    setBridgeLoading(true);
    document.getElementById('bridge-result').classList.add('hidden');

    csrfFetch('/api/tools/bridge-rd', { method: 'POST' })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setBridgeLoading(false);
      renderBridgeResult(data);
      if (data.matched > 0) {
        showToast('Bridged ' + data.matched + ' item' + (data.matched === 1 ? '' : 's') + ' to RD torrents', 'success');
      } else {
        showToast('Bridge complete — no new links created', 'info');
      }
    })
    .catch(function(err) {
      setBridgeLoading(false);
      showToast('Bridge failed: ' + err.message, 'error');
    });
  };

  function setBridgeLoading(loading) {
    var btn = document.getElementById('bridge-btn');
    var icon = document.getElementById('bridge-icon');
    var spinner = document.getElementById('bridge-spinner');
    var label = document.getElementById('bridge-btn-label');
    var status = document.getElementById('bridge-status');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Bridging...' : 'Bridge RD Torrents';
    if (status) {
      status.textContent = loading ? 'Scanning RD account...' : '';
      status.classList.toggle('hidden', !loading);
    }
  }

  function renderBridgeResult(data) {
    var wrap = document.getElementById('bridge-result');
    var content = document.getElementById('bridge-result-content');

    var html = '<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">' +
      statCard2('RD Torrents', data.total_rd_torrents, 'text-white') +
      statCard2('Migration Items', data.total_migration_items, 'text-white') +
      statCard2('Matched', data.matched, data.matched > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCard2('Already Bridged', data.already_bridged, 'text-vd-muted') +
      statCard2('Unmatched', data.unmatched_items, data.unmatched_items > 0 ? 'text-yellow-400' : 'text-vd-muted') +
      statCard2('Errors', data.errors ? data.errors.length : 0, (data.errors && data.errors.length > 0) ? 'text-red-400' : 'text-vd-muted') +
      '</div>';

    if (data.errors && data.errors.length > 0) {
      html += '<div class="mt-3">' +
        '<p class="text-xs text-red-400 font-medium mb-1">Errors:</p>' +
        '<ul class="text-xs text-red-300 space-y-0.5 font-mono">' +
        data.errors.map(function(e) { return '<li>' + esc2(e) + '</li>'; }).join('') +
        '</ul></div>';
    }

    content.innerHTML = html;
    wrap.classList.remove('hidden');
  }

  // ---------------------------------------------------------------------------
  // Smart Cleanup — Preview
  // ---------------------------------------------------------------------------
  window.runSmartCleanupPreview = function() {
    setSmartPreviewLoading(true);
    document.getElementById('smart-preview-section').classList.add('hidden');
    document.getElementById('smart-result').classList.add('hidden');
    _smartPreview = null;

    fetch('/api/tools/cleanup/preview')
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setSmartPreviewLoading(false);
      _smartPreview = data;
      renderSmartPreview(data);
    })
    .catch(function(err) {
      setSmartPreviewLoading(false);
      showToast('Cleanup preview failed: ' + err.message, 'error');
    });
  };

  function setSmartPreviewLoading(loading) {
    var btn = document.getElementById('smart-preview-btn');
    var icon = document.getElementById('smart-preview-icon');
    var spinner = document.getElementById('smart-preview-spinner');
    var label = document.getElementById('smart-preview-btn-label');
    var status = document.getElementById('smart-preview-status');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Analysing...' : 'Preview Smart Cleanup';
    if (status) status.classList.toggle('hidden', !loading);
  }

  function renderSmartPreview(data) {
    // Summary stats
    var statsEl = document.getElementById('smart-summary-stats');
    statsEl.innerHTML =
      statCard2('Assessed', data.total_items_assessed, 'text-white') +
      statCard2('Live', data.live_count, data.live_count > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCard2('Bridged', data.bridged_count, data.bridged_count > 0 ? 'text-yellow-400' : 'text-vd-muted') +
      statCard2('Dead', data.dead_count, data.dead_count > 0 ? 'text-red-400' : 'text-vd-muted') +
      statCard2('Groups', data.groups ? data.groups.length : 0, 'text-white') +
      statCard2('To Remove', data.total_to_remove, data.total_to_remove > 0 ? 'text-red-400' : 'text-vd-muted');

    // Warnings
    var warningsSection = document.getElementById('smart-warnings-section');
    var warningsList = document.getElementById('smart-warnings-list');
    if (data.warnings && data.warnings.length > 0) {
      warningsList.innerHTML = data.warnings.map(function(w) {
        return '<li>' + esc2(w) + '</li>';
      }).join('');
      warningsSection.classList.remove('hidden');
    } else {
      warningsSection.classList.add('hidden');
    }

    // Bridge reminder (shown when no bridged items and some dead items with groups)
    var bridgeReminder = document.getElementById('smart-bridge-reminder');
    var hasBridged = data.bridged_count > 0 || data.live_count > 0;
    var hasDeadGroups = data.total_to_remove > 0 && data.bridged_count === 0;
    bridgeReminder.classList.toggle('hidden', hasBridged || !hasDeadGroups);

    // RD deletion summary
    var rdSummary = document.getElementById('smart-rd-summary');
    var rdText = document.getElementById('smart-rd-summary-text');
    var rdToDelete = (data.rd_ids_to_delete || []).length;
    var rdProtected = (data.rd_ids_protected || []).length;
    var symlinksDisk = data.symlinks_to_delete || 0;
    if (rdToDelete > 0 || symlinksDisk > 0) {
      var parts = [];
      if (rdToDelete > 0) {
        parts.push(rdToDelete + ' RD torrent' + (rdToDelete === 1 ? '' : 's') + ' will be deleted from your account');
      }
      if (rdProtected > 0) {
        parts.push(rdProtected + ' protected (shared with kept items)');
      }
      if (symlinksDisk > 0) {
        parts.push(symlinksDisk + ' symlink' + (symlinksDisk === 1 ? '' : 's') + ' will be removed from disk');
      }
      rdText.textContent = parts.join('. ') + '.';
      rdSummary.classList.remove('hidden');
    } else {
      rdSummary.classList.add('hidden');
    }

    // Groups accordion
    var container = document.getElementById('smart-groups-container');
    var groups = data.groups || [];
    if (groups.length === 0) {
      container.innerHTML = '<p class="text-sm text-vd-muted py-2">No duplicate groups found among migration items.</p>';
    } else {
      container.innerHTML = groups.map(function(group, gi) {
        var seInfo = group.season != null
          ? ' S' + pad2(group.season) + (group.episode != null ? 'E' + pad2(group.episode) : '')
          : '';
        var title = esc2(group.title) + seInfo;
        var groupId = 'smart-group-' + gi;

        // Rows for all items (keeper first, then removes)
        var allItems = [group.keep].concat(group.remove || []);
        var rows = allItems.map(function(item, idx) {
          var action = item.item_id === group.keep.item_id
            ? '<span class="text-xs font-semibold text-green-400">KEEP</span>'
            : '<span class="text-xs font-semibold text-red-400">REMOVE</span>';
          var rdInfo = item.rd_filename
            ? '<span class="font-mono text-vd-muted" title="' + esc2(item.rd_filename) + '">' + esc2(item.rd_filename.length > 30 ? item.rd_filename.slice(0, 28) + '...' : item.rd_filename) + '</span>'
            : '<span class="text-vd-muted/50">—</span>';
          var sourceLabel = item.source
            ? '<span class="text-xs text-vd-muted">' + esc2(item.source) + '</span>'
            : '<span class="text-vd-muted/50">—</span>';
          var rowClass = idx % 2 === 1 ? 'bg-vd-bg/30' : '';
          return '<tr class="' + rowClass + ' border-t border-vd-border/50">' +
            '<td class="px-3 py-1.5 font-mono text-xs text-vd-muted">#' + esc2(String(item.item_id)) + '</td>' +
            '<td class="px-3 py-1.5">' + action + '</td>' +
            '<td class="px-3 py-1.5">' + livenessBadge(item.liveness) + '</td>' +
            '<td class="px-3 py-1.5">' + sourceLabel + '</td>' +
            '<td class="px-3 py-1.5 text-xs">' + rdInfo + '</td>' +
            '</tr>';
        });

        var keeperLiveness = group.keep.liveness;
        var headerBadge = livenessBadge(keeperLiveness);
        var reasonText = esc2(group.reason || '');

        return '<div class="bg-vd-bg border border-vd-border rounded-lg overflow-hidden">' +
          '<button type="button" onclick="toggleTableSection(\'' + groupId + '\')" ' +
          'class="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-white/5 transition-colors">' +
          '<div class="flex items-center gap-2 flex-wrap">' +
          '<span class="text-sm font-medium text-gray-200">' + title + '</span>' +
          headerBadge +
          '<span class="text-xs text-vd-muted">' + group.count + ' items</span>' +
          '</div>' +
          '<div class="flex items-center gap-2">' +
          '<span class="text-xs text-vd-muted hidden sm:block italic">' + reasonText + '</span>' +
          '<svg id="chevron-' + groupId + '" class="w-4 h-4 text-vd-muted chevron rotated" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
          '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>' +
          '</svg>' +
          '</div>' +
          '</button>' +
          '<div id="body-' + groupId + '" class="section-body collapsed">' +
          '<div>' +
          '<div class="border-t border-vd-border overflow-x-auto">' +
          '<table class="w-full text-sm">' +
          '<thead><tr class="text-xs text-vd-muted uppercase tracking-wider bg-vd-card">' +
          '<th class="px-3 py-1.5 text-left">ID</th>' +
          '<th class="px-3 py-1.5 text-left">Action</th>' +
          '<th class="px-3 py-1.5 text-left">Liveness</th>' +
          '<th class="px-3 py-1.5 text-left">Source</th>' +
          '<th class="px-3 py-1.5 text-left">RD Filename</th>' +
          '</tr></thead>' +
          '<tbody>' + rows.join('') + '</tbody>' +
          '</table>' +
          '<p class="px-3 py-1.5 text-xs text-vd-muted italic border-t border-vd-border/50">Reason: ' + reasonText + '</p>' +
          '</div></div></div>' +
          '</div>';
      }).join('');
    }

    // Show/hide execute button
    var executeWrap = document.getElementById('smart-execute-wrap');
    executeWrap.classList.toggle('hidden', data.total_to_remove === 0);

    document.getElementById('smart-preview-section').classList.remove('hidden');
  }

  // ---------------------------------------------------------------------------
  // Smart Cleanup — Execute
  // ---------------------------------------------------------------------------
  window.runSmartCleanupExecute = function() {
    if (!_smartPreview || !_smartPreview.groups || !_smartPreview.groups.length) return;

    var toRemove = _smartPreview.total_to_remove || 0;
    var rdDelete = (_smartPreview.rd_ids_to_delete || []).length;

    var msg = 'This will permanently delete ' + toRemove + ' duplicate item' +
              (toRemove === 1 ? '' : 's') +
              ' and their associated records.';
    if (rdDelete > 0) {
      msg += ' It will also delete ' + rdDelete + ' RD torrent' +
             (rdDelete === 1 ? '' : 's') + ' from your Real-Debrid account.';
    }
    msg += ' Continue?';
    if (!confirm(msg)) return;

    setSmartExecuteLoading(true);
    document.getElementById('smart-result').classList.add('hidden');

    csrfFetch('/api/tools/cleanup/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ groups: _smartPreview.groups })
    })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setSmartExecuteLoading(false);
      renderSmartResult(data);
      showToast('Removed ' + data.items_removed + ' item' + (data.items_removed === 1 ? '' : 's'), 'success');
      // Hide the execute button and groups so the user sees the result clearly
      document.getElementById('smart-execute-wrap').classList.add('hidden');
      document.getElementById('smart-groups-container').innerHTML = '';
      _smartPreview = null;
    })
    .catch(function(err) {
      setSmartExecuteLoading(false);
      showToast('Cleanup failed: ' + err.message, 'error');
    });
  };

  function setSmartExecuteLoading(loading) {
    var btn = document.getElementById('smart-execute-btn');
    var icon = document.getElementById('smart-execute-icon');
    var spinner = document.getElementById('smart-execute-spinner');
    var label = document.getElementById('smart-execute-btn-label');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Running cleanup...' : 'Run Cleanup';
  }

  function renderSmartResult(data) {
    var statsEl = document.getElementById('smart-result-stats');
    statsEl.innerHTML =
      statCard2('Groups', data.groups_processed, 'text-white') +
      statCard2('Removed', data.items_removed, data.items_removed > 0 ? 'text-red-400' : 'text-vd-muted') +
      statCard2('Kept', data.items_kept, 'text-green-400') +
      statCard2('Symlinks', data.symlinks_deleted_from_disk, 'text-vd-muted') +
      statCard2('RD Deleted', data.rd_torrents_deleted, data.rd_torrents_deleted > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCard2('RD Failed', data.rd_torrents_failed, data.rd_torrents_failed > 0 ? 'text-red-400' : 'text-vd-muted');

    var errorsWrap = document.getElementById('smart-result-errors-wrap');
    var errorsList = document.getElementById('smart-result-errors-list');
    if (data.errors && data.errors.length) {
      errorsList.innerHTML = data.errors.map(function(e) {
        return '<li>' + esc2(e) + '</li>';
      }).join('');
      errorsWrap.classList.remove('hidden');
    } else {
      errorsWrap.classList.add('hidden');
    }

    document.getElementById('smart-result').classList.remove('hidden');
  }

})();

// ---------------------------------------------------------------------------
// RD Account Cleanup (Phase 2)
// ---------------------------------------------------------------------------
(function() {

  var _rdScanData = null;   // Full RdCleanupScan response
  var _activeFilter = 'all';
  var _selectedIds = new Set();  // rd_id strings currently checked
  var _executing = false;

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  function escRd(s) { return VD.escapeHtml(s); }

  function formatBytes(bytes) { return VD.formatBytes(bytes); }

  function statCardRd(label, value, sub, colorClass) {
    return '<div class="bg-vd-card border border-vd-border rounded-lg px-4 py-3">' +
      '<p class="text-xs text-vd-muted uppercase tracking-wider mb-1">' + escRd(label) + '</p>' +
      '<p class="text-xl font-bold ' + colorClass + '">' + escRd(String(value)) + '</p>' +
      (sub ? '<p class="text-xs text-vd-muted mt-0.5">' + escRd(sub) + '</p>' : '') +
      '</div>';
  }

  var _CAT_COLORS = {
    protected: { badge: 'bg-green-900/50 text-green-300 border-green-700',  stat: 'text-green-400'  },
    dead:      { badge: 'bg-red-900/50 text-red-300 border-red-700',        stat: 'text-red-400'    },
    stale:     { badge: 'bg-amber-900/50 text-amber-300 border-amber-700',  stat: 'text-amber-400'  },
    orphaned:  { badge: 'bg-gray-700/60 text-gray-300 border-gray-600',     stat: 'text-gray-300'   },
    duplicate: { badge: 'bg-blue-900/50 text-blue-300 border-blue-700',     stat: 'text-blue-400'   },
  };

  function categoryBadge(cat) {
    var cfg = _CAT_COLORS[cat] || _CAT_COLORS.orphaned;
    return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ' + cfg.badge + '">' +
      escRd(cat.charAt(0).toUpperCase() + cat.slice(1)) + '</span>';
  }

  // ---------------------------------------------------------------------------
  // Tab styling
  // ---------------------------------------------------------------------------
  function updateTabStyles(activeTab) {
    var tabs = ['all','dead','stale','orphaned','duplicate','protected'];
    tabs.forEach(function(t) {
      var el = document.getElementById('rd-tab-' + t);
      if (!el) return;
      if (t === activeTab) {
        el.className = 'rd-tab rd-tab-active px-3 py-1 rounded text-xs font-medium transition-colors bg-vd-accent text-white';
      } else {
        el.className = 'rd-tab px-3 py-1 rounded text-xs font-medium transition-colors bg-vd-bg border border-vd-border text-vd-muted hover:text-gray-200 hover:bg-white/5';
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Scan
  // ---------------------------------------------------------------------------
  window.runRdCleanupScan = function() {
    if (_executing) return;
    setRdScanLoading(true);
    document.getElementById('rd-results-section').classList.add('hidden');
    document.getElementById('rd-execute-result').classList.add('hidden');
    _rdScanData = null;
    _selectedIds.clear();
    _activeFilter = 'all';

    csrfFetch('/api/tools/rd-cleanup/scan', { method: 'POST' })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setRdScanLoading(false);
      _rdScanData = data;
      renderRdScanResults(data);
    })
    .catch(function(err) {
      setRdScanLoading(false);
      showToast('RD scan failed: ' + err.message, 'error');
    });
  };

  function setRdScanLoading(loading) {
    var btn = document.getElementById('rd-scan-btn');
    var icon = document.getElementById('rd-scan-icon');
    var spinner = document.getElementById('rd-scan-spinner');
    var label = document.getElementById('rd-scan-btn-label');
    var status = document.getElementById('rd-scan-status');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Scanning...' : 'Scan RD Account';
    if (status) status.classList.toggle('hidden', !loading);
  }

  // ---------------------------------------------------------------------------
  // Render scan results
  // ---------------------------------------------------------------------------
  function renderRdScanResults(data) {
    // Summary badges
    var summaries = data.summaries || [];
    var catMap = {};
    summaries.forEach(function(s) { catMap[s.category] = s; });

    var order = ['protected','dead','stale','orphaned','duplicate'];
    var badgesHtml = order.map(function(cat) {
      var s = catMap[cat] || { count: 0, total_bytes: 0 };
      var cfg = _CAT_COLORS[cat] || _CAT_COLORS.orphaned;
      return statCardRd(
        cat.charAt(0).toUpperCase() + cat.slice(1),
        s.count,
        formatBytes(s.total_bytes),
        cfg.stat
      );
    }).join('');
    document.getElementById('rd-summary-badges').innerHTML = badgesHtml;

    // Warnings
    var warningsPanel = document.getElementById('rd-warnings-panel');
    var warningsList = document.getElementById('rd-warnings-list');
    if (data.warnings && data.warnings.length > 0) {
      warningsList.innerHTML = data.warnings.map(function(w) {
        return '<li>' + escRd(w) + '</li>';
      }).join('');
      warningsPanel.classList.remove('hidden');
    } else {
      warningsPanel.classList.add('hidden');
    }

    // Apply default filter
    _activeFilter = 'all';
    updateTabStyles('all');
    renderTorrentTable();

    document.getElementById('rd-results-section').classList.remove('hidden');
  }

  // ---------------------------------------------------------------------------
  // Torrent table render (respects _activeFilter)
  // ---------------------------------------------------------------------------
  var TABLE_CAP_RD = 100;

  function renderTorrentTable() {
    if (!_rdScanData) return;

    var allTorrents = _rdScanData.torrents || [];
    var filtered = _activeFilter === 'all'
      ? allTorrents
      : allTorrents.filter(function(t) { return t.category === _activeFilter; });

    var visible = filtered.slice(0, TABLE_CAP_RD);
    var overflow = filtered.length - visible.length;

    var tbody = document.getElementById('rd-torrent-tbody');
    var overflowEl = document.getElementById('rd-table-overflow');

    // Header checkbox — indeterminate if some selected
    var allCb = document.getElementById('rd-select-all-cb');
    if (allCb) {
      var visibleCheckable = visible.filter(function(t) { return t.category !== 'protected'; });
      var checkedCount = visibleCheckable.filter(function(t) { return _selectedIds.has(t.rd_id); }).length;
      allCb.checked = visibleCheckable.length > 0 && checkedCount === visibleCheckable.length;
      allCb.indeterminate = checkedCount > 0 && checkedCount < visibleCheckable.length;
      allCb.disabled = _executing;
    }

    var rows = visible.map(function(t, idx) {
      var isProtected = t.category === 'protected';
      var checked = _selectedIds.has(t.rd_id);
      var rowTint = isProtected ? ' bg-green-950/20' : (idx % 2 === 1 ? ' bg-vd-bg/30' : '');

      var cbCell = isProtected
        ? '<td class="px-3 py-1.5 w-8"></td>'
        : '<td class="px-3 py-1.5 w-8"><input type="checkbox" ' +
          'data-rdid="' + escRd(t.rd_id) + '" ' +
          (checked ? 'checked ' : '') +
          (_executing ? 'disabled ' : '') +
          'onchange="rdToggleItem(this)" ' +
          'class="rounded border-vd-border bg-vd-bg text-vd-accent focus:ring-vd-accent"></td>';

      var shortName = t.filename.length > 60 ? t.filename.slice(0, 58) + '...' : t.filename;
      var filenameCell = '<td class="px-3 py-1.5 font-mono text-xs text-gray-200" title="' + escRd(t.filename) + '">' +
        escRd(shortName) + '</td>';

      var sizeCell = '<td class="px-3 py-1.5 text-xs text-vd-muted whitespace-nowrap">' + formatBytes(t.filesize) + '</td>';
      var statusCell = '<td class="px-3 py-1.5 text-xs text-vd-muted font-mono">' + escRd(t.status) + '</td>';
      var catCell = '<td class="px-3 py-1.5">' + categoryBadge(t.category) + '</td>';
      var reasonCell = '<td class="px-3 py-1.5 text-xs text-vd-muted max-w-xs truncate" title="' + escRd(t.reason) + '">' + escRd(t.reason) + '</td>';

      return '<tr class="border-t border-vd-border/50' + rowTint + '">' +
        cbCell + filenameCell + sizeCell + statusCell + catCell + reasonCell + '</tr>';
    });

    tbody.innerHTML = rows.join('');

    if (overflow > 0) {
      overflowEl.textContent = '... and ' + overflow + ' more in this category (not shown)';
      overflowEl.classList.remove('hidden');
    } else {
      overflowEl.classList.add('hidden');
    }

    updateSelectionCounter();
  }

  // ---------------------------------------------------------------------------
  // Filter by category tab
  // ---------------------------------------------------------------------------
  window.rdFilterByCategory = function(cat) {
    _activeFilter = cat;
    updateTabStyles(cat);
    renderTorrentTable();
  };

  // ---------------------------------------------------------------------------
  // Checkbox handling
  // ---------------------------------------------------------------------------
  window.rdToggleItem = function(cb) {
    var rdId = cb.getAttribute('data-rdid');
    if (!rdId) return;
    if (cb.checked) {
      _selectedIds.add(rdId);
    } else {
      _selectedIds.delete(rdId);
    }
    // Update header checkbox state
    var allCb = document.getElementById('rd-select-all-cb');
    if (allCb) {
      var allTorrents = _rdScanData ? _rdScanData.torrents || [] : [];
      var filtered = _activeFilter === 'all'
        ? allTorrents
        : allTorrents.filter(function(t) { return t.category === _activeFilter; });
      var visible = filtered.slice(0, TABLE_CAP_RD);
      var checkable = visible.filter(function(t) { return t.category !== 'protected'; });
      var checked = checkable.filter(function(t) { return _selectedIds.has(t.rd_id); }).length;
      allCb.checked = checkable.length > 0 && checked === checkable.length;
      allCb.indeterminate = checked > 0 && checked < checkable.length;
    }
    updateSelectionCounter();
  };

  window.rdToggleSelectAll = function(checked) {
    if (!_rdScanData) return;
    var allTorrents = _rdScanData.torrents || [];
    var filtered = _activeFilter === 'all'
      ? allTorrents
      : allTorrents.filter(function(t) { return t.category === _activeFilter; });
    var visible = filtered.slice(0, TABLE_CAP_RD);
    visible.forEach(function(t) {
      if (t.category === 'protected') return;
      if (checked) {
        _selectedIds.add(t.rd_id);
      } else {
        _selectedIds.delete(t.rd_id);
      }
    });
    renderTorrentTable();
  };

  // ---------------------------------------------------------------------------
  // Quick-select helpers
  // ---------------------------------------------------------------------------
  window.rdSelectAllDeadStale = function() {
    if (!_rdScanData) return;
    (_rdScanData.torrents || []).forEach(function(t) {
      if (t.category === 'dead' || t.category === 'stale') {
        _selectedIds.add(t.rd_id);
      }
    });
    renderTorrentTable();
  };

  window.rdSelectAllOrphaned = function() {
    if (!_rdScanData) return;
    (_rdScanData.torrents || []).forEach(function(t) {
      if (t.category === 'orphaned') {
        _selectedIds.add(t.rd_id);
      }
    });
    renderTorrentTable();
  };

  window.rdDeselectAll = function() {
    _selectedIds.clear();
    renderTorrentTable();
  };

  // ---------------------------------------------------------------------------
  // Selection counter
  // ---------------------------------------------------------------------------
  function updateSelectionCounter() {
    var counter = document.getElementById('rd-selection-counter');
    var removeBtn = document.getElementById('rd-remove-btn');
    if (!counter) return;

    var count = _selectedIds.size;
    var totalBytes = 0;
    if (_rdScanData) {
      (_rdScanData.torrents || []).forEach(function(t) {
        if (_selectedIds.has(t.rd_id)) totalBytes += (t.filesize || 0);
      });
    }
    counter.textContent = count + ' selected (' + formatBytes(totalBytes) + ')';
    if (removeBtn) removeBtn.disabled = count === 0 || _executing;
  }

  // ---------------------------------------------------------------------------
  // Execute removal
  // ---------------------------------------------------------------------------
  window.runRdCleanupExecute = function() {
    if (_executing || _selectedIds.size === 0) return;

    var rdIds = Array.from(_selectedIds);
    var count = rdIds.length;
    var totalBytes = 0;
    if (_rdScanData) {
      (_rdScanData.torrents || []).forEach(function(t) {
        if (_selectedIds.has(t.rd_id)) totalBytes += (t.filesize || 0);
      });
    }

    var msg = 'This will permanently delete ' + count + ' torrent' +
      (count === 1 ? '' : 's') + ' (' + formatBytes(totalBytes) + ') from your Real-Debrid account. ' +
      'This cannot be undone. Continue?';
    if (!confirm(msg)) return;

    _executing = true;
    setRdRemoveLoading(true);
    document.getElementById('rd-execute-result').classList.add('hidden');

    // Disable all checkboxes
    renderTorrentTable();

    csrfFetch('/api/tools/rd-cleanup/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rd_ids: rdIds })
    })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      _executing = false;
      setRdRemoveLoading(false);
      _selectedIds.clear();
      renderRdCleanupResult(data);
      var msg2 = 'Deleted ' + data.deleted + ' torrent' + (data.deleted === 1 ? '' : 's');
      if (data.failed > 0) msg2 += ', ' + data.failed + ' failed';
      showToast(msg2, data.failed > 0 ? 'warning' : 'success');
    })
    .catch(function(err) {
      _executing = false;
      setRdRemoveLoading(false);
      renderTorrentTable();
      showToast('Remove failed: ' + err.message, 'error');
    });
  };

  function setRdRemoveLoading(loading) {
    var btn = document.getElementById('rd-remove-btn');
    var icon = document.getElementById('rd-remove-icon');
    var spinner = document.getElementById('rd-remove-spinner');
    var label = document.getElementById('rd-remove-btn-label');
    if (!btn) return;
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Removing...' : 'Remove Selected';
  }

  // ---------------------------------------------------------------------------
  // Render execute result
  // ---------------------------------------------------------------------------
  function renderRdCleanupResult(data) {
    var statsEl = document.getElementById('rd-execute-stats');
    statsEl.innerHTML =
      statCardRd('Deleted', data.deleted, null, data.deleted > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCardRd('Failed', data.failed, null, data.failed > 0 ? 'text-red-400' : 'text-vd-muted') +
      statCardRd('Rejected (Protected)', data.rejected_protected, null, data.rejected_protected > 0 ? 'text-yellow-400' : 'text-vd-muted') +
      statCardRd('Rejected (Not Found)', data.rejected_not_found, null, data.rejected_not_found > 0 ? 'text-yellow-400' : 'text-vd-muted');

    var rateLimitWarn = document.getElementById('rd-rate-limit-warn');
    if (rateLimitWarn) rateLimitWarn.classList.toggle('hidden', !data.rate_limited);

    var errorsWrap = document.getElementById('rd-execute-errors-wrap');
    var errorsList = document.getElementById('rd-execute-errors-list');
    if (data.errors && data.errors.length > 0) {
      errorsList.innerHTML = data.errors.map(function(e) {
        return '<li>' + escRd(e) + '</li>';
      }).join('');
      errorsWrap.classList.remove('hidden');
    } else {
      errorsWrap.classList.add('hidden');
    }

    // Hide results table, show execute result
    document.getElementById('rd-results-section').classList.add('hidden');
    document.getElementById('rd-execute-result').classList.remove('hidden');
  }

})();

// ---------------------------------------------------------------------------
// Symlink Health Check
// ---------------------------------------------------------------------------
(function() {

  var _shScanData = null;   // Full scan response
  var _activeFilter = 'all';
  var _selectedIds = new Set();  // symlink_id numbers currently checked
  var _executing = false;

  var TABLE_CAP_SH = 100;

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  function escSh(s) { return VD.escapeHtml(s); }

  function statCardSh(label, value, colorClass) {
    return '<div class="bg-vd-card border border-vd-border rounded-lg px-4 py-3">' +
      '<p class="text-xs text-vd-muted uppercase tracking-wider mb-1">' + escSh(label) + '</p>' +
      '<p class="text-xl font-bold ' + colorClass + '">' + escSh(String(value)) + '</p>' +
      '</div>';
  }

  // Truncate a filesystem path to the last 2 path segments: dir/filename
  function shortPath(p) {
    if (!p) return '';
    var parts = p.replace(/\\/g, '/').split('/').filter(Boolean);
    if (parts.length <= 2) return parts.join('/');
    return parts.slice(-2).join('/');
  }

  function statusBadge(status) {
    if (status === 'recoverable') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-emerald-900/50 text-emerald-300 border border-emerald-700">Recoverable</span>';
    }
    if (status === 'requeued') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-900/50 text-blue-300 border border-blue-700">Re-queued</span>';
    }
    if (status === 'recreated') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-cyan-900/50 text-cyan-300 border border-cyan-700">Recreated</span>';
    }
    if (status === 'cleaned') {
      return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-800/50 text-gray-400 border border-gray-600">Cleaned</span>';
    }
    return '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-900/50 text-red-300 border border-red-700">Dead</span>';
  }

  function pad2sh(n) {
    return n < 10 ? '0' + n : String(n);
  }

  // ---------------------------------------------------------------------------
  // Tab styling
  // ---------------------------------------------------------------------------
  function updateShTabStyles(activeTab) {
    var tabs = ['all', 'recoverable', 'dead'];
    tabs.forEach(function(t) {
      var el = document.getElementById('sh-tab-' + t);
      if (!el) return;
      if (t === activeTab) {
        el.className = 'sh-tab sh-tab-active px-3 py-1 rounded text-xs font-medium transition-colors bg-vd-accent text-white';
      } else {
        el.className = 'sh-tab px-3 py-1 rounded text-xs font-medium transition-colors bg-vd-bg border border-vd-border text-vd-muted hover:text-gray-200 hover:bg-white/5';
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Scan
  // ---------------------------------------------------------------------------
  window.runSymlinkHealthScan = function() {
    if (_executing) return;
    setShScanLoading(true);
    document.getElementById('sh-results-section').classList.add('hidden');
    document.getElementById('sh-execute-result').classList.add('hidden');
    _shScanData = null;
    _selectedIds.clear();
    _activeFilter = 'all';

    csrfFetch('/api/tools/symlink-health/scan', { method: 'POST' })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      setShScanLoading(false);
      _shScanData = data;
      renderShScanResults(data);
    })
    .catch(function(err) {
      setShScanLoading(false);
      showToast('Symlink scan failed: ' + err.message, 'error');
    });
  };

  function setShScanLoading(loading) {
    var btn = document.getElementById('sh-scan-btn');
    var icon = document.getElementById('sh-scan-icon');
    var spinner = document.getElementById('sh-scan-spinner');
    var label = document.getElementById('sh-scan-btn-label');
    btn.disabled = loading;
    icon.classList.toggle('hidden', loading);
    spinner.classList.toggle('hidden', !loading);
    label.textContent = loading ? 'Scanning...' : 'Scan Symlinks';
  }

  // ---------------------------------------------------------------------------
  // Render scan results
  // ---------------------------------------------------------------------------
  function renderShScanResults(data) {
    // Summary badges
    var badgesEl = document.getElementById('sh-summary-badges');
    badgesEl.innerHTML =
      statCardSh('Total Symlinks', data.total_symlinks || 0, 'text-white') +
      statCardSh('Healthy', data.healthy || 0, (data.healthy || 0) > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCardSh('Broken', (data.recoverable || 0) + (data.dead || 0), ((data.recoverable || 0) + (data.dead || 0)) > 0 ? 'text-amber-400' : 'text-vd-muted') +
      statCardSh('Recoverable', data.recoverable || 0, (data.recoverable || 0) > 0 ? 'text-emerald-400' : 'text-vd-muted') +
      statCardSh('Dead', data.dead || 0, (data.dead || 0) > 0 ? 'text-red-400' : 'text-vd-muted');

    // Filter only broken items for the table
    _activeFilter = 'all';
    updateShTabStyles('all');
    renderShTable();

    document.getElementById('sh-results-section').classList.remove('hidden');
  }

  // ---------------------------------------------------------------------------
  // Table render (respects _activeFilter; only broken items are shown)
  // ---------------------------------------------------------------------------
  function renderShTable() {
    if (!_shScanData) return;

    var allItems = _shScanData.items || [];
    var filtered = _activeFilter === 'all'
      ? allItems
      : allItems.filter(function(item) { return item.status === _activeFilter; });

    var visible = filtered.slice(0, TABLE_CAP_SH);
    var overflow = filtered.length - visible.length;

    var tbody = document.getElementById('sh-symlink-tbody');
    var overflowEl = document.getElementById('sh-table-overflow');

    // Header checkbox state
    var allCb = document.getElementById('sh-select-all-cb');
    if (allCb) {
      var checkedCount = visible.filter(function(item) { return _selectedIds.has(item.symlink_id); }).length;
      allCb.checked = visible.length > 0 && checkedCount === visible.length;
      allCb.indeterminate = checkedCount > 0 && checkedCount < visible.length;
      allCb.disabled = _executing;
    }

    if (visible.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="px-4 py-6 text-center text-sm text-vd-muted">No broken symlinks found.</td></tr>';
      overflowEl.classList.add('hidden');
      updateShSelectionCounter();
      return;
    }

    var rows = visible.map(function(item, idx) {
      var checked = _selectedIds.has(item.symlink_id);
      var processed = item.status === 'requeued' || item.status === 'cleaned' || item.status === 'recreated';
      var rowTint = processed ? ' opacity-50' : (idx % 2 === 1 ? ' bg-vd-bg/30' : '');

      var cbCell = processed
        ? '<td class="px-3 py-1.5 w-8"></td>'
        : '<td class="px-3 py-1.5 w-8"><input type="checkbox" ' +
          'data-symlinkid="' + escSh(String(item.symlink_id)) + '" ' +
          (checked ? 'checked ' : '') +
          (_executing ? 'disabled ' : '') +
          'onchange="shToggleItem(this)" ' +
          'class="rounded border-vd-border bg-vd-bg text-vd-accent focus:ring-vd-accent"></td>';

      var seInfo = item.season != null
        ? ' S' + pad2sh(item.season) + (item.episode != null ? 'E' + pad2sh(item.episode) : '')
        : '';
      var typeColor = item.media_type === 'movie' ? 'text-purple-400' : 'text-teal-400';
      var titleCell = '<td class="px-3 py-1.5">' +
        '<span class="text-gray-200">' + escSh(item.title) + seInfo + '</span>' +
        '<span class="ml-2 text-xs uppercase tracking-wide ' + typeColor + '">' + escSh(item.media_type) + '</span>' +
        '</td>';

      var stateBadge = '<span class="badge-' + escSh(item.state) + ' text-white text-xs px-2 py-0.5 rounded-full">' + escSh(item.state) + '</span>';
      var stateCell = '<td class="px-3 py-1.5">' + stateBadge + '</td>';

      var statusCell = '<td class="px-3 py-1.5">' + statusBadge(item.status) + '</td>';

      var oldPathShort = shortPath(item.source_path);
      var oldPathCell = '<td class="px-3 py-1.5 font-mono text-xs text-vd-muted max-w-xs truncate" title="' + escSh(item.source_path) + '">' + escSh(oldPathShort) + '</td>';

      var newPathVal = item.mount_match_path || null;
      var newPathShort = newPathVal ? shortPath(newPathVal) : null;
      var newPathCell = newPathShort
        ? '<td class="px-3 py-1.5 font-mono text-xs text-emerald-400 max-w-xs truncate" title="' + escSh(newPathVal) + '">' + escSh(newPathShort) + '</td>'
        : '<td class="px-3 py-1.5 text-xs text-vd-muted/50">&mdash;</td>';

      return '<tr class="border-t border-vd-border/50' + rowTint + '">' +
        cbCell + titleCell + stateCell + statusCell + oldPathCell + newPathCell + '</tr>';
    });

    tbody.innerHTML = rows.join('');

    if (overflow > 0) {
      overflowEl.textContent = '... and ' + overflow + ' more in this filter (not shown)';
      overflowEl.classList.remove('hidden');
    } else {
      overflowEl.classList.add('hidden');
    }

    updateShSelectionCounter();
  }

  // ---------------------------------------------------------------------------
  // Filter tab
  // ---------------------------------------------------------------------------
  window.shFilterByStatus = function(status) {
    _activeFilter = status;
    updateShTabStyles(status);
    renderShTable();
  };

  // ---------------------------------------------------------------------------
  // Checkbox handling
  // ---------------------------------------------------------------------------
  window.shToggleItem = function(cb) {
    var id = parseInt(cb.getAttribute('data-symlinkid'), 10);
    if (isNaN(id)) return;
    if (cb.checked) {
      _selectedIds.add(id);
    } else {
      _selectedIds.delete(id);
    }
    var allCb = document.getElementById('sh-select-all-cb');
    if (allCb && _shScanData) {
      var allItems = _shScanData.items || [];
      var filtered = _activeFilter === 'all'
        ? allItems
        : allItems.filter(function(item) { return item.status === _activeFilter; });
      var visible = filtered.slice(0, TABLE_CAP_SH);
      var checked = visible.filter(function(item) { return _selectedIds.has(item.symlink_id); }).length;
      allCb.checked = visible.length > 0 && checked === visible.length;
      allCb.indeterminate = checked > 0 && checked < visible.length;
    }
    updateShSelectionCounter();
  };

  window.shToggleSelectAll = function(checked) {
    if (!_shScanData) return;
    var allItems = _shScanData.items || [];
    var filtered = _activeFilter === 'all'
      ? allItems
      : allItems.filter(function(item) { return item.status === _activeFilter; });
    var visible = filtered.slice(0, TABLE_CAP_SH);
    visible.forEach(function(item) {
      if (checked) {
        _selectedIds.add(item.symlink_id);
      } else {
        _selectedIds.delete(item.symlink_id);
      }
    });
    renderShTable();
  };

  // ---------------------------------------------------------------------------
  // Quick-select helpers
  // ---------------------------------------------------------------------------
  window.shSelectAllRecoverable = function() {
    if (!_shScanData) return;
    (_shScanData.items || []).forEach(function(item) {
      if (item.status === 'recoverable') _selectedIds.add(item.symlink_id);
    });
    renderShTable();
  };

  window.shSelectAllDead = function() {
    if (!_shScanData) return;
    (_shScanData.items || []).forEach(function(item) {
      if (item.status === 'dead') _selectedIds.add(item.symlink_id);
    });
    renderShTable();
  };

  window.shDeselectAll = function() {
    _selectedIds.clear();
    renderShTable();
  };

  // ---------------------------------------------------------------------------
  // Selection counter + button state
  // ---------------------------------------------------------------------------
  function updateShSelectionCounter() {
    var counter = document.getElementById('sh-selection-counter');
    var requeueBtn = document.getElementById('sh-requeue-btn');
    var cleanupBtn = document.getElementById('sh-cleanup-btn');
    if (!counter) return;

    var count = _selectedIds.size;
    counter.textContent = count + ' selected';

    // Enable requeue when any unprocessed item is selected
    var hasActionable = false;
    var hasDead = false;
    if (_shScanData) {
      (_shScanData.items || []).forEach(function(item) {
        if (_selectedIds.has(item.symlink_id)) {
          if (item.status === 'recoverable' || item.status === 'dead') hasActionable = true;
          if (item.status === 'dead') hasDead = true;
        }
      });
    }

    if (requeueBtn) requeueBtn.disabled = !hasActionable || _executing;
    if (cleanupBtn) cleanupBtn.disabled = !hasDead || _executing;
  }

  // ---------------------------------------------------------------------------
  // Re-queue selected items (recoverable or dead)
  // ---------------------------------------------------------------------------
  window.runShRequeue = function() {
    if (_executing || !_shScanData) return;

    var requeueIds = [];
    (_shScanData.items || []).forEach(function(item) {
      if (_selectedIds.has(item.symlink_id) && (item.status === 'recoverable' || item.status === 'dead')) {
        requeueIds.push(item.item_id);
      }
    });

    if (requeueIds.length === 0) return;

    var msg = 'Re-queue ' + requeueIds.length + ' item' + (requeueIds.length === 1 ? '' : 's') +
      ' back to WANTED? Recoverable items will find existing files in mount. Dead items will be re-scraped. Continue?';
    if (!confirm(msg)) return;

    runShExecute({ requeue_ids: requeueIds, cleanup_ids: [] }, 'Requeueing...');
  };

  // ---------------------------------------------------------------------------
  // Clean up selected dead items
  // ---------------------------------------------------------------------------
  window.runShCleanup = function() {
    if (_executing || !_shScanData) return;

    var cleanupIds = [];
    (_shScanData.items || []).forEach(function(item) {
      if (_selectedIds.has(item.symlink_id) && item.status === 'dead') {
        cleanupIds.push(item.item_id);
      }
    });

    if (cleanupIds.length === 0) return;

    var msg = 'Permanently remove symlink records for ' + cleanupIds.length + ' dead item' + (cleanupIds.length === 1 ? '' : 's') +
      '? Queue entries will remain unchanged (items stay DONE). This cannot be undone. Continue?';
    if (!confirm(msg)) return;

    runShExecute({ requeue_ids: [], cleanup_ids: cleanupIds }, 'Cleaning up...');
  };

  // ---------------------------------------------------------------------------
  // Execute (shared by requeue + cleanup)
  // ---------------------------------------------------------------------------
  function runShExecute(payload, loadingLabel) {
    _executing = true;
    setShActionLoading(true, loadingLabel);
    document.getElementById('sh-execute-result').classList.add('hidden');

    csrfFetch('/api/tools/symlink-health/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().then(function(body) {
          throw new Error(body.detail || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      _executing = false;
      setShActionLoading(false, '');

      // Update in-memory scan data: mark processed items
      if (_shScanData && _shScanData.items) {
        var recreatedSet = new Set(data.recreated_ids || []);
        var requeuedSet = new Set((payload.requeue_ids || []).filter(function(id) { return !recreatedSet.has(id); }));
        var cleanedSet = new Set(payload.cleanup_ids || []);
        _shScanData.items = _shScanData.items.map(function(item) {
          if (recreatedSet.has(item.item_id)) {
            return Object.assign({}, item, { status: 'recreated' });
          }
          if (requeuedSet.has(item.item_id)) {
            return Object.assign({}, item, { status: 'requeued', state: 'wanted' });
          }
          if (cleanedSet.has(item.item_id)) {
            return Object.assign({}, item, { status: 'cleaned', state: item.state });
          }
          return item;
        });
        // Update summary counts
        var remaining = _shScanData.items.filter(function(i) { return i.status === 'recoverable' || i.status === 'dead'; });
        _shScanData.broken = remaining.length;
        _shScanData.recoverable = remaining.filter(function(i) { return i.status === 'recoverable'; }).length;
        _shScanData.dead = remaining.filter(function(i) { return i.status === 'dead'; }).length;
      }

      _selectedIds.clear();
      renderShExecuteResult(data);
      // Re-render table to reflect updated statuses
      renderShTable();
      updateShSelectionCounter();

      var parts = [];
      if (data.recreated > 0) parts.push(data.recreated + ' item' + (data.recreated === 1 ? '' : 's') + ' recreated');
      if (data.requeued > 0) parts.push(data.requeued + ' item' + (data.requeued === 1 ? '' : 's') + ' re-queued');
      if (data.cleaned > 0) parts.push(data.cleaned + ' item' + (data.cleaned === 1 ? '' : 's') + ' cleaned up');
      if (parts.length > 0) {
        showToast(parts.join(', '), 'success');
      }
    })
    .catch(function(err) {
      _executing = false;
      setShActionLoading(false, '');
      showToast('Operation failed: ' + err.message, 'error');
    });
  }

  function setShActionLoading(loading, label) {
    var requeueBtn = document.getElementById('sh-requeue-btn');
    var requeueIcon = document.getElementById('sh-requeue-icon');
    var requeueSpinner = document.getElementById('sh-requeue-spinner');
    var requeueLabel = document.getElementById('sh-requeue-btn-label');
    var cleanupBtn = document.getElementById('sh-cleanup-btn');
    var cleanupIcon = document.getElementById('sh-cleanup-icon');
    var cleanupSpinner = document.getElementById('sh-cleanup-spinner');
    var cleanupLabel = document.getElementById('sh-cleanup-btn-label');

    if (requeueBtn) {
      requeueBtn.disabled = loading;
      requeueIcon.classList.toggle('hidden', loading);
      requeueSpinner.classList.toggle('hidden', !loading);
      requeueLabel.textContent = loading ? label : 'Re-queue Selected';
    }
    if (cleanupBtn) {
      cleanupBtn.disabled = loading;
      cleanupIcon.classList.toggle('hidden', loading);
      cleanupSpinner.classList.toggle('hidden', !loading);
      cleanupLabel.textContent = loading ? label : 'Clean Up Selected';
    }
  }

  // ---------------------------------------------------------------------------
  // Render execute result
  // ---------------------------------------------------------------------------
  function renderShExecuteResult(data) {
    var statsEl = document.getElementById('sh-execute-stats');
    statsEl.innerHTML =
      statCardSh('Recreated', data.recreated || 0, (data.recreated || 0) > 0 ? 'text-cyan-400' : 'text-vd-muted') +
      statCardSh('Re-queued', data.requeued || 0, (data.requeued || 0) > 0 ? 'text-green-400' : 'text-vd-muted') +
      statCardSh('Cleaned Up', data.cleaned || 0, (data.cleaned || 0) > 0 ? 'text-red-400' : 'text-vd-muted') +
      statCardSh('Symlinks Removed', data.symlinks_removed_from_disk || 0, 'text-vd-muted') +
      statCardSh('Errors', data.errors ? data.errors.length : 0, (data.errors && data.errors.length > 0) ? 'text-red-400' : 'text-vd-muted');

    var errorsWrap = document.getElementById('sh-execute-errors-wrap');
    var errorsList = document.getElementById('sh-execute-errors-list');
    if (data.errors && data.errors.length > 0) {
      errorsList.innerHTML = data.errors.map(function(e) {
        return '<li>' + escSh(e) + '</li>';
      }).join('');
      errorsWrap.classList.remove('hidden');
    } else {
      errorsWrap.classList.add('hidden');
    }

    document.getElementById('sh-execute-result').classList.remove('hidden');
  }

})();
