(function() {
'use strict';

// ─── Helpers ────────────────────────────────────────────────────────────────

function scoreColorClass(score) {
  if (score >= 70) return 'score-bar-high';
  if (score >= 40) return 'score-bar-mid';
  return 'score-bar-low';
}

function scoreTextColor(score) {
  if (score >= 70) return 'text-green-400';
  if (score >= 40) return 'text-yellow-400';
  return 'text-red-400';
}

// ─── Show/hide season+episode fields ────────────────────────────────────────

window.toggleShowFields = function(mediaType) {
  const fields = document.getElementById('show-fields');
  if (mediaType === 'show') {
    fields.classList.add('visible');
  } else {
    fields.classList.remove('visible');
    document.getElementById('season').value = '';
    document.getElementById('episode').value = '';
  }
};

// ─── Manual add: show/hide show fields based on manual-type ─────────────────

document.getElementById('manual-type').addEventListener('change', function() {
  document.getElementById('manual-show-fields').classList.toggle('hidden', this.value !== 'show');
});

// ─── Manual add: auto-detect "Season N" from title input ────────────────────

document.getElementById('manual-title').addEventListener('input', function() {
  const match = this.value.match(/\bseason\s+(\d+)\b/i);
  if (match) {
    // Auto-switch to TV Show and show the fields
    const typeSelect = document.getElementById('manual-type');
    if (typeSelect.value !== 'show') {
      typeSelect.value = 'show';
      document.getElementById('manual-show-fields').classList.remove('hidden');
    }
    const seasonInput = document.getElementById('manual-season');
    const packCheckbox = document.getElementById('manual-season-pack');
    if (seasonInput && !seasonInput.value) {
      seasonInput.value = match[1];
    }
    if (packCheckbox && !packCheckbox.checked) {
      packCheckbox.checked = true;
    }
  }
});

// ─── Search form submission ──────────────────────────────────────────────────

// Store the current search payload so add buttons can reference it.
let _lastSearchPayload = null;
let _currentResults = [];      // current merged result list
let _totalRaw = 0;
let _totalFiltered = 0;
const _cacheStatusMap = new Map();  // info_hash → boolean, survives re-renders

window.handleSearch = async function(event) {
  event.preventDefault();

  const form     = document.getElementById('search-form');
  const errorEl  = document.getElementById('form-error');
  const query    = document.getElementById('query').value.trim();

  errorEl.classList.add('hidden');
  errorEl.textContent = '';

  if (!query) {
    errorEl.textContent = 'Please enter a title to search.';
    errorEl.classList.remove('hidden');
    document.getElementById('query').focus();
    return;
  }

  const imdbId  = document.getElementById('imdb_id').value.trim() || null;
  const season  = document.getElementById('season').value.trim();
  const episode = document.getElementById('episode').value.trim();

  const payload = {
    query:           query,
    imdb_id:         imdbId || null,
    media_type:      document.getElementById('media_type').value,
    season:          season  ? parseInt(season,  10) : null,
    episode:         episode ? parseInt(episode, 10) : null,
    quality_profile: document.getElementById('quality_profile').value,
  };

  // Auto-resolve IMDB ID from TMDB if not provided
  if (!payload.imdb_id && payload.query) {
    try {
      const tmdbMediaType = payload.media_type === 'show' ? 'tv' : 'movie';
      const searchResp = await fetch('/api/discover/search?' + new URLSearchParams({
        q: payload.query,
        media_type: tmdbMediaType,
        page: '1'
      }));
      if (searchResp.ok) {
        const tmdbData = await searchResp.json();
        const items = tmdbData.items || [];
        if (items.length > 0) {
          // Pick the best title match instead of blindly using first result
          const queryLower = payload.query.toLowerCase();
          let bestMatch = items[0];
          let bestScore = 0;
          for (const item of items) {
            const itemTitle = (item.title || '').toLowerCase();
            let score = 0;
            if (itemTitle === queryLower) {
              score = 3; // exact match
            } else if (itemTitle.startsWith(queryLower)) {
              score = 2; // title starts with query
            } else if (itemTitle.includes(queryLower)) {
              score = 1; // title contains query
            }
            if (score > bestScore) {
              bestScore = score;
              bestMatch = item;
            }
          }
          // Store tmdb_id, year, and canonical title from the best match
          payload.tmdb_id = bestMatch.tmdb_id || null;
          const rawDate = bestMatch.release_date || bestMatch.first_air_date || '';
          payload.year = rawDate.length >= 4 ? parseInt(rawDate.substring(0, 4), 10) : null;
          payload.tmdb_title = bestMatch.title || bestMatch.name || null;

          const resolveResp = await fetch('/api/discover/resolve/' + encodeURIComponent(tmdbMediaType) + '/' + encodeURIComponent(bestMatch.tmdb_id));
          if (resolveResp.ok) {
            const resolveData = await resolveResp.json();
            if (resolveData.imdb_id) {
              payload.imdb_id = resolveData.imdb_id;
              document.getElementById('imdb_id').value = resolveData.imdb_id;
            }
            if (resolveData.tvdb_id) {
              payload.tvdb_id = resolveData.tvdb_id;
            }
          }
        }
      }
    } catch (err) {
      // Non-fatal — continue search without IMDB ID
      console.debug('TMDB auto-resolve failed:', err);
    }
  }

  _lastSearchPayload = payload;
  setSearchLoading(true);

  try {
    _cacheCheckGeneration++;   // invalidate any stale cache-check loops immediately
    _cacheStatusMap.clear();
    _currentResults = [];
    _totalRaw = 0;
    _totalFiltered = 0;

    // Fire zilean request (fast)
    const zileanPromise = csrfFetch('/api/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...payload, scrapers: ['zilean'] }),
    }).then(r => {
      if (!r.ok) throw new Error('Search failed');
      return r.json();
    });

    // Fire torrentio request in parallel (slow, only if imdb_id is set)
    let torrentioPromise = null;
    if (payload.imdb_id) {
      torrentioPromise = csrfFetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...payload, scrapers: ['torrentio'] }),
      }).then(r => {
        if (!r.ok) throw new Error('Torrentio search failed');
        return r.json();
      }).catch(err => {
        console.warn('Torrentio search failed:', err);
        return null;
      });
    }

    // Render zilean results immediately
    const zileanData = await zileanPromise;
    _currentResults = (zileanData.results || []).slice();
    _totalRaw = zileanData.total_raw || 0;
    _totalFiltered = zileanData.total_filtered || 0;

    renderResults({ results: _currentResults, total_raw: _totalRaw, total_filtered: _totalFiltered });
    setSearchLoading(false);

    // Scroll results into view (on mobile the form pushes results below the fold).
    document.getElementById('results-section').scrollIntoView({ behavior: 'instant', block: 'start' });

    // Force a browser paint so the user sees results before cache checks start.
    await new Promise(r => requestAnimationFrame(r));

    // Start cache checking for zilean results
    const zileanHashes = _currentResults.slice(0, PAGE_DATA.cacheCheckLimit).map(r => r.info_hash).filter(Boolean);
    checkCachedProgressive(zileanHashes);

    // Wait for torrentio results and merge
    if (torrentioPromise) {
      showMoreResultsIndicator(true);

      const torrentioData = await torrentioPromise;
      showMoreResultsIndicator(false);

      if (torrentioData && torrentioData.results && torrentioData.results.length > 0) {
        // Dedup by info_hash
        const existingHashes = new Set(_currentResults.map(r => r.info_hash));
        const newResults = torrentioData.results.filter(r => !existingHashes.has(r.info_hash));

        if (newResults.length > 0) {
          // Merge, re-sort by score, re-render
          _currentResults = [..._currentResults, ...newResults].sort((a, b) => (b.score || 0) - (a.score || 0));
          _totalRaw += torrentioData.total_raw || 0;
          _totalFiltered += torrentioData.total_filtered || 0;

          renderResults({ results: _currentResults, total_raw: _totalRaw, total_filtered: _totalFiltered });

          // Reapply cached statuses that were already checked
          reapplyCacheStatuses();

          // Check cache for NEW torrentio hashes only (reuse current generation
          // so we don't cancel the still-running zilean cache loop).
          const newHashes = newResults.slice(0, PAGE_DATA.cacheCheckLimit).map(r => r.info_hash).filter(Boolean);
          checkCachedProgressive(newHashes, _cacheCheckGeneration);
        }
      }
    }

  } catch (err) {
    showResultsError(err.message || 'An unexpected error occurred.');
  } finally {
    setSearchLoading(false);
    showMoreResultsIndicator(false);
  }
};

function setSearchLoading(loading) {
  const btn     = document.getElementById('search-btn');
  const spinner = document.getElementById('search-spinner');
  const icon    = document.getElementById('search-icon');
  const label   = document.getElementById('search-btn-text');

  btn.disabled = loading;
  spinner.classList.toggle('hidden', !loading);
  icon.classList.toggle('hidden', loading);
  label.textContent = loading ? 'Searching\u2026' : 'Search';
}

function showMoreResultsIndicator(show) {
  let el = document.getElementById('more-results-indicator');
  if (show && !el) {
    el = document.createElement('div');
    el.id = 'more-results-indicator';
    el.className = 'flex items-center gap-2 text-sm text-vd-muted py-2';
    el.innerHTML = '<svg class="w-4 h-4 cache-spinner" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg> Loading more results from Torrentio\u2026';
    const summary = document.getElementById('results-summary');
    if (summary) summary.parentNode.insertBefore(el, summary.nextSibling);
  } else if (!show && el) {
    el.remove();
  }
}

function reapplyCacheStatuses() {
  for (const [hash, cached] of _cacheStatusMap) {
    updateCacheBadge(hash, cached);
  }
}

// ─── Progressive cache checking ────────────────────────────────────────────

// Incremented each search to cancel stale cache-check loops.
let _cacheCheckGeneration = 0;

async function checkCachedProgressive(hashes, existingGen) {
  // existingGen is passed when merging results mid-search so the same
  // generation token is reused and the zilean loop is not cancelled.
  const gen = existingGen !== undefined ? existingGen : ++_cacheCheckGeneration;

  for (const hash of hashes) {
    // Abort if a new search was started.
    if (gen !== _cacheCheckGeneration) return;

    if (_cacheStatusMap.has(hash)) {
      updateCacheBadge(hash, _cacheStatusMap.get(hash));
      continue;
    }

    try {
      const resp = await csrfFetch('/api/check-cached', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ info_hash: hash }),
      });
      if (!resp.ok) continue;
      const data = await resp.json();
      if (gen !== _cacheCheckGeneration) return;
      updateCacheBadge(data.info_hash, data.cached);
    } catch (_) {
      // Silently skip — badge stays as "Checking…"
    }
  }
}

function updateCacheBadge(infoHash, cached) {
  _cacheStatusMap.set(infoHash, cached);
  const card = document.getElementById('result-' + infoHash);
  if (!card) return;

  // Update all cache-badge containers in this card (desktop + mobile).
  card.querySelectorAll('.cache-badge').forEach(el => {
    if (cached) {
      el.innerHTML = `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-green-900/60 border border-green-700/50 text-green-300">
          <span>&#10003;</span>Cached
        </span>`;
    } else {
      el.innerHTML = `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-red-900/60 border border-red-700/50 text-red-300">
          <span>&#10007;</span>Not Cached
        </span>`;
    }
  });

  // Update left border accent.
  if (cached) {
    card.classList.remove('result-uncached');
    card.classList.add('result-cached');
  }

  // Update score: add or confirm the +10 cached bonus.
  const item = _currentResults.find(r => r.info_hash === infoHash);
  if (item && item.score_breakdown) {
    const oldCached = item.score_breakdown.cached || 0;
    const newCached = cached ? 10.0 : 0.0;
    if (oldCached !== newCached) {
      item.score_breakdown.cached = newCached;
      item.score = Object.values(item.score_breakdown).reduce((a, b) => a + b, 0);

      // Update score display in the card.
      const scoreClass = scoreTextColor(item.score);
      const barClass = scoreColorClass(item.score);
      const barWidth = Math.min(100, item.score).toFixed(1);
      const breakdownStr = Object.entries(item.score_breakdown)
        .map(([k, v]) => `${k}: +${v}`)
        .join(' / ');

      card.querySelectorAll('.score-value').forEach(el => {
        el.textContent = item.score.toFixed(1);
        el.className = el.className.replace(/text-(green|yellow|red)-400/g, '');
        el.classList.add(scoreClass);
      });
      card.querySelectorAll('.score-bar-fill').forEach(el => {
        el.className = el.className.replace(/score-bar-(high|mid|low)/g, '');
        el.classList.add(barClass);
        el.style.width = barWidth + '%';
      });
      card.querySelectorAll('.score-breakdown').forEach(el => {
        el.textContent = breakdownStr;
      });
    }
  }
}

// ─── Render results ─────────────────────────────────────────────────────────

function renderResults(data) {
  const section  = document.getElementById('results-section');
  const list     = document.getElementById('results-list');
  const summary  = document.getElementById('results-summary');
  const empty    = document.getElementById('results-empty');
  const errorDiv = document.getElementById('results-error');

  section.classList.remove('hidden');
  errorDiv.classList.add('hidden');
  list.innerHTML = '';

  // Sort by score descending (cache status updates progressively later).
  const results = (data.results || []).slice().sort((a, b) => {
    return (b.score || 0) - (a.score || 0);
  });

  const total    = data.total_filtered ?? results.length;
  const totalRaw = data.total_raw ?? results.length;

  summary.textContent = `Showing ${results.length} of ${total} results (${totalRaw} raw results before filtering)`;

  if (results.length === 0) {
    empty.classList.remove('hidden');
    list.classList.add('hidden');
    return;
  }

  empty.classList.add('hidden');
  list.classList.remove('hidden');

  results.forEach((item, idx) => {
    const card = buildResultCard(item, idx);
    list.appendChild(card);
  });
}

function cacheBadgeHtml(item, idx) {
  // If we already know the cache status (from a previous check), show it directly.
  if (_cacheStatusMap.has(item.info_hash)) {
    const cached = _cacheStatusMap.get(item.info_hash);
    if (cached) {
      return `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-green-900/60 border border-green-700/50 text-green-300">
          <span>&#10003;</span>Cached
        </span>`;
    }
    return `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-red-900/60 border border-red-700/50 text-red-300">
        <span>&#10007;</span>Not Cached
      </span>`;
  }
  // Top PAGE_DATA.cacheCheckLimit results will be checked — show a spinner.
  // The rest show a manual Check RD button.
  if (idx < PAGE_DATA.cacheCheckLimit) {
    return `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-vd-bg border border-vd-border text-vd-muted">
        <svg class="w-3 h-3 cache-spinner" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
        Checking&hellip;
      </span>`;
  }
  return `<button class="check-rd-btn inline-flex items-center gap-1 text-xs font-semibold px-3 py-1 rounded-full bg-blue-600 hover:bg-blue-500 text-white cursor-pointer transition-colors" data-hash="${VD.escapeHtml(item.info_hash)}">
  <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
  Check RD
</button>`;
}

function buildResultCard(item, idx) {
  const wrapper = document.createElement('div');
  wrapper.id = `result-${item.info_hash}`;
  wrapper.className = 'bg-vd-card border border-vd-border rounded-xl overflow-hidden result-uncached';

  const score          = item.score || 0;
  const scoreBarClass  = scoreColorClass(score);
  const scoreTextClass = scoreTextColor(score);
  const barWidth       = Math.min(100, score).toFixed(1);
  const sizeFormatted  = VD.formatBytes(item.size_bytes);

  const breakdown    = item.score_breakdown || {};
  const breakdownStr = Object.entries(breakdown)
    .map(([k, v]) => `${k}: +${v}`)
    .join(' / ');

  // Resolution badge colour
  const resBadgeColor = {
    '2160p': 'bg-purple-700 text-purple-100',
    '1080p': 'bg-blue-700 text-blue-100',
    '720p':  'bg-sky-700 text-sky-100',
  }[item.resolution] || 'bg-gray-700 text-gray-200';

  const badgeHtml = cacheBadgeHtml(item, idx);

  wrapper.innerHTML = `
    <!-- Desktop table-like row -->
    <div class="hidden md:grid md:grid-cols-[1fr_auto_auto_auto_auto_auto_auto] md:items-center gap-4 px-5 py-4">

      <!-- Title + badges -->
      <div class="min-w-0">
        <p class="text-sm text-white font-medium truncate" title="${VD.escapeHtml(item.title)}">${VD.escapeHtml(item.title)}</p>
        <div class="flex items-center gap-2 mt-1 flex-wrap">
          ${item.resolution ? `<span class="inline-block text-xs font-semibold px-1.5 py-0.5 rounded ${resBadgeColor}">${VD.escapeHtml(item.resolution)}</span>` : ''}
          ${item.codec ? `<span class="inline-block text-xs font-semibold px-1.5 py-0.5 rounded bg-vd-bg border border-vd-border text-gray-300">${VD.escapeHtml(item.codec)}</span>` : ''}
          ${item.quality ? `<span class="inline-block text-xs px-1.5 py-0.5 rounded bg-vd-bg border border-vd-border text-vd-muted">${VD.escapeHtml(item.quality)}</span>` : ''}
          ${item.is_season_pack ? `<span class="inline-block text-xs px-1.5 py-0.5 rounded bg-amber-900/60 border border-amber-700/50 text-amber-300">Season Pack</span>` : ''}
        </div>
      </div>

      <!-- Season -->
      <div class="text-center">
        <span class="text-sm text-gray-300 tabular-nums">${item.season != null ? 'S' + String(item.season).padStart(2, '0') : '\u2014'}</span>
      </div>

      <!-- Episode -->
      <div class="text-center">
        ${item.is_season_pack
          ? '<span class="inline-block text-xs font-semibold px-1.5 py-0.5 rounded bg-amber-900/60 border border-amber-700/50 text-amber-300">Full</span>'
          : `<span class="text-sm text-gray-300 tabular-nums">${item.episode != null ? 'E' + String(item.episode).padStart(2, '0') : '\u2014'}</span>`
        }
      </div>

      <!-- Size -->
      <div class="text-right">
        <span class="text-sm text-gray-300 tabular-nums">${VD.escapeHtml(sizeFormatted)}</span>
      </div>

      <!-- Cached badge -->
      <div class="flex justify-center cache-badge">${badgeHtml}</div>

      <!-- Score -->
      <div class="w-24">
        <div class="flex items-center justify-end gap-1.5 mb-1">
          <span class="score-value text-sm font-semibold tabular-nums ${scoreTextClass}">${score.toFixed(1)}</span>
        </div>
        <div class="w-full h-1.5 rounded-full bg-vd-bg overflow-hidden">
          <div class="score-bar-fill h-full rounded-full ${scoreBarClass}" style="width: ${barWidth}%"></div>
        </div>
        ${breakdownStr ? `<p class="text-xs text-vd-muted/60 mt-1 score-breakdown">${VD.escapeHtml(breakdownStr)}</p>` : ''}
      </div>

      <!-- Add / Switch button -->
      <div>
        <button
          class="add-btn inline-flex items-center gap-1.5 text-xs font-semibold px-4 py-2 rounded-lg
                 ${window._switchItemId ? 'bg-blue-600 hover:bg-blue-500' : 'bg-vd-accent hover:bg-vd-accent-hover'} text-white transition-colors
                 focus:outline-none focus:ring-2 focus:ring-vd-accent focus:ring-offset-1 focus:ring-offset-vd-card"
          data-hash="${VD.escapeHtml(item.info_hash)}"
          data-title="${VD.escapeHtml(item.title)}"
          data-season="${item.season}"
          data-episode="${item.episode}"
          data-season-pack="${item.is_season_pack}"
          data-resolution="${VD.escapeAttr(item.resolution || '')}"
          data-codec="${VD.escapeAttr(item.codec || '')}"
          data-quality="${VD.escapeAttr(item.quality || '')}"
          data-size="${item.size_bytes || 0}"
        >
          <svg class="add-icon w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            ${window._switchItemId
              ? '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>'
              : '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>'}
          </svg>
          <svg class="add-spinner w-3.5 h-3.5 spinner hidden" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>
          </svg>
          ${window._switchItemId ? 'Switch' : 'Add'}
        </button>
      </div>
    </div>

    <!-- Mobile card layout -->
    <div class="md:hidden px-4 py-4 space-y-3">
      <!-- Title + badges -->
      <div>
        <p class="text-sm text-white font-medium break-all">${VD.escapeHtml(item.title)}</p>
        <div class="flex items-center gap-2 mt-1.5 flex-wrap">
          ${item.resolution ? `<span class="inline-block text-xs font-semibold px-1.5 py-0.5 rounded ${resBadgeColor}">${VD.escapeHtml(item.resolution)}</span>` : ''}
          ${item.codec ? `<span class="inline-block text-xs font-semibold px-1.5 py-0.5 rounded bg-vd-bg border border-vd-border text-gray-300">${VD.escapeHtml(item.codec)}</span>` : ''}
          ${item.quality ? `<span class="inline-block text-xs px-1.5 py-0.5 rounded bg-vd-bg border border-vd-border text-vd-muted">${VD.escapeHtml(item.quality)}</span>` : ''}
          ${item.is_season_pack ? `<span class="inline-block text-xs px-1.5 py-0.5 rounded bg-amber-900/60 border border-amber-700/50 text-amber-300">Season Pack</span>` : ''}
          <span class="cache-badge">${badgeHtml}</span>
        </div>
      </div>

      <!-- Stats row -->
      <div class="flex items-center gap-4 text-xs text-vd-muted flex-wrap">
        <span>${VD.escapeHtml(sizeFormatted)}</span>
        ${item.season != null ? `<span class="text-gray-300">${'S' + String(item.season).padStart(2, '0')}${item.is_season_pack ? ' Full Season' : (item.episode != null ? 'E' + String(item.episode).padStart(2, '0') : '')}</span>` : ''}
      </div>

      <!-- Score bar -->
      <div>
        <div class="flex items-center justify-between mb-1">
          <span class="text-xs text-vd-muted">Score</span>
          <span class="score-value text-xs font-semibold tabular-nums ${scoreTextClass}">${score.toFixed(1)}</span>
        </div>
        <div class="w-full h-1.5 rounded-full bg-vd-bg overflow-hidden">
          <div class="score-bar-fill h-full rounded-full ${scoreBarClass}" style="width: ${barWidth}%"></div>
        </div>
        ${breakdownStr ? `<p class="text-xs text-vd-muted/60 mt-1 score-breakdown">${VD.escapeHtml(breakdownStr)}</p>` : ''}
      </div>

      <!-- Add / Switch button -->
      <button
        class="add-btn w-full inline-flex items-center justify-center gap-1.5 text-sm font-semibold px-4 py-2.5 rounded-lg
               ${window._switchItemId ? 'bg-blue-600 hover:bg-blue-500' : 'bg-vd-accent hover:bg-vd-accent-hover'} text-white transition-colors
               focus:outline-none focus:ring-2 focus:ring-vd-accent"
        data-hash="${VD.escapeHtml(item.info_hash)}"
        data-title="${VD.escapeHtml(item.title)}"
        data-season="${item.season}"
        data-episode="${item.episode}"
        data-season-pack="${item.is_season_pack}"
        data-resolution="${VD.escapeAttr(item.resolution || '')}"
        data-codec="${VD.escapeAttr(item.codec || '')}"
        data-quality="${VD.escapeAttr(item.quality || '')}"
        data-size="${item.size_bytes || 0}"
      >
        <svg class="add-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          ${window._switchItemId
            ? '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>'
            : '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>'}
        </svg>
        <svg class="add-spinner w-4 h-4 spinner hidden" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>
        </svg>
        ${window._switchItemId ? 'Switch' : 'Add'}
      </button>
    </div>
  `;

  return wrapper;
}

// ─── Add via event delegation ──────────────────────────────────────────────

document.getElementById('results-list').addEventListener('click', function(e) {
  const btn = e.target.closest('.add-btn');
  if (!btn || btn.disabled) return;

  const infoHash = btn.dataset.hash;
  if (!infoHash || !_lastSearchPayload) return;

  // Switch-torrent mode: delegate to switch handler
  if (window._switchItemId) {
    handleSwitch(btn, infoHash);
    return;
  }

  const isSeasonPack = btn.dataset.seasonPack === 'true';
  // Prefer user-supplied season/episode (from the search form) over the
  // PTN-parsed value from the torrent title.  For multi-season packs like
  // "S01-S04 COMPLETE", PTN reports season=1 which would override the user's
  // intent (e.g. "season 2").  Fall back to the button dataset only when the
  // user did not supply a value.
  const userSeason = (_lastSearchPayload.season != null) ? _lastSearchPayload.season : null;
  const btnSeason = btn.dataset.season !== 'null' && btn.dataset.season !== 'undefined' ? parseInt(btn.dataset.season, 10) : null;
  const season = (userSeason !== null) ? userSeason : btnSeason;

  const userEpisode = (_lastSearchPayload.episode != null) ? _lastSearchPayload.episode : null;
  const btnEpisode = isSeasonPack ? null : (btn.dataset.episode !== 'null' && btn.dataset.episode !== 'undefined' ? parseInt(btn.dataset.episode, 10) : null);
  const episode = isSeasonPack ? null : ((userEpisode !== null) ? userEpisode : btnEpisode);

  const releaseTitle = btn.dataset.title || null;

  const payload = {
    magnet_or_hash:    infoHash,
    // Prefer the TMDB-resolved canonical title over the raw search query so
    // the queue item title is accurate.  Fall back to the query string when
    // TMDB resolution was not performed.
    title:             _lastSearchPayload.tmdb_title || _lastSearchPayload.query,
    release_title:     releaseTitle,
    imdb_id:           _lastSearchPayload.imdb_id || null,
    tmdb_id:           _lastSearchPayload.tmdb_id || null,
    tvdb_id:           _lastSearchPayload.tvdb_id || null,
    media_type:        _lastSearchPayload.media_type,
    year:              _lastSearchPayload.year || null,
    season:            season,
    episode:           episode,
    is_season_pack:    isSeasonPack,
    quality_profile:   _lastSearchPayload.quality_profile,
    original_language: window._discoverOriginalLanguage || null,
  };

  handleAdd(btn, infoHash, payload);
});

document.getElementById('results-list').addEventListener('click', function(e) {
  const btn = e.target.closest('.check-rd-btn');
  if (!btn) return;

  const infoHash = btn.dataset.hash;
  if (!infoHash) return;

  // Find the card and replace ALL .cache-badge containers with spinner
  const card = document.getElementById('result-' + infoHash);
  if (!card) return;

  const spinnerHtml = `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-vd-bg border border-vd-border text-vd-muted">
    <svg class="w-3 h-3 cache-spinner" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
    Checking&hellip;
  </span>`;

  card.querySelectorAll('.cache-badge').forEach(el => {
    el.innerHTML = spinnerHtml;
  });

  // Make the API call
  csrfFetch('/api/check-cached', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ info_hash: infoHash }),
  })
    .then(resp => {
      if (!resp.ok) throw new Error('Check failed');
      return resp.json();
    })
    .then(data => {
      updateCacheBadge(data.info_hash, data.cached);
    })
    .catch(() => {
      // Show error badge on failure
      card.querySelectorAll('.cache-badge').forEach(el => {
        el.innerHTML = `<span class="inline-flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full bg-red-900/60 border border-red-700/50 text-red-300">
          <span>&#10007;</span>Error
        </span>`;
      });
    });
});

async function handleAdd(btn, infoHash, payload) {
  // Find all add buttons in this card and disable them
  const card = document.getElementById(`result-${infoHash}`);
  const allAddBtns = card ? card.querySelectorAll('.add-btn') : [btn];

  allAddBtns.forEach(b => { b.disabled = true; });

  // Show spinner on clicked button
  const icon    = btn.querySelector('.add-icon');
  const spinner = btn.querySelector('.add-spinner');
  if (icon)    icon.classList.add('hidden');
  if (spinner) spinner.classList.remove('hidden');

  try {
    const resp = await csrfFetch('/api/add', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });

    let data = {};
    try { data = await resp.json(); } catch(_) {}

    if (!resp.ok) {
      throw new Error(data.detail || data.message || 'Failed to add torrent.');
    }

    // Update all add buttons in the card to reflect outcome
    const isQueued = data.status === 'queued';
    allAddBtns.forEach(b => {
      b.disabled = true;
      b.classList.remove('bg-vd-accent', 'hover:bg-vd-accent-hover');
      b.classList.add(isQueued ? 'bg-yellow-700' : 'bg-green-700', 'cursor-default');
      const bIcon    = b.querySelector('.add-icon');
      const bSpinner = b.querySelector('.add-spinner');
      if (bIcon)    bIcon.classList.add('hidden');
      if (bSpinner) bSpinner.classList.add('hidden');

      // Replace button text node
      const textNodes = [...b.childNodes].filter(n => n.nodeType === Node.TEXT_NODE);
      textNodes.forEach(n => n.remove());

      // Insert icon + label
      const iconSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      iconSvg.setAttribute('class', 'w-3.5 h-3.5 flex-shrink-0');
      iconSvg.setAttribute('fill', 'none');
      iconSvg.setAttribute('stroke', 'currentColor');
      iconSvg.setAttribute('viewBox', '0 0 24 24');
      if (isQueued) {
        // Warning/clock icon for queued state
        iconSvg.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>';
      } else {
        // Checkmark icon for added state
        iconSvg.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/>';
      }
      b.appendChild(iconSvg);

      const label = document.createElement('span');
      label.textContent = isQueued ? 'Queued' : 'Added';
      b.appendChild(label);
    });

    if (isQueued) {
      showToast('RD unavailable \u2014 item queued for retry', 'warning');
    } else {
      const rdId = data.rd_id ? ` (RD: ${data.rd_id})` : '';
      showToast(`Added to Real-Debrid${rdId}`, 'success');
    }

    // If we came from discover, redirect back after a short delay
    if (window._searchReturnTo === 'discover') {
      setTimeout(function() { window.location.href = '/discover'; }, 1500);
    }

  } catch (err) {
    // Restore button on failure
    allAddBtns.forEach(b => { b.disabled = false; });
    if (icon)    icon.classList.remove('hidden');
    if (spinner) spinner.classList.add('hidden');

    showToast(err.message || 'Failed to add torrent.', 'error');
  }
}

// ─── Switch torrent (switch-torrent mode) ───────────────────────────────────

async function handleSwitch(btn, infoHash) {
  const switchItemId = window._switchItemId;
  if (!switchItemId) return;

  const card = document.getElementById('result-' + infoHash);
  const allAddBtns = card ? card.querySelectorAll('.add-btn') : [btn];

  allAddBtns.forEach(b => { b.disabled = true; });

  const icon    = btn.querySelector('.add-icon');
  const spinner = btn.querySelector('.add-spinner');
  if (icon)    icon.classList.add('hidden');
  if (spinner) spinner.classList.remove('hidden');

  const isSeasonPack = btn.dataset.seasonPack === 'true';

  const payload = {
    magnet_or_hash: infoHash,
    release_title:  btn.dataset.title  || null,
    resolution:     btn.dataset.resolution || null,
    size_bytes:     parseInt(btn.dataset.size, 10) || null,
    codec:          btn.dataset.codec  || null,
    quality:        btn.dataset.quality || null,
    is_season_pack: isSeasonPack,
  };

  try {
    const resp = await csrfFetch('/api/queue/' + switchItemId + '/switch-torrent', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });

    let data = {};
    try { data = await resp.json(); } catch(_) {}

    if (!resp.ok) {
      throw new Error(data.detail || data.message || 'Failed to switch torrent.');
    }

    // Mark all buttons in the card as switched
    allAddBtns.forEach(b => {
      b.disabled = true;
      b.classList.remove('bg-blue-600', 'hover:bg-blue-500');
      b.classList.add('bg-green-700', 'cursor-default');
      const bIcon    = b.querySelector('.add-icon');
      const bSpinner = b.querySelector('.add-spinner');
      if (bIcon)    bIcon.classList.add('hidden');
      if (bSpinner) bSpinner.classList.add('hidden');

      const textNodes = [...b.childNodes].filter(n => n.nodeType === Node.TEXT_NODE);
      textNodes.forEach(n => n.remove());

      const iconSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      iconSvg.setAttribute('class', 'w-3.5 h-3.5 flex-shrink-0');
      iconSvg.setAttribute('fill', 'none');
      iconSvg.setAttribute('stroke', 'currentColor');
      iconSvg.setAttribute('viewBox', '0 0 24 24');
      iconSvg.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/>';
      b.appendChild(iconSvg);

      const label = document.createElement('span');
      label.textContent = 'Switched';
      b.appendChild(label);
    });

    showToast('Torrent switched successfully', 'success');
    setTimeout(function() { window.location.href = '/queue'; }, 1500);

  } catch (err) {
    allAddBtns.forEach(b => { b.disabled = false; });
    if (icon)    icon.classList.remove('hidden');
    if (spinner) spinner.classList.add('hidden');
    showToast(err.message || 'Failed to switch torrent.', 'error');
  }
}

// ─── Manual add ─────────────────────────────────────────────────────────────

window.handleManualAdd = async function(event) {
  event.preventDefault();

  const errorEl = document.getElementById('manual-error');
  const input   = document.getElementById('manual-input').value.trim();
  const title   = document.getElementById('manual-title').value.trim();
  const btn     = document.getElementById('manual-btn');
  const spinner = document.getElementById('manual-spinner');

  errorEl.classList.add('hidden');
  errorEl.textContent = '';

  if (!input) {
    errorEl.textContent = 'Please enter a magnet link or info hash.';
    errorEl.classList.remove('hidden');
    document.getElementById('manual-input').focus();
    return;
  }

  btn.disabled = true;
  spinner.classList.remove('hidden');

  const mediaType    = document.getElementById('manual-type').value;
  const manualSeason = document.getElementById('manual-season').value.trim();
  const isSeasonPack = document.getElementById('manual-season-pack').checked;

  const payload = {
    magnet_or_hash:  input,
    title:           title || null,
    imdb_id:         null,
    media_type:      mediaType,
    year:            null,
    season:          manualSeason ? parseInt(manualSeason, 10) : null,
    episode:         null,
    is_season_pack:  isSeasonPack,
    quality_profile: document.getElementById('quality_profile').value,
  };

  try {
    const resp = await csrfFetch('/api/add', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });

    let data = {};
    try { data = await resp.json(); } catch(_) {}

    if (!resp.ok) {
      throw new Error(data.detail || data.message || 'Failed to add torrent.');
    }

    if (data.status === 'queued') {
      showToast('RD unavailable \u2014 item queued for retry', 'warning');
    } else {
      const rdId = data.rd_id ? ` (RD: ${data.rd_id})` : '';
      showToast(`Added to Real-Debrid${rdId}`, 'success');
    }

    // Clear inputs on success
    document.getElementById('manual-input').value = '';
    document.getElementById('manual-title').value = '';
    document.getElementById('manual-type').value = 'movie';
    document.getElementById('manual-show-fields').classList.add('hidden');
    document.getElementById('manual-season').value = '';
    document.getElementById('manual-season-pack').checked = false;

  } catch (err) {
    errorEl.textContent = err.message || 'An unexpected error occurred.';
    errorEl.classList.remove('hidden');
  } finally {
    btn.disabled = false;
    spinner.classList.add('hidden');
  }
};

// ─── Error display helper ────────────────────────────────────────────────────

// ─── Auto-fill from URL params (discover → search flow, switch-torrent flow) ─

// Stores original_language passed in from discover page for threading into add payload
window._discoverOriginalLanguage = null;

// Switch-torrent mode: set when navigated from queue "Browse Alternatives"
window._switchItemId = null;

(function() {
  var params = new URLSearchParams(window.location.search);
  var query = params.get('query');
  if (!query) return;

  // Pre-fill form fields
  document.getElementById('query').value = query;

  var imdbId = params.get('imdb_id');
  if (imdbId) document.getElementById('imdb_id').value = imdbId;

  var mediaType = params.get('media_type');
  if (mediaType) {
    document.getElementById('media_type').value = mediaType;
    window.toggleShowFields(mediaType);
  }

  // Pre-fill season / episode when present (show mode)
  var season = params.get('season');
  if (season) {
    var seasonEl = document.getElementById('season');
    if (seasonEl) seasonEl.value = season;
  }
  var episode = params.get('episode');
  if (episode) {
    var episodeEl = document.getElementById('episode');
    if (episodeEl) episodeEl.value = episode;
  }

  // Store return destination (discover flow)
  var fromPage = params.get('from');
  if (fromPage) window._searchReturnTo = fromPage;

  // Store original_language for threading into add payload
  var origLang = params.get('original_language');
  if (origLang) window._discoverOriginalLanguage = origLang;

  // Switch-torrent mode
  var switchItemId = params.get('switch_item_id');
  if (switchItemId) {
    window._switchItemId = switchItemId;
    _injectSwitchBanner(query);
  }

  // Auto-submit after a short delay (let DOM settle).
  // Triggered by any URL with a query param (discover flow, movie detail, switch flow).
  setTimeout(function() {
    window.handleSearch(new Event('submit'));
  }, 100);
})();

function _injectSwitchBanner(queryText) {
  var resultsSection = document.getElementById('results-section');
  if (!resultsSection) return;
  // Avoid double-injection
  if (document.getElementById('switch-mode-banner')) return;

  var banner = document.createElement('div');
  banner.id = 'switch-mode-banner';
  banner.className = 'flex items-center justify-between gap-4 px-4 py-3 bg-blue-900/40 border border-blue-700/50 rounded-xl text-sm text-blue-200';
  banner.innerHTML =
    '<div class="flex items-center gap-2">' +
      '<svg class="w-4 h-4 flex-shrink-0 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
        '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>' +
      '</svg>' +
      '<span>Switching torrent for: <strong class="text-white">' + VD.escapeHtml(queryText) + '</strong> — pick a replacement below</span>' +
    '</div>' +
    '<a href="/queue" class="flex-shrink-0 text-xs font-medium text-blue-300 hover:text-white underline transition-colors">Cancel</a>';

  // Insert before the results section so it is always visible
  resultsSection.parentNode.insertBefore(banner, resultsSection);
}

function showResultsError(msg) {
  const section  = document.getElementById('results-section');
  const errorDiv = document.getElementById('results-error');
  const errorMsg = document.getElementById('results-error-msg');
  const list     = document.getElementById('results-list');
  const empty    = document.getElementById('results-empty');

  section.classList.remove('hidden');
  list.innerHTML = '';
  list.classList.add('hidden');
  empty.classList.add('hidden');
  errorMsg.textContent = msg;
  errorDiv.classList.remove('hidden');
}

})();
