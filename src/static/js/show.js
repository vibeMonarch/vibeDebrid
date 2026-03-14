(function () {
  'use strict';

  const TMDB_ID = PAGE_DATA.tmdbId;
  let _showData = null;

  // ---- Fetch and render ----

  async function loadShow() {
    try {
      const resp = await fetch(`/api/show/${TMDB_ID}`);
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        showError(err.detail || 'Unknown error');
        return;
      }
      _showData = await resp.json();
      render(_showData);
    } catch (e) {
      showError('Network error: ' + e.message);
    }
  }

  function showError(msg) {
    document.getElementById('show-loading').classList.add('hidden');
    const errEl = document.getElementById('show-error');
    document.getElementById('show-error-msg').textContent = msg;
    errEl.classList.remove('hidden');
  }

  function render(data) {
    document.getElementById('show-loading').classList.add('hidden');

    // Hero
    if (data.backdrop_url && data.backdrop_url.startsWith('https://image.tmdb.org/')) {
      document.getElementById('show-backdrop').style.backgroundImage = `url('${data.backdrop_url}')`;
    }
    if (data.poster_url) {
      const img = document.getElementById('show-poster');
      img.src = data.poster_url;
      img.alt = data.title;
      img.classList.remove('hidden');
    }
    document.getElementById('show-title').textContent = data.title;

    if (data.year) {
      document.getElementById('show-year').textContent = data.year;
    }
    if (data.show_status) {
      document.getElementById('show-status-dot').classList.remove('hidden');
      document.getElementById('show-status').textContent = data.show_status;
    }
    if (data.vote_average > 0) {
      document.getElementById('show-rating-dot').classList.remove('hidden');
      document.getElementById('show-rating').textContent = '★ ' + data.vote_average.toFixed(1);
    }

    // Genres
    const genresEl = document.getElementById('show-genres');
    (data.genres || []).forEach(g => {
      const chip = document.createElement('span');
      chip.className = 'px-2 py-0.5 bg-white/10 text-white/70 text-xs rounded-full';
      chip.textContent = g;
      genresEl.appendChild(chip);
    });

    // Overview
    document.getElementById('show-overview').textContent = data.overview || '';

    // Quality profiles
    const qSelect = document.getElementById('quality-select');
    (data.quality_profiles || []).forEach(p => {
      const opt = document.createElement('option');
      opt.value = p;
      opt.textContent = p;
      if (p === data.default_profile) opt.selected = true;
      qSelect.appendChild(opt);
    });

    // Subscribe toggle
    const subToggle = document.getElementById('subscribe-toggle');
    subToggle.checked = data.is_subscribed || false;

    // Seasons grid
    renderSeasons(data.seasons || []);

    // XEM scene-numbering indicator
    const hasXem = (data.seasons || []).some(s => s.xem_mapped);
    document.getElementById('xem-indicator').classList.toggle('hidden', !hasXem);

    document.getElementById('show-content').classList.remove('hidden');
  }

  function statusLabel(status) {
    const labels = {
      available: 'Available',
      in_queue: 'In Queue',
      in_library: 'In Library',
      upcoming: 'Upcoming',
      airing: 'Airing',
    };
    return labels[status] || status;
  }

  function renderSeasons(seasons) {
    const grid = document.getElementById('seasons-grid');
    grid.innerHTML = '';

    seasons.forEach(s => {
      const airingFullyQueued = s.status === 'airing' && s.queue_item_ids && s.queue_item_ids.length >= s.aired_episodes && s.aired_episodes > 0;
      const isDisabled = s.status === 'in_library' || s.status === 'in_queue' || s.status === 'upcoming' || airingFullyQueued;
      const checkId = `season-${s.season_number}`;

      const wrapper = document.createElement('label');
      wrapper.className = 'relative block cursor-pointer';
      if (isDisabled) wrapper.style.cursor = 'default';

      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.id = checkId;
      cb.value = s.season_number;
      cb.className = 'season-checkbox sr-only';
      cb.disabled = isDisabled;
      cb.addEventListener('change', updateAddBar);

      const hasCheckIcon = s.status === 'in_library' || s.status === 'in_queue' || airingFullyQueued;
      const card = document.createElement('div');
      card.className = `season-card border border-vd-border rounded-lg p-4 select-none ${isDisabled ? 'opacity-60' : ''} ${hasCheckIcon ? 'status-' + s.status : ''}`;

      const top = document.createElement('div');
      top.className = 'flex items-start justify-between gap-2 mb-2';

      const titleRow = document.createElement('div');
      titleRow.className = 'flex items-center gap-1.5 min-w-0';

      const title = document.createElement('div');
      title.className = 'font-medium text-white text-sm leading-tight truncate';
      title.textContent = s.name || `Season ${s.season_number}`;

      titleRow.appendChild(title);

      if (hasCheckIcon) {
        const checkSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        checkSvg.setAttribute('class', 'season-check-icon w-3.5 h-3.5 flex-shrink-0');
        checkSvg.setAttribute('fill', 'none');
        checkSvg.setAttribute('viewBox', '0 0 24 24');
        checkSvg.setAttribute('stroke', 'currentColor');
        checkSvg.setAttribute('stroke-width', '2.5');
        const checkPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        checkPath.setAttribute('stroke-linecap', 'round');
        checkPath.setAttribute('stroke-linejoin', 'round');
        checkPath.setAttribute('d', 'M5 13l4 4L19 7');
        checkSvg.appendChild(checkPath);
        titleRow.appendChild(checkSvg);
      }

      const badgeWrap = document.createElement('div');
      badgeWrap.className = 'flex items-center gap-1.5 flex-shrink-0';

      const badge = document.createElement('span');
      badge.className = `text-xs px-2 py-0.5 rounded-full status-badge-${s.status}`;
      badge.textContent = statusLabel(s.status);
      badgeWrap.appendChild(badge);

      if (airingFullyQueued) {
        const monBadge = document.createElement('span');
        monBadge.className = 'text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-400';
        monBadge.textContent = 'Monitoring';
        badgeWrap.appendChild(monBadge);
      }

      top.appendChild(titleRow);
      top.appendChild(badgeWrap);

      const meta = document.createElement('p');
      meta.className = 'text-vd-muted text-xs';
      let epText;
      if (s.status === 'airing' && s.aired_episodes > 0 && s.aired_episodes < s.episode_count) {
        epText = `${s.aired_episodes} of ${s.episode_count} episodes aired`;
      } else if (s.episode_count > 0) {
        epText = `${s.episode_count} episode${s.episode_count !== 1 ? 's' : ''}`;
      } else {
        epText = 'No episodes';
      }
      const airText = s.air_date ? ` · ${s.air_date.substring(0, 4)}` : '';
      meta.textContent = epText + airText;

      card.appendChild(top);
      card.appendChild(meta);

      wrapper.appendChild(cb);
      wrapper.appendChild(card);
      grid.appendChild(wrapper);
    });

    updateAddBar();
  }

  function getCheckedSeasons() {
    return Array.from(document.querySelectorAll('.season-checkbox:checked')).map(
      cb => parseInt(cb.value, 10)
    );
  }

  function updateAddBar() {
    const checked = getCheckedSeasons();
    const count = checked.length;
    const subscribe = document.getElementById('subscribe-toggle').checked;

    const countEl = document.getElementById('selection-count');
    const numEl = document.getElementById('count-num');
    const addBtn = document.getElementById('btn-add');

    numEl.textContent = count;
    countEl.classList.toggle('hidden', count === 0);

    const canAdd = count > 0 || subscribe;
    addBtn.disabled = !canAdd;
  }

  // Bind subscribe toggle to update add bar
  document.getElementById('subscribe-toggle').addEventListener('change', updateAddBar);

  // ---- Selection helpers ----

  window.selectAllSeasons = function () {
    document.querySelectorAll('.season-checkbox:not(:disabled)').forEach(cb => {
      cb.checked = true;
    });
    updateAddBar();
  };

  window.selectAvailableSeasons = function () {
    document.querySelectorAll('.season-checkbox:not(:disabled)').forEach(cb => {
      const val = parseInt(cb.value, 10);
      const season = (_showData?.seasons || []).find(s => s.season_number === val);
      cb.checked = season && (season.status === 'available' || season.status === 'airing');
    });
    updateAddBar();
  };

  // ---- Add to queue ----

  window.addSelectedSeasons = async function () {
    if (!_showData) return;

    const seasons = getCheckedSeasons();
    const subscribe = document.getElementById('subscribe-toggle').checked;
    const quality_profile = document.getElementById('quality-select').value || null;

    if (seasons.length === 0 && !subscribe) return;

    const addBtn = document.getElementById('btn-add');
    addBtn.disabled = true;
    addBtn.textContent = 'Adding...';

    try {
      const payload = {
        tmdb_id: _showData.tmdb_id,
        imdb_id: _showData.imdb_id || null,
        title: _showData.title,
        year: _showData.year || null,
        seasons: seasons,
        quality_profile: quality_profile,
        subscribe: subscribe,
        original_language: _showData.original_language || window._discoverOriginalLanguage || null,
      };

      const resp = await csrfFetch('/api/show/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        alert('Error: ' + (err.detail || 'Failed to add seasons'));
        addBtn.disabled = false;
        addBtn.textContent = 'Add to Queue';
        return;
      }

      const result = await resp.json();
      let msg = '';
      if (result.created_items > 0) {
        if (result.created_episodes > 0 || result.created_unreleased > 0) {
          const parts = [];
          if (result.created_episodes > 0) {
            parts.push(`${result.created_episodes} episode${result.created_episodes !== 1 ? 's' : ''}`);
          }
          if (result.created_unreleased > 0) {
            parts.push(`${result.created_unreleased} upcoming`);
          }
          msg += `Added ${parts.join(' + ')} to queue. `;
        } else {
          msg += `Added ${result.created_items} season${result.created_items !== 1 ? 's' : ''} to queue. `;
        }
      }
      if (result.skipped_seasons.length > 0) {
        msg += `Skipped S${result.skipped_seasons.map(n => String(n).padStart(2, '0')).join(', S')} (already queued). `;
      }
      if (result.subscription_status === 'created') {
        msg += 'Monitoring enabled.';
      } else if (result.subscription_status === 'updated') {
        msg += 'Monitoring updated.';
      }

      addBtn.textContent = 'Added!';
      var fromDiscover = new URLSearchParams(window.location.search).get('from') === 'discover';
      if (fromDiscover) {
        // Return to Discover after brief confirmation
        setTimeout(function () { window.location.href = '/discover'; }, 1500);
      } else {
        setTimeout(function () {
          loadShow().then(function () {
            addBtn.textContent = 'Add to Queue';
          });
        }, 1200);
      }

      if (msg.trim()) {
        // Show a brief toast-style notification
        showToast(msg.trim());
      }

    } catch (e) {
      alert('Network error: ' + e.message);
      addBtn.disabled = false;
      addBtn.textContent = 'Add to Queue';
    }
  };

  // Kick off load
  loadShow();
}());
