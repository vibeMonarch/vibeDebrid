(function () {
  'use strict';

  const TMDB_ID = PAGE_DATA.tmdbId;
  let _showData = null;

  // Episode browser state
  let _episodeCache = {};      // season_number → SeasonEpisodesResponse
  let _selectedEpisodes = {};  // season_number → Set of episode_numbers
  let _activeSeason = null;    // currently displayed season tab
  let _browserExpanded = false;

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
      // Reset episode browser state on each fresh load
      _episodeCache = {};
      _selectedEpisodes = {};
      _activeSeason = null;
      _browserExpanded = false;
      render(_showData);
      if (_showData.imdb_id) {
        fetchOmdbRatings(_showData.imdb_id);
      }
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

    // Episode browser section
    renderEpisodeBrowser(data.seasons || []);

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
      cb.addEventListener('change', function () {
        updateAddBar();
        syncEpisodeCheckboxesForSeason(s.season_number);
      });

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

  function countSelectedEpisodes() {
    let total = 0;
    for (const seasonStr in _selectedEpisodes) {
      total += _selectedEpisodes[seasonStr].size;
    }
    return total;
  }

  function updateAddBar() {
    const checked = getCheckedSeasons();
    const seasonCount = checked.length;
    const episodeCount = countSelectedEpisodes();
    const subscribe = document.getElementById('subscribe-toggle').checked;

    const countEl = document.getElementById('selection-count');
    const numEl = document.getElementById('count-num');
    const addBtn = document.getElementById('btn-add');

    const parts = [];
    if (seasonCount > 0) {
      parts.push(`${seasonCount} season${seasonCount !== 1 ? 's' : ''}`);
    }
    if (episodeCount > 0) {
      parts.push(`${episodeCount} episode${episodeCount !== 1 ? 's' : ''}`);
    }

    if (parts.length > 0) {
      numEl.textContent = parts.join(', ');
      countEl.classList.remove('hidden');
    } else {
      numEl.textContent = '0';
      countEl.classList.add('hidden');
    }

    const canAdd = seasonCount > 0 || episodeCount > 0 || subscribe;
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

    // Build episodes array from _selectedEpisodes
    const episodes = [];
    for (const seasonStr in _selectedEpisodes) {
      const seasonNum = parseInt(seasonStr, 10);
      const epSet = _selectedEpisodes[seasonStr];
      epSet.forEach(function (epNum) {
        const cached = _episodeCache[seasonNum];
        const epData = cached ? cached.episodes.find(e => e.episode_number === epNum) : null;
        episodes.push({
          season: seasonNum,
          episode: epNum,
          tmdb_season: epData ? epData.tmdb_season : null,
          tmdb_episode: epData ? epData.tmdb_episode : null,
        });
      });
    }

    if (seasons.length === 0 && episodes.length === 0 && !subscribe) return;

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
        episodes: episodes,
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
      if (result.skipped_episodes > 0) {
        msg += `${result.skipped_episodes} episode${result.skipped_episodes !== 1 ? 's' : ''} already queued. `;
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

  // ---- Episode browser ----

  function renderEpisodeBrowser(seasons) {
    const container = document.getElementById('episode-browser');
    if (!seasons || seasons.length === 0) return;

    // Filter to seasons that have episodes (skip specials season 0 with 0 episodes)
    const browseable = seasons.filter(s => s.season_number > 0 && s.episode_count > 0);
    if (browseable.length === 0) return;

    container.classList.remove('hidden');
    container.innerHTML = '';

    // Section header / toggle
    const header = document.createElement('button');
    header.type = 'button';
    header.className = 'w-full flex items-center justify-between gap-3 py-3 text-left group';
    header.setAttribute('onclick', 'toggleEpisodeBrowser()');
    header.innerHTML =
      '<span class="text-lg font-semibold text-white group-hover:text-vd-accent-hover transition-colors">Browse Episodes</span>' +
      '<svg id="episode-browser-chevron" class="w-5 h-5 text-vd-muted transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
        '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>' +
      '</svg>';

    // Collapsible body
    const body = document.createElement('div');
    body.id = 'episode-browser-body';
    body.className = 'hidden';

    // Season pill row
    const pillRow = document.createElement('div');
    pillRow.className = 'flex items-center gap-2 overflow-x-auto pb-2 mb-4 scrollbar-thin';
    pillRow.id = 'episode-season-pills';

    browseable.forEach(s => {
      const pill = document.createElement('button');
      pill.type = 'button';
      pill.className = 'season-pill flex-shrink-0 px-3 py-1 text-xs font-medium border border-vd-border rounded-full text-vd-muted hover:text-white hover:border-white/40 transition-colors';
      pill.id = 'season-pill-' + s.season_number;
      pill.textContent = 'S' + String(s.season_number).padStart(2, '0');
      pill.setAttribute('onclick', 'selectSeasonTab(' + s.season_number + ')');
      pillRow.appendChild(pill);
    });

    // Episode list area
    const listArea = document.createElement('div');
    listArea.id = 'episode-list-area';

    body.appendChild(pillRow);
    body.appendChild(listArea);

    container.appendChild(header);
    container.appendChild(body);
  }

  window.toggleEpisodeBrowser = function () {
    _browserExpanded = !_browserExpanded;
    const body = document.getElementById('episode-browser-body');
    const chevron = document.getElementById('episode-browser-chevron');
    if (!body) return;
    body.classList.toggle('hidden', !_browserExpanded);
    if (chevron) chevron.style.transform = _browserExpanded ? 'rotate(180deg)' : '';

    // Auto-select first season pill on first open
    if (_browserExpanded && _activeSeason === null && _showData) {
      const first = (_showData.seasons || []).find(s => s.season_number > 0 && s.episode_count > 0);
      if (first) selectSeasonTab(first.season_number);
    }
  };

  window.selectSeasonTab = function (seasonNumber) {
    _activeSeason = seasonNumber;

    // Initialize selection set if needed
    if (!_selectedEpisodes[seasonNumber]) {
      _selectedEpisodes[seasonNumber] = new Set();
    }

    // Update pill active state
    document.querySelectorAll('.season-pill').forEach(p => p.classList.remove('active'));
    const activePill = document.getElementById('season-pill-' + seasonNumber);
    if (activePill) activePill.classList.add('active');

    if (_episodeCache[seasonNumber]) {
      renderEpisodeList(seasonNumber);
    } else {
      fetchSeasonEpisodes(seasonNumber);
    }
  };

  async function fetchSeasonEpisodes(seasonNumber) {
    const listArea = document.getElementById('episode-list-area');
    if (!listArea) return;

    // Loading spinner
    listArea.innerHTML =
      '<div class="flex items-center justify-center py-8 gap-3 text-vd-muted">' +
        '<svg class="spinner w-5 h-5 text-vd-accent" fill="none" viewBox="0 0 24 24">' +
          '<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>' +
          '<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>' +
        '</svg>' +
        '<span class="text-sm">Loading episodes...</span>' +
      '</div>';

    try {
      const resp = await fetch(`/api/show/${TMDB_ID}/season/${seasonNumber}/episodes`);
      if (!resp.ok) {
        throw new Error(resp.statusText || 'Request failed');
      }
      const data = await resp.json();
      _episodeCache[seasonNumber] = data;

      // Only render if this season is still active
      if (_activeSeason === seasonNumber) {
        renderEpisodeList(seasonNumber);
      }
    } catch (e) {
      if (_activeSeason === seasonNumber && listArea) {
        listArea.innerHTML =
          '<div class="py-6 text-center text-sm text-vd-danger">' +
            'Failed to load episodes. ' +
            '<button type="button" onclick="selectSeasonTab(' + seasonNumber + ')" ' +
              'class="underline text-vd-accent hover:text-vd-accent-hover ml-1">Retry</button>' +
          '</div>';
      }
    }
  }

  function isSeasonChecked(seasonNumber) {
    const cb = document.getElementById('season-' + seasonNumber);
    return cb ? cb.checked : false;
  }

  function renderEpisodeList(seasonNumber) {
    const listArea = document.getElementById('episode-list-area');
    if (!listArea) return;

    const cached = _episodeCache[seasonNumber];
    if (!cached || !cached.episodes) return;

    const seasonCovered = isSeasonChecked(seasonNumber);
    const episodes = cached.episodes;

    // Ensure selection set exists
    if (!_selectedEpisodes[seasonNumber]) {
      _selectedEpisodes[seasonNumber] = new Set();
    }

    // Count available (selectable) episodes
    const availableEps = episodes.filter(ep => {
      return ep.status === 'available' || ep.status === 'airing';
    });

    listArea.innerHTML = '';

    // Controls row: Select All / Deselect All
    if (availableEps.length > 0) {
      const controls = document.createElement('div');
      controls.className = 'flex items-center gap-3 mb-3 text-xs';

      if (seasonCovered) {
        const note = document.createElement('span');
        note.className = 'text-vd-muted italic';
        note.textContent = 'Covered by season pack';
        controls.appendChild(note);
      } else {
        const selAll = document.createElement('button');
        selAll.type = 'button';
        selAll.className = 'text-vd-accent hover:text-vd-accent-hover transition-colors';
        selAll.textContent = 'Select all';
        selAll.setAttribute('onclick', 'selectAllEpisodes(' + seasonNumber + ')');

        const sep = document.createElement('span');
        sep.className = 'text-vd-border';
        sep.textContent = '|';

        const deselAll = document.createElement('button');
        deselAll.type = 'button';
        deselAll.className = 'text-vd-accent hover:text-vd-accent-hover transition-colors';
        deselAll.textContent = 'Deselect all';
        deselAll.setAttribute('onclick', 'deselectAllEpisodes(' + seasonNumber + ')');

        controls.appendChild(selAll);
        controls.appendChild(sep);
        controls.appendChild(deselAll);
      }

      listArea.appendChild(controls);
    }

    // Episode rows
    const list = document.createElement('div');
    list.className = 'border border-vd-border rounded-lg overflow-hidden';

    episodes.forEach((ep, idx) => {
      const isStatusDisabled = ep.status === 'in_queue' || ep.status === 'in_library' || ep.status === 'unreleased';
      const isDisabled = isStatusDisabled || seasonCovered;

      const row = document.createElement('div');
      row.className = 'episode-row flex items-center gap-3 px-3 py-2.5' +
        (idx < episodes.length - 1 ? ' border-b border-vd-border' : '') +
        (isDisabled ? ' opacity-50' : '');
      row.id = 'ep-row-' + seasonNumber + '-' + ep.episode_number;

      // Checkbox
      const cbWrap = document.createElement('label');
      cbWrap.className = 'flex items-center flex-shrink-0';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'episode-cb w-4 h-4 accent-indigo-500 rounded';
      cb.dataset.season = seasonNumber;
      cb.dataset.episode = ep.episode_number;
      cb.dataset.tmdbSeason = ep.tmdb_season != null ? ep.tmdb_season : '';
      cb.dataset.tmdbEpisode = ep.tmdb_episode != null ? ep.tmdb_episode : '';
      cb.disabled = isDisabled;
      cb.checked = !isDisabled && _selectedEpisodes[seasonNumber].has(ep.episode_number);
      cb.addEventListener('change', function () {
        const sn = parseInt(this.dataset.season, 10);
        const en = parseInt(this.dataset.episode, 10);
        if (!_selectedEpisodes[sn]) _selectedEpisodes[sn] = new Set();
        if (this.checked) {
          _selectedEpisodes[sn].add(en);
        } else {
          _selectedEpisodes[sn].delete(en);
        }
        updateAddBar();
      });
      cbWrap.appendChild(cb);

      // Episode badge
      const badge = document.createElement('span');
      badge.className = 'flex-shrink-0 w-9 text-center text-xs font-mono font-semibold text-vd-muted bg-vd-card px-1 py-0.5 rounded';
      badge.textContent = 'E' + String(ep.episode_number).padStart(2, '0');

      // Name
      const name = document.createElement('span');
      name.className = 'flex-1 text-sm text-white truncate min-w-0';
      name.textContent = ep.name || `Episode ${ep.episode_number}`;

      // Air date
      const dateEl = document.createElement('span');
      dateEl.className = 'flex-shrink-0 text-xs text-vd-muted hidden sm:block';
      if (ep.air_date) {
        dateEl.textContent = formatAirDate(ep.air_date);
      }

      // Status badge
      const statusBadge = document.createElement('span');
      statusBadge.className = 'flex-shrink-0 text-xs px-2 py-0.5 rounded-full';
      if (ep.status === 'in_library') {
        statusBadge.className += ' bg-green-500/20 text-green-400';
        statusBadge.textContent = 'In Library';
      } else if (ep.status === 'in_queue') {
        statusBadge.className += ' bg-indigo-500/20 text-indigo-400';
        statusBadge.textContent = 'In Queue';
      } else if (ep.status === 'unreleased') {
        statusBadge.className += ' bg-amber-500/20 text-amber-400';
        statusBadge.textContent = 'Upcoming';
      } else {
        // available / airing — no badge, just empty spacer for alignment
        statusBadge.className += ' invisible';
        statusBadge.textContent = 'Available';
      }

      row.appendChild(cbWrap);
      row.appendChild(badge);
      row.appendChild(name);
      row.appendChild(dateEl);
      row.appendChild(statusBadge);
      list.appendChild(row);
    });

    if (episodes.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'py-8 text-center text-sm text-vd-muted';
      empty.textContent = 'No episodes found for this season.';
      list.appendChild(empty);
    }

    listArea.appendChild(list);
  }

  function formatAirDate(isoDate) {
    try {
      const d = new Date(isoDate + 'T12:00:00Z');
      return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
    } catch (e) {
      return isoDate;
    }
  }

  function syncEpisodeCheckboxesForSeason(seasonNumber) {
    // Called when a season checkbox changes state
    const seasonCovered = isSeasonChecked(seasonNumber);
    if (seasonCovered) {
      // Clear episode selections for this season
      if (_selectedEpisodes[seasonNumber]) {
        _selectedEpisodes[seasonNumber].clear();
      }
    }
    // Re-render if this season is currently shown
    if (_activeSeason === seasonNumber && _episodeCache[seasonNumber]) {
      renderEpisodeList(seasonNumber);
    }
    updateAddBar();
  }

  window.selectAllEpisodes = function (seasonNumber) {
    const cached = _episodeCache[seasonNumber];
    if (!cached) return;
    if (!_selectedEpisodes[seasonNumber]) _selectedEpisodes[seasonNumber] = new Set();
    cached.episodes.forEach(ep => {
      if (ep.status === 'available' || ep.status === 'airing') {
        _selectedEpisodes[seasonNumber].add(ep.episode_number);
      }
    });
    renderEpisodeList(seasonNumber);
    updateAddBar();
  };

  window.deselectAllEpisodes = function (seasonNumber) {
    if (_selectedEpisodes[seasonNumber]) {
      _selectedEpisodes[seasonNumber].clear();
    }
    renderEpisodeList(seasonNumber);
    updateAddBar();
  };

  // ---- OMDb ratings ----

  function fetchOmdbRatings(imdbId) {
    fetch('/api/omdb/' + encodeURIComponent(imdbId))
      .then(function(resp) {
        if (!resp.ok) return null;
        return resp.json();
      })
      .then(function(data) {
        if (data) renderRatings(data);
      })
      .catch(function() {
        // Silently fail — OMDb ratings are optional
      });
  }

  function renderRatings(data) {
    var container = document.getElementById('show-ratings');
    if (!container) return;
    var hasAny = false;

    if (data.imdb_rating != null) {
      document.getElementById('rating-imdb-value').textContent = '\u2605 ' + data.imdb_rating;
      var votesEl = document.getElementById('rating-imdb-votes');
      if (data.imdb_votes) votesEl.textContent = '(' + data.imdb_votes + ')';
      document.getElementById('rating-imdb').classList.remove('hidden');
      hasAny = true;
    }

    if (data.rt_score != null) {
      document.getElementById('rating-rt-icon').textContent = data.rt_score >= 60 ? '\uD83C\uDF45' : '\uD83E\uDD22';
      document.getElementById('rating-rt-value').textContent = data.rt_score + '%';
      document.getElementById('rating-rt').classList.remove('hidden');
      hasAny = true;
    }

    if (data.metascore != null) {
      var badge = document.getElementById('rating-meta-badge');
      badge.textContent = data.metascore;
      if (data.metascore >= 61) {
        badge.className = 'text-xs font-bold px-1.5 py-0.5 rounded bg-green-600 text-white';
      } else if (data.metascore >= 40) {
        badge.className = 'text-xs font-bold px-1.5 py-0.5 rounded bg-yellow-500 text-black';
      } else {
        badge.className = 'text-xs font-bold px-1.5 py-0.5 rounded bg-red-600 text-white';
      }
      document.getElementById('rating-meta').classList.remove('hidden');
      hasAny = true;
    }

    if (hasAny) container.classList.remove('hidden');
  }

  // Kick off load
  loadShow();
}());
