/**
 * settings.js — logic for the Settings page.
 *
 * Depends on VD.escapeHtml from utils.js,
 * and on csrfFetch / showToast provided by base.html.
 */
(function () {
  'use strict';

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  let currentSettings = {};

  // ---------------------------------------------------------------------------
  // Collapsible cards
  // ---------------------------------------------------------------------------
  window.toggleCard = function(section) {
    const body    = document.getElementById('body-' + section);
    const chevron = document.getElementById('chevron-' + section);
    body.classList.toggle('collapsed');
    chevron.classList.toggle('rotated');
  };

  // ---------------------------------------------------------------------------
  // Toggle switch helper: read / write checkbox state
  // ---------------------------------------------------------------------------
  function setToggle(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    el.checked = !!value;
  }

  function getToggle(id) {
    const el = document.getElementById(id);
    return el ? el.checked : false;
  }

  // ---------------------------------------------------------------------------
  // Populate form fields from the settings object
  // ---------------------------------------------------------------------------
  function populateForm(s) {
    // Real-Debrid
    const rd = s.real_debrid || {};
    const maskedEl = document.getElementById('rd-api-key-masked');
    if (rd.api_key && maskedEl) {
      maskedEl.textContent = 'Current: ' + rd.api_key + '  — enter a new value to replace it';
    }
    setField('rd-api-url',       rd.api_url             || '');
    setToggle('rd-prefer-cached',  rd.prefer_cached);
    setToggle('rd-allow-uncached', rd.allow_uncached);
    setField('rd-max-uncached',  rd.max_uncached_size_gb != null ? rd.max_uncached_size_gb : '');

    // Scrapers — Torrentio
    const torrentio = (s.scrapers || {}).torrentio || {};
    setToggle('torrentio-enabled', torrentio.enabled);
    setField('torrentio-url',     torrentio.base_url        || '');
    setField('torrentio-timeout', torrentio.timeout_seconds != null ? torrentio.timeout_seconds : '');

    // Scrapers — Zilean
    const zilean = (s.scrapers || {}).zilean || {};
    setToggle('zilean-enabled', zilean.enabled);
    setField('zilean-url',     zilean.base_url        || '');
    setField('zilean-timeout', zilean.timeout_seconds != null ? zilean.timeout_seconds : '');

    // Paths
    const paths = s.paths || {};
    setField('path-zurg-mount',     paths.zurg_mount     || '');
    setField('path-library-movies', paths.library_movies || '');
    setField('path-library-shows',  paths.library_shows  || '');

    // Quality
    const quality = s.quality || {};
    const profiles = quality.profiles || {};
    const profileSelect = document.getElementById('quality-default-profile');
    profileSelect.innerHTML = '';
    Object.keys(profiles).forEach(name => {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;
      if (name === quality.default_profile) opt.selected = true;
      profileSelect.appendChild(opt);
    });
    // Read-only profile summary
    const summaryEl = document.getElementById('quality-profiles-summary');
    summaryEl.innerHTML = '';
    if (Object.keys(profiles).length === 0) {
      summaryEl.innerHTML = '<p class="text-vd-muted text-sm italic">No profiles defined.</p>';
    } else {
      Object.entries(profiles).forEach(([name, cfg]) => {
        const row = document.createElement('div');
        row.className = 'flex items-start gap-3 p-3 bg-vd-bg rounded-lg border border-vd-border';
        const badge = document.createElement('span');
        badge.className = 'text-xs font-mono font-semibold text-vd-accent mt-0.5';
        badge.textContent = name;
        const details = document.createElement('span');
        details.className = 'text-xs text-vd-muted font-mono break-all';
        details.textContent = JSON.stringify(cfg, null, 0).replace(/[{}"]/g, '').replace(/,/g, '  ');
        row.appendChild(badge);
        row.appendChild(details);
        summaryEl.appendChild(row);
      });
    }

    // Filters
    const filters = s.filters || {};
    setField('filters-blocked-keywords',    (filters.blocked_keywords        || []).join(', '));
    setField('filters-blocked-groups',      (filters.blocked_release_groups  || []).join(', '));
    setField('filters-preferred-languages', (filters.preferred_languages      || []).join(', '));
    setToggle('filters-prefer-original-language', filters.prefer_original_language === true);
    setField('filters-dub-penalty',         filters.dub_penalty         != null ? filters.dub_penalty         : 20);
    setField('filters-dual-audio-bonus',    filters.dual_audio_bonus    != null ? filters.dual_audio_bonus    : 10);

    // Retry
    const retry = s.retry || {};
    const scheduleEl = document.getElementById('retry-schedule-display');
    scheduleEl.innerHTML = '';
    (retry.schedule_minutes || []).forEach(min => {
      const pill = document.createElement('span');
      pill.className = 'px-2 py-1 bg-vd-bg border border-vd-border rounded text-xs font-mono text-gray-300';
      pill.textContent = formatMinutes(min);
      scheduleEl.appendChild(pill);
    });
    setField('retry-dormant-days',        retry.dormant_recheck_days       != null ? retry.dormant_recheck_days       : '');
    setField('retry-max-active',          retry.max_active_retries         != null ? retry.max_active_retries         : '');
    setField('checking-timeout-minutes',  retry.checking_timeout_minutes   != null ? retry.checking_timeout_minutes   : '');

    // Symlink Naming
    const sn = s.symlink_naming || {};
    setToggle('sn-plex-naming',  sn.plex_naming === true);
    setToggle('sn-date-prefix',  sn.date_prefix !== false);
    setToggle('sn-release-year', sn.release_year !== false);
    setToggle('sn-resolution',   sn.resolution === true);
    window.updateNamingPreview();

    // Server
    const server = s.server || {};
    setField('server-host', server.host || '');
    setField('server-port', server.port != null ? server.port : '');

    // Search
    const search = s.search || {};
    setField('search-cache-check-limit', search.cache_check_limit != null ? search.cache_check_limit : 10);

    // TMDB
    if (s.tmdb) {
      document.getElementById('tmdb-enabled').checked = s.tmdb.enabled ?? true;
      const tmdbMasked = document.getElementById('tmdb-api-key-masked');
      if (tmdbMasked) tmdbMasked.textContent = s.tmdb.api_key || '';
    }

    // Plex
    if (s.plex) {
      document.getElementById('plex-enabled').checked = s.plex.enabled || false;
      document.getElementById('plex-url').value = s.plex.url || 'http://localhost:32400';
      document.getElementById('plex-token-display').value = s.plex.token || '';
      document.getElementById('plex-scan-after-symlink').checked = s.plex.scan_after_symlink !== false;
      document.getElementById('plex-movie-section-ids').value = (s.plex.movie_section_ids || []).join(',');
      document.getElementById('plex-show-section-ids').value = (s.plex.show_section_ids || []).join(',');
      document.getElementById('plex-watchlist-sync').checked = s.plex.watchlist_sync_enabled || false;
      setField('plex-watchlist-poll', s.plex.watchlist_poll_minutes != null ? s.plex.watchlist_poll_minutes : 30);
      window.updateWatchlistPollVisibility();
    }

    // OMDb
    var omdb = s.omdb || {};
    var omdbMasked = document.getElementById('omdb-api-key-masked');
    if (omdb.api_key && omdbMasked) {
      omdbMasked.textContent = 'Current: ' + omdb.api_key + '  — enter a new value to replace it';
    }
    setToggle('omdb-enabled', omdb.enabled);
    setField('omdb-cache-hours', omdb.cache_hours != null ? omdb.cache_hours : 168);
  }

  function setField(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value;
  }

  function formatMinutes(min) {
    if (min < 60)   return min + 'm';
    if (min < 1440) return (min / 60) + 'h';
    return (min / 1440) + 'd';
  }

  // ---------------------------------------------------------------------------
  // Load settings on page load
  // ---------------------------------------------------------------------------
  async function loadSettings() {
    try {
      const res  = await fetch('/api/settings');
      if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + res.statusText);
      const data = await res.json();
      currentSettings = data.settings || {};
      populateForm(currentSettings);

      // Show all cards, hide loader
      document.getElementById('settings-loading').classList.add('hidden');
      document.querySelectorAll('.settings-card').forEach(el => el.classList.remove('hidden'));
    } catch (err) {
      document.getElementById('settings-loading').innerHTML =
        '<p class="text-red-400 text-sm">Failed to load settings: ' + VD.escapeHtml(err.message) + '</p>';
    }
  }

  // ---------------------------------------------------------------------------
  // Collect payload for each section
  // ---------------------------------------------------------------------------
  function collectSection(section) {
    switch (section) {
      case 'realdebrid': {
        const payload = {
          real_debrid: {
            api_url:             document.getElementById('rd-api-url').value.trim(),
            prefer_cached:       getToggle('rd-prefer-cached'),
            allow_uncached:      getToggle('rd-allow-uncached'),
            max_uncached_size_gb: parseFloat(document.getElementById('rd-max-uncached').value) || 0,
          }
        };
        const newKey = document.getElementById('rd-api-key').value.trim();
        if (newKey) payload.real_debrid.api_key = newKey;
        return payload;
      }
      case 'scrapers': {
        return {
          scrapers: {
            torrentio: {
              enabled:         getToggle('torrentio-enabled'),
              base_url:        document.getElementById('torrentio-url').value.trim(),
              timeout_seconds: parseInt(document.getElementById('torrentio-timeout').value) || 30,
            },
            zilean: {
              enabled:         getToggle('zilean-enabled'),
              base_url:        document.getElementById('zilean-url').value.trim(),
              timeout_seconds: parseInt(document.getElementById('zilean-timeout').value) || 10,
            }
          }
        };
      }
      case 'paths': {
        return {
          paths: {
            zurg_mount:     document.getElementById('path-zurg-mount').value.trim(),
            library_movies: document.getElementById('path-library-movies').value.trim(),
            library_shows:  document.getElementById('path-library-shows').value.trim(),
          }
        };
      }
      case 'quality': {
        return {
          quality: {
            default_profile: document.getElementById('quality-default-profile').value,
          }
        };
      }
      case 'filters': {
        const keywords = document.getElementById('filters-blocked-keywords').value
          .split(',').map(s => s.trim()).filter(Boolean);
        const groups = document.getElementById('filters-blocked-groups').value
          .split(',').map(s => s.trim()).filter(Boolean);
        const preferredLangs = document.getElementById('filters-preferred-languages').value
          .split(',').map(s => s.trim()).filter(Boolean);
        const dubPenalty = parseInt(document.getElementById('filters-dub-penalty').value);
        const dualAudioBonus = parseInt(document.getElementById('filters-dual-audio-bonus').value);
        return {
          filters: {
            blocked_keywords:          keywords,
            blocked_release_groups:    groups,
            preferred_languages:       preferredLangs,
            prefer_original_language:  getToggle('filters-prefer-original-language'),
            dub_penalty:               isNaN(dubPenalty)      ? 20 : Math.min(50, Math.max(0, dubPenalty)),
            dual_audio_bonus:          isNaN(dualAudioBonus)  ? 10 : Math.min(30, Math.max(0, dualAudioBonus)),
          }
        };
      }
      case 'retry': {
        return {
          retry: {
            dormant_recheck_days:     parseInt(document.getElementById('retry-dormant-days').value)          || 7,
            max_active_retries:       parseInt(document.getElementById('retry-max-active').value)            || 10,
            checking_timeout_minutes: parseInt(document.getElementById('checking-timeout-minutes').value)    || 30,
          }
        };
      }
      case 'server': {
        return {
          server: {
            host: document.getElementById('server-host').value.trim(),
            port: parseInt(document.getElementById('server-port').value) || 5100,
          }
        };
      }
      case 'symlink_naming': {
        return {
          symlink_naming: {
            plex_naming:  getToggle('sn-plex-naming'),
            date_prefix:  getToggle('sn-date-prefix'),
            release_year: getToggle('sn-release-year'),
            resolution:   getToggle('sn-resolution'),
          }
        };
      }
      case 'search':
        return {
          search: {
            cache_check_limit: (() => { const v = parseInt(document.getElementById('search-cache-check-limit').value); return isNaN(v) ? 10 : v; })(),
          }
        };
      case 'tmdb': {
        const newKey = document.getElementById('tmdb-api-key').value;
        return {
          tmdb: {
            enabled: document.getElementById('tmdb-enabled').checked,
            ...(newKey ? { api_key: newKey } : {})
          }
        };
      }
      case 'plex': {
        return {
          plex: {
            enabled:                  document.getElementById('plex-enabled').checked,
            url:                      document.getElementById('plex-url').value.trim(),
            // INTENTIONALLY omit token — don't send the masked/display value back to the server
            scan_after_symlink:       document.getElementById('plex-scan-after-symlink').checked,
            movie_section_ids:        document.getElementById('plex-movie-section-ids').value.split(',').filter(Boolean).map(Number),
            show_section_ids:         document.getElementById('plex-show-section-ids').value.split(',').filter(Boolean).map(Number),
            watchlist_sync_enabled:   document.getElementById('plex-watchlist-sync').checked,
            watchlist_poll_minutes:   (() => { const v = parseInt(document.getElementById('plex-watchlist-poll').value); return isNaN(v) || v < 15 ? 30 : v; })(),
          }
        };
      }
      case 'omdb': {
        var key = document.getElementById('omdb-api-key').value.trim();
        var payload = {
          enabled: getToggle('omdb-enabled'),
          cache_hours: parseInt(document.getElementById('omdb-cache-hours').value) || 168,
        };
        if (key) payload.api_key = key;
        return { omdb: payload };
      }
      default:
        return {};
    }
  }

  // ---------------------------------------------------------------------------
  // Save a section
  // ---------------------------------------------------------------------------
  window.saveSection = async function(section) {
    const payload = collectSection(section);
    try {
      const res = await csrfFetch('/api/settings', {
        method:  'PUT',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || 'HTTP ' + res.status);
      }
      showToast('Settings saved.', 'success');
      // Refresh local copy so masked key label updates if api_key was changed
      if (section === 'realdebrid') {
        const refreshed = await fetch('/api/settings');
        if (refreshed.ok) {
          const data = await refreshed.json();
          currentSettings = data.settings || {};
          const rd = currentSettings.real_debrid || {};
          const maskedEl = document.getElementById('rd-api-key-masked');
          if (maskedEl && rd.api_key) {
            maskedEl.textContent = 'Current: ' + rd.api_key + '  — enter a new value to replace it';
          }
          // Clear the password field after a successful save
          document.getElementById('rd-api-key').value = '';
        }
      }
      if (section === 'tmdb') {
        const refreshed = await fetch('/api/settings');
        if (refreshed.ok) {
          const data = await refreshed.json();
          currentSettings = data.settings || {};
          const tmdb = currentSettings.tmdb || {};
          const maskedEl = document.getElementById('tmdb-api-key-masked');
          if (maskedEl) maskedEl.textContent = tmdb.api_key || '';
          // Clear the password field after a successful save
          document.getElementById('tmdb-api-key').value = '';
        }
      }
      if (section === 'omdb') {
        const refreshed = await fetch('/api/settings');
        if (refreshed.ok) {
          const data = await refreshed.json();
          currentSettings = data.settings || {};
          const omdb = currentSettings.omdb || {};
          const maskedEl = document.getElementById('omdb-api-key-masked');
          if (maskedEl && omdb.api_key) {
            maskedEl.textContent = 'Current: ' + omdb.api_key + '  — enter a new value to replace it';
          }
          // Clear the password field after a successful save
          document.getElementById('omdb-api-key').value = '';
        }
      }
    } catch (err) {
      showToast('Save failed: ' + err.message, 'error');
    }
  };

  // ---------------------------------------------------------------------------
  // Connection tests
  // ---------------------------------------------------------------------------
  window.runTest = async function(service, btn) {
    const resultEl = document.getElementById('test-result-' + service);
    const icon     = btn.querySelector('.btn-icon');
    const spinner  = btn.querySelector('.btn-spinner');

    // Show loading state
    btn.disabled = true;
    icon.classList.add('hidden');
    spinner.classList.remove('hidden');
    resultEl.textContent = '';
    resultEl.className   = 'test-result text-sm text-vd-muted';

    try {
      const res  = await csrfFetch('/api/settings/test/' + service, { method: 'POST' });
      const data = await res.json().catch(() => ({}));
      if (res.ok && data.status === 'ok') {
        resultEl.className   = 'test-result text-sm text-green-400 flex items-center gap-1';
        resultEl.innerHTML   = checkIcon() + VD.escapeHtml(data.message || 'Connected');
      } else {
        const msg = data.message || data.detail || ('HTTP ' + res.status);
        resultEl.className   = 'test-result text-sm text-red-400 flex items-center gap-1';
        resultEl.innerHTML   = crossIcon() + VD.escapeHtml(msg);
      }
    } catch (err) {
      resultEl.className   = 'test-result text-sm text-red-400 flex items-center gap-1';
      resultEl.innerHTML   = crossIcon() + VD.escapeHtml(err.message);
    } finally {
      btn.disabled = false;
      icon.classList.remove('hidden');
      spinner.classList.add('hidden');
    }
  };

  function checkIcon() {
    return '<svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
           '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>' +
           '</svg>';
  }

  function crossIcon() {
    return '<svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
           '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"/>' +
           '</svg>';
  }

  // ---------------------------------------------------------------------------
  // Symlink naming live preview
  // ---------------------------------------------------------------------------
  window.updateNamingPreview = function() {
    const preview = document.getElementById('naming-preview');
    if (!preview) return;
    const plexNaming = getToggle('sn-plex-naming');
    const dp = getToggle('sn-date-prefix');
    const ry = getToggle('sn-release-year');
    const res = getToggle('sn-resolution');

    // Dim the other three toggles when plex naming is active
    ['sn-date-prefix', 'sn-release-year', 'sn-resolution'].forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      const row = el.closest('.flex.items-center');
      if (row) {
        row.style.opacity = plexNaming ? '0.4' : '1';
        row.style.pointerEvents = plexNaming ? 'none' : '';
      }
    });

    let movieLine, showLine;

    if (plexNaming) {
      movieLine = 'movies/The Matrix (1999) {tmdb-603}/The Matrix (1999).mkv';
      showLine  = 'shows/Breaking Bad (2008) {tmdb-1396}/Season 01/Breaking Bad (2008) - S01E01.mkv';
    } else {
      const now = new Date();
      const ts = String(now.getFullYear())
        + String(now.getMonth() + 1).padStart(2, '0')
        + String(now.getDate()).padStart(2, '0')
        + String(now.getHours()).padStart(2, '0')
        + String(now.getMinutes()).padStart(2, '0');

      // Movie example
      let movieParts = [];
      if (dp) movieParts.push(ts);
      movieParts.push('The Matrix');
      if (ry) movieParts.push('(1999)');
      if (res) movieParts.push('2160p');
      const movieDir = movieParts.join(' ');

      // Show example
      let showParts = [];
      if (dp) showParts.push(ts);
      showParts.push('Breaking Bad');
      if (ry) showParts.push('(2008)');
      if (res) showParts.push('1080p');
      const showDir = showParts.join(' ');
      const epPrefix = dp ? ts + ' ' : '';

      movieLine = 'movies/' + VD.escapeHtml(movieDir) + '/The.Matrix.1999.2160p.mkv';
      showLine  = 'shows/' + VD.escapeHtml(showDir) + '/Season 01/' + epPrefix + 'Breaking.Bad.S01E01.mkv';
    }

    preview.innerHTML =
      '<div>' + movieLine + '</div>' +
      '<div>' + showLine + '</div>';
  };

  // ---------------------------------------------------------------------------
  // Plex watchlist sync visibility
  // ---------------------------------------------------------------------------
  window.updateWatchlistPollVisibility = function() {
    const enabled = document.getElementById('plex-watchlist-sync')?.checked;
    const row = document.getElementById('plex-watchlist-poll-row');
    const input = document.getElementById('plex-watchlist-poll');
    if (!row || !input) return;
    row.style.opacity = enabled ? '1' : '0.4';
    row.style.pointerEvents = enabled ? '' : 'none';
    input.disabled = !enabled;
  };

  // ---------------------------------------------------------------------------
  // Plex authentication + library helpers
  // ---------------------------------------------------------------------------
  window.startPlexAuth = async function() {
    try {
      const resp = await csrfFetch('/api/settings/plex/auth/start', { method: 'POST' });
      if (!resp.ok) throw new Error('Failed to start auth');
      const data = await resp.json();

      // Open popup for Plex auth
      const popup = window.open(data.auth_url, 'plex_auth', 'width=800,height=600');

      // Poll for completion every 2s
      const pollInterval = setInterval(async () => {
        if (popup && popup.closed) {
          clearInterval(pollInterval);
          return;
        }
        try {
          const checkResp = await csrfFetch(`/api/settings/plex/auth/check/${data.pin_id}`, { method: 'POST' });
          const checkData = await checkResp.json();
          if (checkData.status === 'authenticated') {
            clearInterval(pollInterval);
            if (popup && !popup.closed) popup.close();
            document.getElementById('plex-token-display').value = checkData.token_masked;
            showToast('Plex authenticated successfully!', 'success');
          }
        } catch (e) {
          console.warn('Plex auth poll error:', e);
        }
      }, 2000);

      // Stop polling after 5 minutes
      setTimeout(() => {
        clearInterval(pollInterval);
      }, 300000);
    } catch (e) {
      showToast('Failed to start Plex auth: ' + e.message, 'error');
    }
  };

  window.fetchPlexLibraries = async function() {
    try {
      const resp = await fetch('/api/settings/plex/libraries');
      if (!resp.ok) throw new Error('Failed to fetch libraries');
      const data = await resp.json();
      syncPlexSectionCheckboxes(data.sections);
    } catch (e) {
      showToast('Failed to fetch Plex libraries: ' + e.message, 'error');
    }
  };

  function syncPlexSectionCheckboxes(sections) {
    const container = document.getElementById('plex-libraries-container');
    if (!container) return;

    // Get currently selected IDs from the hidden inputs
    const movieIds = (document.getElementById('plex-movie-section-ids')?.value || '').split(',').filter(Boolean).map(Number);
    const showIds  = (document.getElementById('plex-show-section-ids')?.value  || '').split(',').filter(Boolean).map(Number);

    const movieSections = sections.filter(s => s.type === 'movie');
    const showSections  = sections.filter(s => s.type === 'show');

    let html = '';

    if (movieSections.length > 0) {
      html += '<div class="mb-3"><p class="text-sm font-medium text-gray-300 mb-2">Movie Libraries</p>';
      for (const s of movieSections) {
        const checked = movieIds.includes(s.section_id) ? 'checked' : '';
        html += `<label class="flex items-center gap-2 text-sm text-gray-300 mb-1 cursor-pointer">
          <input type="checkbox" class="plex-movie-section" value="${s.section_id}" ${checked}
                 onchange="updatePlexSectionIds()" />
          ${VD.escapeHtml(s.title)} <span class="text-vd-muted text-xs">(ID: ${s.section_id})</span>
        </label>`;
      }
      html += '</div>';
    }

    if (showSections.length > 0) {
      html += '<div class="mb-3"><p class="text-sm font-medium text-gray-300 mb-2">TV Show Libraries</p>';
      for (const s of showSections) {
        const checked = showIds.includes(s.section_id) ? 'checked' : '';
        html += `<label class="flex items-center gap-2 text-sm text-gray-300 mb-1 cursor-pointer">
          <input type="checkbox" class="plex-show-section" value="${s.section_id}" ${checked}
                 onchange="updatePlexSectionIds()" />
          ${VD.escapeHtml(s.title)} <span class="text-vd-muted text-xs">(ID: ${s.section_id})</span>
        </label>`;
      }
      html += '</div>';
    }

    if (!html) {
      html = '<p class="text-sm text-vd-muted">No libraries found.</p>';
    }

    container.innerHTML = html;
  }

  window.updatePlexSectionIds = function() {
    const movieIds = [...document.querySelectorAll('.plex-movie-section:checked')].map(el => el.value);
    const showIds  = [...document.querySelectorAll('.plex-show-section:checked')].map(el => el.value);
    document.getElementById('plex-movie-section-ids').value = movieIds.join(',');
    document.getElementById('plex-show-section-ids').value  = showIds.join(',');
  };

  // ---------------------------------------------------------------------------
  // Boot
  // ---------------------------------------------------------------------------
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadSettings);
  } else {
    loadSettings();
  }
})();
