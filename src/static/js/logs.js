/**
 * Logs viewer page — fetches, renders, and auto-refreshes application log lines.
 *
 * PAGE_DATA expected shape:
 *   { defaultLines: number }
 */

(function () {
  'use strict';

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  var _autoRefreshTimer = null;
  var _autoRefreshEnabled = false;

  // ---------------------------------------------------------------------------
  // DOM references (resolved after DOMContentLoaded)
  // ---------------------------------------------------------------------------
  var _logContainer = null;
  var _levelSelect = null;
  var _linesInput = null;
  var _refreshBtn = null;
  var _autoToggleBtn = null;
  var _statusBar = null;

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Return the Tailwind text-color class for a log line based on its level.
   * Falls back to dim gray for DEBUG or unrecognised lines.
   *
   * @param {string} line - Raw log line text.
   * @returns {string} Tailwind class string.
   */
  function _levelClass(line) {
    if (line.indexOf('| ERROR') !== -1 || line.indexOf('ERROR:') !== -1) {
      return 'text-red-400';
    }
    if (line.indexOf('| WARNING') !== -1 || line.indexOf('WARNING:') !== -1) {
      return 'text-yellow-400';
    }
    if (line.indexOf('| INFO') !== -1 || line.indexOf('INFO:') !== -1) {
      return 'text-gray-300';
    }
    if (line.indexOf('| DEBUG') !== -1 || line.indexOf('DEBUG:') !== -1) {
      return 'text-gray-500';
    }
    return 'text-gray-400';
  }

  /**
   * Build the query string for the /api/logs endpoint using current control values.
   *
   * @returns {string} URL with query parameters.
   */
  function _buildUrl() {
    var lines = parseInt(_linesInput.value, 10) || 500;
    var level = _levelSelect.value;
    var url = '/api/logs?lines=' + encodeURIComponent(lines);
    if (level) {
      url += '&level=' + encodeURIComponent(level);
    }
    return url;
  }

  /**
   * Render an array of log-line strings into the log container.
   * Each line is a `<div>` with color coding applied via _levelClass().
   * Scrolls to the bottom after render.
   *
   * @param {string[]} lines - Log lines to display.
   */
  function _renderLines(lines) {
    if (!lines || lines.length === 0) {
      _logContainer.innerHTML =
        '<div class="text-vd-muted italic text-xs p-4">No log lines found.</div>';
      return;
    }

    var html = '';
    for (var i = 0; i < lines.length; i++) {
      var cls = _levelClass(lines[i]);
      html += '<div class="' + cls + ' leading-5">' + VD.escapeHtml(lines[i]) + '</div>';
    }
    _logContainer.innerHTML = html;

    // Auto-scroll to bottom
    _logContainer.scrollTop = _logContainer.scrollHeight;
  }

  /**
   * Set the status bar text.
   *
   * @param {string} msg - Message to display.
   */
  function _setStatus(msg) {
    if (_statusBar) _statusBar.textContent = msg;
  }

  // ---------------------------------------------------------------------------
  // Fetch and refresh
  // ---------------------------------------------------------------------------

  /**
   * Fetch logs from the API and render them.
   * Updates the status bar with a timestamp on success, or an error message on failure.
   */
  function fetchLogs() {
    _setStatus('Loading...');
    var url = _buildUrl();

    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        _renderLines(data.lines || []);
        var now = new Date().toLocaleTimeString();
        _setStatus(
          'Showing ' + (data.total || 0) + ' lines from ' +
          VD.escapeHtml(data.file || 'vibedebrid.log') +
          ' — refreshed at ' + now
        );
      })
      .catch(function (err) {
        _setStatus('Error loading logs: ' + err.message);
        _logContainer.innerHTML =
          '<div class="text-red-400 p-4">Failed to load logs. ' +
          VD.escapeHtml(err.message) + '</div>';
      });
  }

  // ---------------------------------------------------------------------------
  // Auto-refresh
  // ---------------------------------------------------------------------------

  /**
   * Start the 10-second auto-refresh interval.
   * No-op if already running.
   */
  function _startAutoRefresh() {
    if (_autoRefreshTimer) return;
    _autoRefreshTimer = setInterval(fetchLogs, 10000);
  }

  /**
   * Stop the auto-refresh interval.
   */
  function _stopAutoRefresh() {
    if (_autoRefreshTimer) {
      clearInterval(_autoRefreshTimer);
      _autoRefreshTimer = null;
    }
  }

  /**
   * Toggle auto-refresh on/off and update the button label.
   */
  function toggleAutoRefresh() {
    _autoRefreshEnabled = !_autoRefreshEnabled;
    if (_autoRefreshEnabled) {
      _startAutoRefresh();
      _autoToggleBtn.textContent = 'Auto-refresh: ON';
      _autoToggleBtn.classList.remove('bg-vd-bg', 'border-vd-border', 'text-gray-300');
      _autoToggleBtn.classList.add('bg-vd-accent', 'text-white', 'border-vd-accent');
    } else {
      _stopAutoRefresh();
      _autoToggleBtn.textContent = 'Auto-refresh: OFF';
      _autoToggleBtn.classList.remove('bg-vd-accent', 'text-white', 'border-vd-accent');
      _autoToggleBtn.classList.add('bg-vd-bg', 'border-vd-border', 'text-gray-300');
    }
  }

  // ---------------------------------------------------------------------------
  // Download
  // ---------------------------------------------------------------------------

  /**
   * Download all available log lines as a plain-text file.
   * Fetches up to 100,000 lines from the API (no level filter) and creates
   * a Blob download so no server-side file serving is required.
   */
  function downloadLogs() {
    var lines = parseInt(_linesInput.value, 10) || 500;
    var url = '/api/logs?lines=' + Math.max(lines, 10000);

    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        var text = (data.lines || []).join('\n');
        var blob = new Blob([text], { type: 'text/plain' });
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'vibedebrid.log';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) {
        showToast('Download failed: ' + err.message, 'error');
      });
  }

  // ---------------------------------------------------------------------------
  // Expose to window (called from HTML attributes)
  // ---------------------------------------------------------------------------

  window.fetchLogs = fetchLogs;
  window.toggleAutoRefresh = toggleAutoRefresh;
  window.downloadLogs = downloadLogs;

  // ---------------------------------------------------------------------------
  // Init
  // ---------------------------------------------------------------------------

  document.addEventListener('DOMContentLoaded', function () {
    _logContainer = document.getElementById('log-output');
    _levelSelect = document.getElementById('log-level');
    _linesInput = document.getElementById('log-lines');
    _refreshBtn = document.getElementById('refresh-btn');
    _autoToggleBtn = document.getElementById('auto-refresh-btn');
    _statusBar = document.getElementById('log-status');

    // Pre-populate lines input from PAGE_DATA if available
    if (typeof PAGE_DATA !== 'undefined' && PAGE_DATA.defaultLines) {
      _linesInput.value = PAGE_DATA.defaultLines;
    }

    // Initial fetch
    fetchLogs();
  });
})();
