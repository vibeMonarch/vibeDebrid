(function() {
'use strict';

// State display configuration
var STATE_CONFIG = {
  unreleased: { countId: 'count-unreleased' },
  wanted:     { countId: 'count-wanted'     },
  scraping:   { countId: 'count-scraping'   },
  adding:     { countId: 'count-adding'     },
  checking:   { countId: 'count-checking'   },
  sleeping:   { countId: 'count-sleeping'   },
  dormant:    { countId: 'count-dormant'    },
  complete:   { countId: 'count-complete'   },
  done:       { countId: 'count-done'       },
};

function setTextContent(id, value) {
  var el = document.getElementById(id);
  if (el) el.textContent = value;
}

function formatRelativeDate(isoDate) {
  if (!isoDate) return '';
  var d = new Date(isoDate + 'T00:00:00');
  var today = new Date();
  today.setHours(0, 0, 0, 0);
  var diffDays = Math.round((d - today) / 86400000);
  if (diffDays === 0) return 'Today';
  if (diffDays === 1) return 'Tomorrow';
  if (diffDays === -1) return 'Yesterday';
  if (diffDays > 0 && diffDays <= 6) return d.toLocaleDateString(undefined, { weekday: 'long' });
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function applyStats(data) {
  var queue = data.queue || {};
  var health = data.health || {};

  // Queue state cards
  for (var _i = 0, _a = Object.entries(STATE_CONFIG); _i < _a.length; _i++) {
    var state = _a[_i][0], cfg = _a[_i][1];
    var count = queue[state] ?? 0;
    setTextContent(cfg.countId, count);
  }

  // Upcoming episodes
  var listEl = document.getElementById('upcoming-list');
  if (listEl) {
    var upcoming = data.upcoming || [];
    if (upcoming.length === 0) {
      listEl.innerHTML = '<div class="p-8 text-center text-sm text-vd-muted">'
        + '<svg class="w-8 h-8 mx-auto mb-2 text-vd-border" fill="none" stroke="currentColor" viewBox="0 0 24 24">'
        + '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>'
        + '</svg>'
        + 'No upcoming episodes</div>';
    } else {
      var html = '';
      upcoming.forEach(function(ep) {
        var badge = '';
        if (ep.season != null && ep.episode != null) {
          badge = 'S' + String(ep.season).padStart(2, '0') + 'E' + String(ep.episode).padStart(2, '0');
        }
        var dateStr = formatRelativeDate(ep.air_date);
        var link = ep.tmdb_id ? '/show/' + VD.escapeAttr(String(ep.tmdb_id)) : '#';
        html += '<a href="' + link + '" class="flex items-center justify-between px-4 py-3 hover:bg-white/5 transition-colors">';
        html += '<div class="flex items-center gap-3 min-w-0">';
        html += '<span class="text-sm text-white truncate">' + VD.escapeHtml(ep.title) + '</span>';
        if (badge) html += '<span class="text-xs text-vd-muted font-mono flex-shrink-0">' + badge + '</span>';
        html += '</div>';
        html += '<span class="text-xs text-vd-muted flex-shrink-0 ml-3">' + VD.escapeHtml(dateStr) + '</span>';
        html += '</a>';
      });
      listEl.innerHTML = html;
    }
  }

  // Health summary cards
  var active = (queue.wanted ?? 0) + (queue.scraping ?? 0)
             + (queue.adding ?? 0) + (queue.checking ?? 0);
  setTextContent('health-active-label', active + ' item' + (active !== 1 ? 's' : ''));

  var total = queue.total ?? 0;
  setTextContent('health-total-label', total + ' item' + (total !== 1 ? 's' : ''));

  var done = (queue.complete ?? 0) + (queue.done ?? 0);
  setTextContent('health-done-label', done + ' item' + (done !== 1 ? 's' : ''));

  // Mount health indicator
  var mountDot   = document.getElementById('health-mount-dot');
  var mountLabel = document.getElementById('health-mount-label');
  if (health.mount_available) {
    mountDot.className   = 'w-3 h-3 rounded-full bg-vd-success flex-shrink-0';
    mountLabel.className = 'text-sm font-medium text-green-400';
    mountLabel.textContent = 'Available';
  } else {
    mountDot.className   = 'w-3 h-3 rounded-full bg-vd-danger flex-shrink-0';
    mountLabel.className = 'text-sm font-medium text-red-400';
    mountLabel.textContent = 'Unavailable';
  }

  // Real-Debrid account health
  var rdDot   = document.getElementById('health-rd-dot');
  var rdLabel = document.getElementById('health-rd-label');
  var rd = health.rd;
  if (rd) {
    var days = rd.days_remaining ?? 0;
    var isPremium = rd.premium_type === 'premium';
    var dotClass, labelClass, labelText;
    if (!isPremium) {
      dotClass   = 'w-3 h-3 rounded-full bg-vd-danger flex-shrink-0';
      labelClass = 'text-sm font-medium text-red-400';
      labelText  = 'Free';
    } else if (days >= 30) {
      dotClass   = 'w-3 h-3 rounded-full bg-vd-success flex-shrink-0';
      labelClass = 'text-sm font-medium text-green-400';
      labelText  = 'Premium \u2014 ' + days + 'd left';
    } else if (days >= 7) {
      dotClass   = 'w-3 h-3 rounded-full bg-vd-warning flex-shrink-0';
      labelClass = 'text-sm font-medium text-yellow-400';
      labelText  = 'Premium \u2014 ' + days + 'd left';
    } else {
      dotClass   = 'w-3 h-3 rounded-full bg-vd-danger flex-shrink-0';
      labelClass = 'text-sm font-medium text-red-400';
      labelText  = days > 0 ? 'Premium \u2014 ' + days + 'd left' : 'Expired';
    }
    rdDot.className   = dotClass;
    rdLabel.className = labelClass;
    rdLabel.textContent = labelText;
  } else {
    rdDot.className     = 'w-3 h-3 rounded-full bg-gray-500 flex-shrink-0';
    rdLabel.className   = 'text-sm font-medium text-gray-400';
    rdLabel.textContent = 'Unavailable';
  }

  // Update refresh timestamp
  var now = new Date();
  var timeStr = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  setTextContent('last-refresh', timeStr);

  // Animate refresh icon briefly
  var icon = document.getElementById('refresh-icon');
  if (icon) {
    icon.classList.add('spinner');
    setTimeout(function() { icon.classList.remove('spinner'); }, 600);
  }
}

window.fetchStats = function() {
  fetch('/api/stats')
    .then(function(res) {
      if (!res.ok) throw new Error('HTTP ' + res.status);
      return res.json();
    })
    .then(applyStats)
    .catch(function(err) {
      console.error('Failed to fetch stats:', err);
      setTextContent('last-refresh', 'fetch failed');
    });
};

// Initial fetch on load
window.fetchStats();

// SSE-driven refresh (primary), fallback poll every 60s
var _dashDebounce = null;
VD_SSE.onStateChange(function() {
  if (_dashDebounce) clearTimeout(_dashDebounce);
  _dashDebounce = setTimeout(window.fetchStats, 1000);
});
setInterval(window.fetchStats, 60000);

})();
