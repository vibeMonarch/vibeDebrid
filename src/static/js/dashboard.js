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

function applyStats(data) {
  var queue = data.queue || {};
  var health = data.health || {};

  // Queue state cards
  for (var _i = 0, _a = Object.entries(STATE_CONFIG); _i < _a.length; _i++) {
    var state = _a[_i][0], cfg = _a[_i][1];
    var count = queue[state] ?? 0;
    setTextContent(cfg.countId, count);
  }

  // Active processing breakdown
  setTextContent('active-wanted',   queue.wanted   ?? 0);
  setTextContent('active-scraping', queue.scraping ?? 0);
  setTextContent('active-adding',   queue.adding   ?? 0);
  setTextContent('active-checking', queue.checking ?? 0);

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
