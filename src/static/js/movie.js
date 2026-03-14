(function () {
  'use strict';

  var TMDB_ID = PAGE_DATA.tmdbId;
  var _movieData = null;

  // ---- Fetch and render ----

  async function loadMovie() {
    try {
      var resp = await fetch('/api/movie/' + TMDB_ID);
      if (!resp.ok) {
        var err = await resp.json().catch(function () { return { detail: resp.statusText }; });
        showError(err.detail || 'Unknown error');
        return;
      }
      _movieData = await resp.json();
      render(_movieData);
      if (_movieData.imdb_id) {
        fetchOmdbRatings(_movieData.imdb_id);
      }
    } catch (e) {
      showError('Network error: ' + e.message);
    }
  }

  function showError(msg) {
    document.getElementById('movie-loading').classList.add('hidden');
    document.getElementById('movie-error-msg').textContent = msg;
    document.getElementById('movie-error').classList.remove('hidden');
  }

  function render(data) {
    document.getElementById('movie-loading').classList.add('hidden');

    // Backdrop
    if (data.backdrop_url && data.backdrop_url.startsWith('https://image.tmdb.org/')) {
      document.getElementById('movie-backdrop').style.backgroundImage = "url('" + data.backdrop_url + "')";
    }

    // Poster
    if (data.poster_url) {
      var posterImg = document.getElementById('movie-poster');
      posterImg.src = data.poster_url;
      posterImg.alt = data.title || '';
      posterImg.classList.remove('hidden');
    }

    // Title
    document.getElementById('movie-title').textContent = data.title || '';

    // Year
    if (data.year) {
      document.getElementById('movie-year').textContent = data.year;
    }

    // Runtime
    if (data.runtime) {
      var h = Math.floor(data.runtime / 60);
      var m = data.runtime % 60;
      document.getElementById('movie-runtime').textContent = (h > 0 ? h + 'h ' : '') + m + 'm';
      document.getElementById('movie-runtime-dot').classList.remove('hidden');
    }

    // TMDB rating
    if (data.vote_average > 0) {
      document.getElementById('movie-rating-dot').classList.remove('hidden');
      document.getElementById('movie-rating').textContent = '\u2605 ' + data.vote_average.toFixed(1);
    }

    // Genres — API may return [{id, name}] objects or plain strings
    var genresEl = document.getElementById('movie-genres');
    (data.genres || []).forEach(function (g) {
      var chip = document.createElement('span');
      chip.className = 'px-2 py-0.5 bg-white/10 text-white/70 text-xs rounded-full';
      chip.textContent = (typeof g === 'object' && g !== null) ? (g.name || '') : g;
      genresEl.appendChild(chip);
    });

    // Overview
    document.getElementById('movie-overview').textContent = data.overview || '';

    // Show Add to Queue button
    document.getElementById('movie-add-btn').classList.remove('hidden');

    document.getElementById('movie-content').classList.remove('hidden');
  }

  // ---- OMDb ratings ----

  function fetchOmdbRatings(imdbId) {
    fetch('/api/omdb/' + encodeURIComponent(imdbId))
      .then(function (resp) {
        if (!resp.ok) return null;
        return resp.json();
      })
      .then(function (data) {
        if (data) renderRatings(data);
      })
      .catch(function () {
        // Silently fail — OMDb ratings are optional
      });
  }

  function renderRatings(data) {
    var container = document.getElementById('movie-ratings');
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

  // ---- Add to queue ----

  window.addMovie = async function () {
    if (!_movieData) return;
    var btn = document.getElementById('movie-add-btn');
    btn.disabled = true;
    btn.textContent = 'Loading...';

    try {
      var params = new URLSearchParams({
        query:    _movieData.title || '',
        media_type: 'movie',
      });
      if (_movieData.year)    params.set('year',    String(_movieData.year));
      if (_movieData.imdb_id) params.set('imdb_id', _movieData.imdb_id);
      if (TMDB_ID)            params.set('tmdb_id', String(TMDB_ID));
      if (_movieData.original_language) params.set('original_language', _movieData.original_language);

      // Carry forward the `from` param so search page can navigate back correctly
      var fromParam = new URLSearchParams(window.location.search).get('from');
      if (fromParam) params.set('from', fromParam);

      window.location.href = '/search?' + params.toString();
    } catch (e) {
      btn.disabled = false;
      btn.textContent = 'Add to Queue';
      showToast('Error: ' + e.message, 'error');
    }
  };

  // Kick off load
  loadMovie();
}());
