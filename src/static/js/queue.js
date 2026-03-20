window.queuePage = function() {
  return {
    // State
    items: [],
    totalItems: 0,
    currentPage: 1,
    pageSize: 50,
    totalPages: 1,
    loading: false,
    expandedId: null,
    detail: null,
    detailLoading: false,
    selectedIds: new Set(),
    bulkRemoving: false,
    _sseUnsub: null,
    _sseDebounce: null,

    // Filters (live model, applied on button press)
    filters: {
      state: '',
      media_type: '',
      title: '',
    },
    // Applied filters (synced to URL)
    applied: {
      state: '',
      media_type: '',
      title: '',
    },

    // ------------------------------------------------------------------ //
    // Lifecycle
    // ------------------------------------------------------------------ //
    init() {
      this.readFiltersFromUrl();
      this.loadItems();
      // Subscribe to SSE for live updates
      this._sseUnsub = VD_SSE.onStateChange((data) => this.handleSSE(data));
    },

    destroy() {
      if (this._sseUnsub) this._sseUnsub();
      if (this._sseDebounce) clearTimeout(this._sseDebounce);
    },

    // ------------------------------------------------------------------ //
    // URL param helpers
    // ------------------------------------------------------------------ //
    readFiltersFromUrl() {
      const p = new URLSearchParams(window.location.search);
      this.filters.state      = p.get('state')      || '';
      this.filters.media_type = p.get('media_type') || '';
      this.filters.title      = p.get('title')      || '';
      this.applied = { ...this.filters };
      this.currentPage = parseInt(p.get('page') || '1', 10);
    },

    pushUrl() {
      const p = new URLSearchParams();
      if (this.applied.state)      p.set('state',      this.applied.state);
      if (this.applied.media_type) p.set('media_type', this.applied.media_type);
      if (this.applied.title)      p.set('title',      this.applied.title);
      if (this.currentPage > 1)    p.set('page',       this.currentPage);
      const qs = p.toString();
      window.history.replaceState({}, '', qs ? '?' + qs : window.location.pathname);
    },

    // ------------------------------------------------------------------ //
    // Computed
    // ------------------------------------------------------------------ //
    get allSelected() {
      return this.items.length > 0 && this.items.every(i => this.selectedIds.has(i.id));
    },

    get someSelected() {
      return this.selectedIds.size > 0 && !this.allSelected;
    },

    get activeFilters() {
      const out = [];
      if (this.applied.state)      out.push({ key: 'state',      label: 'State: ' + this.applied.state });
      if (this.applied.media_type) out.push({ key: 'media_type', label: 'Type: '  + this.applied.media_type });
      if (this.applied.title)      out.push({ key: 'title',      label: 'Title: '  + this.applied.title });
      return out;
    },

    get pageRange() {
      // Produce an array like [1, 2, '...', 8, 9, 10] for pagination display
      const total = this.totalPages;
      const cur   = this.currentPage;
      if (total <= 7) {
        return Array.from({ length: total }, (_, i) => i + 1);
      }
      const pages = new Set([1, total, cur]);
      for (let d = -1; d <= 1; d++) {
        const n = cur + d;
        if (n > 0 && n <= total) pages.add(n);
      }
      const sorted = Array.from(pages).sort((a, b) => a - b);
      const result = [];
      let prev = 0;
      for (const p of sorted) {
        if (p - prev > 1) result.push('...');
        result.push(p);
        prev = p;
      }
      return result;
    },

    // ------------------------------------------------------------------ //
    // Data loading
    // ------------------------------------------------------------------ //
    async loadItems() {
      this.loading = true;
      try {
        const p = new URLSearchParams({ page: this.currentPage, page_size: this.pageSize });
        if (this.applied.state)      p.set('state',      this.applied.state);
        if (this.applied.media_type) p.set('media_type', this.applied.media_type);
        if (this.applied.title)      p.set('title',      this.applied.title);

        const res = await fetch('/api/queue?' + p.toString());
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();

        this.items      = data.items  || [];
        this.totalItems = data.total  || 0;
        this.totalPages = Math.max(1, Math.ceil(this.totalItems / this.pageSize));
        this.pushUrl();
      } catch (err) {
        showToast('Failed to load queue: ' + err.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    // ------------------------------------------------------------------ //
    // SSE live updates
    // ------------------------------------------------------------------ //
    handleSSE(data) {
      // Find the item in the current view
      const idx = this.items.findIndex(i => i.id === data.item_id);
      if (idx !== -1) {
        // Check if item should still be visible with current filters
        if (this.applied.state && data.new_state !== this.applied.state) {
          // Item moved out of the filtered state — remove from view
          this.items.splice(idx, 1);
          this.totalItems = Math.max(0, this.totalItems - 1);
          this.totalPages = Math.max(1, Math.ceil(this.totalItems / this.pageSize));
        } else {
          // Update in place
          this.items[idx].state = data.new_state;
          this.items[idx].retry_count = data.retry_count;
        }
      }
      // Debounced full reload to catch pagination/count drift
      if (this._sseDebounce) clearTimeout(this._sseDebounce);
      this._sseDebounce = setTimeout(() => this.loadItems(), 2000);
    },

    async loadDetail(id) {
      if (this.detail && this.detail.id === id) return; // already loaded
      this.detailLoading = true;
      this.detail = null;
      try {
        const res = await fetch('/api/queue/' + id);
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();
        this.detail = { ...data.item, scrape_logs: data.scrape_logs, torrents: data.torrents };
      } catch (err) {
        showToast('Failed to load item detail: ' + err.message, 'error');
      } finally {
        this.detailLoading = false;
      }
    },

    // ------------------------------------------------------------------ //
    // Filters
    // ------------------------------------------------------------------ //
    applyFilters() {
      this.applied = { ...this.filters };
      this.currentPage = 1;
      this.expandedId = null;
      this.detail = null;
      this.clearSelection();
      this.loadItems();
    },

    clearFilters() {
      this.filters = { state: '', media_type: '', title: '' };
      this.applyFilters();
    },

    removeFilter(key) {
      this.filters[key]  = '';
      this.applied[key]  = '';
      this.currentPage = 1;
      this.loadItems();
    },

    // ------------------------------------------------------------------ //
    // Pagination
    // ------------------------------------------------------------------ //
    goToPage(p) {
      if (p < 1 || p > this.totalPages) return;
      this.currentPage = p;
      this.expandedId = null;
      this.detail = null;
      this.loadItems();
    },

    // ------------------------------------------------------------------ //
    // Row expansion
    // ------------------------------------------------------------------ //
    toggleExpand(id, event) {
      // Don't expand when clicking inside the actions cell or checkboxes
      if (event && event.target.closest('button, input, a')) return;
      if (this.expandedId === id) {
        this.expandedId = null;
        this.detail = null;
      } else {
        this.expandedId = id;
        this.loadDetail(id);
      }
    },

    // ------------------------------------------------------------------ //
    // Selection
    // ------------------------------------------------------------------ //
    toggleSelect(id) {
      if (this.selectedIds.has(id)) {
        this.selectedIds.delete(id);
      } else {
        this.selectedIds.add(id);
      }
      // Force Alpine reactivity on Set mutation
      this.selectedIds = new Set(this.selectedIds);
    },

    toggleSelectAll(event) {
      if (event.target.checked) {
        this.selectedIds = new Set(this.items.map(i => i.id));
      } else {
        this.selectedIds = new Set();
      }
    },

    clearSelection() {
      this.selectedIds = new Set();
    },

    // ------------------------------------------------------------------ //
    // Row actions
    // ------------------------------------------------------------------ //
    async retryItem(item) {
      try {
        const res = await csrfFetch('/api/queue/' + item.id + '/retry', { method: 'POST' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        showToast('"' + item.title + '" queued for retry', 'success');
        await this.loadItems();
        // Refresh detail if this item is expanded
        if (this.expandedId === item.id) {
          this.detail = null;
          await this.loadDetail(item.id);
        }
      } catch (err) {
        showToast('Retry failed: ' + err.message, 'error');
      }
    },

    async removeItem(item) {
      if (!confirm('Remove "' + item.title + '" from the queue? This also removes associated data.')) return;
      try {
        const res = await csrfFetch('/api/queue/' + item.id, { method: 'DELETE' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        showToast('"' + item.title + '" removed', 'success');
        if (this.expandedId === item.id) { this.expandedId = null; this.detail = null; }
        this.selectedIds.delete(item.id);
        this.selectedIds = new Set(this.selectedIds);
        await this.loadItems();
      } catch (err) {
        showToast('Remove failed: ' + err.message, 'error');
      }
    },

    // ------------------------------------------------------------------ //
    // Bulk actions (JSON POST via fetch)
    // ------------------------------------------------------------------ //
    async bulkRetry() {
      const ids = Array.from(this.selectedIds);
      if (ids.length === 0) return;
      try {
        const res = await csrfFetch('/api/queue/bulk/retry', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ids }),
        });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        showToast(ids.length + ' item' + (ids.length !== 1 ? 's' : '') + ' queued for retry', 'success');
        this.clearSelection();
        await this.loadItems();
      } catch (err) {
        showToast('Bulk retry failed: ' + err.message, 'error');
      }
    },

    async bulkRemove() {
      const ids = Array.from(this.selectedIds);
      if (ids.length === 0) return;
      if (!confirm('Remove ' + ids.length + ' item' + (ids.length !== 1 ? 's' : '') + '? This cannot be undone.')) return;
      this.bulkRemoving = true;
      try {
        const res = await csrfFetch('/api/queue/bulk/remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ids }),
        });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        showToast(ids.length + ' item' + (ids.length !== 1 ? 's' : '') + ' removed', 'success');
        this.clearSelection();
        if (this.expandedId && ids.includes(this.expandedId)) {
          this.expandedId = null;
          this.detail = null;
        }
        await this.loadItems();
      } catch (err) {
        showToast('Bulk remove failed: ' + err.message, 'error');
      } finally {
        this.bulkRemoving = false;
      }
    },

    // ------------------------------------------------------------------ //
    // Formatting helpers
    // ------------------------------------------------------------------ //
    formatDate(iso) {
      if (!iso) return '\u2014';
      try {
        const d = new Date(iso);
        if (isNaN(d)) return iso;
        const now = new Date();
        const diff = now - d; // ms
        if (diff < 60_000)           return 'just now';
        if (diff < 3_600_000)        return Math.floor(diff / 60_000) + 'm ago';
        if (diff < 86_400_000)       return Math.floor(diff / 3_600_000) + 'h ago';
        if (diff < 7 * 86_400_000)   return Math.floor(diff / 86_400_000) + 'd ago';
        return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: d.getFullYear() !== now.getFullYear() ? 'numeric' : undefined });
      } catch (_) {
        return iso;
      }
    },

    formatBytes(bytes) {
      return VD.formatBytes(bytes);
    },

    // Map ISO 639-1 code to human-readable language name
    langName(code) {
      if (!code) return '';
      const map = {
        en: 'English', ja: 'Japanese', ko: 'Korean', zh: 'Chinese',
        fr: 'French',  de: 'German',   es: 'Spanish', pt: 'Portuguese',
        it: 'Italian', nl: 'Dutch',    ru: 'Russian',
      };
      return map[code.toLowerCase()] || code.toUpperCase();
    },

    browseAlternatives(item) {
      if (!item) return;
      const p = new URLSearchParams();
      p.set('switch_item_id', item.id);
      p.set('query', item.title);
      if (item.imdb_id)    p.set('imdb_id',    item.imdb_id);
      if (item.media_type) p.set('media_type', item.media_type);
      if (item.season  != null) p.set('season',  item.season);
      if (item.episode != null) p.set('episode', item.episode);
      window.location.href = '/search?' + p.toString();
    },
  };
};
