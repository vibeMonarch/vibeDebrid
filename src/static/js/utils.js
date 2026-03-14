/**
 * Shared utility functions for vibeDebrid templates.
 *
 * Loaded synchronously from base.html so VD.* is available
 * in all inline scripts and Alpine component methods.
 */
window.VD = window.VD || {};

/**
 * Format byte count as human-readable string.
 * Returns '--' for null/zero/falsy values.
 */
VD.formatBytes = function(bytes) {
  if (!bytes || bytes <= 0) return '--';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let value = bytes;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return value.toFixed(i === 0 ? 0 : 1) + ' ' + units[i];
};

/**
 * Escape HTML special characters for safe text insertion.
 * Returns empty string for null/undefined.
 */
VD.escapeHtml = function(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
};

/**
 * Escape for HTML attribute values (includes single quotes).
 * Returns empty string for null/undefined.
 */
VD.escapeAttr = function(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/'/g, '&#39;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
};
