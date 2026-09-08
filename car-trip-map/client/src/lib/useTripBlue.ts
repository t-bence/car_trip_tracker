import { useSyncExternalStore } from 'react';

const FALLBACK = '#3b82f6';

function subscribe(onChange: () => void) {
  const media = window.matchMedia('(prefers-color-scheme: dark)');
  media.addEventListener('change', onChange);
  return () => media.removeEventListener('change', onChange);
}

function readTripBlue() {
  return getComputedStyle(document.documentElement).getPropertyValue('--trip-blue').trim() || FALLBACK;
}

/**
 * The `--trip-blue` token as a plain color string, kept in step with the
 * light/dark theme. The charts draw on a canvas, which cannot resolve a CSS
 * variable, so they need the resolved value.
 */
export function useTripBlue() {
  return useSyncExternalStore(subscribe, readTripBlue, () => FALLBACK);
}
