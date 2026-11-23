import { useSyncExternalStore } from 'react';

const FALLBACK = 'hsla(188, 72%, 30%, 1)';

function subscribe(onChange: () => void) {
  const media = window.matchMedia('(prefers-color-scheme: dark)');
  media.addEventListener('change', onChange);
  return () => media.removeEventListener('change', onChange);
}

function readTripColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--trip-color').trim() || FALLBACK;
}

/**
 * The `--trip-color` token as a plain color string, kept in step with the
 * light/dark theme. The charts draw on a canvas, which cannot resolve a CSS
 * variable, so they need the resolved value.
 */
export function useTripColor() {
  return useSyncExternalStore(subscribe, readTripColor, () => FALLBACK);
}
