/**
 * Service worker that makes the static build cross-origin isolated.
 *
 * GitHub Pages cannot set response headers, so the page would otherwise load
 * without COOP/COEP.  That has two concrete costs for this observer:
 *
 *   1. Chrome clamps `performance.now()` to ~100 µs for non-isolated pages.
 *      The static build timestamps every event with that clock, so the
 *      sub-100 µs bins of the inter-arrival histogram would be quantised to
 *      0 or 100 µs - the tool would be measuring the browser's timer, not the
 *      market.
 *   2. `SharedArrayBuffer` is unavailable, so the ingest worker has to copy
 *      every frame across to the render thread instead of writing straight
 *      into a shared ring.
 *
 * A service worker *can* synthesise response headers for same-origin requests,
 * which restores both.  This is the standard technique; the page registers it
 * and reloads once so the document is served by the worker.
 *
 * Only same-origin requests are touched: re-issuing cross-origin requests from
 * here would turn CORS responses opaque and break the Binance REST calls.
 */

self.addEventListener('install', () => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
  const request = event.request;

  // Only same-origin documents and assets need (and may safely receive) these.
  let url;
  try {
    url = new URL(request.url);
  } catch {
    return;
  }
  if (url.origin !== self.location.origin) return;
  if (request.cache === 'only-if-cached' && request.mode !== 'same-origin') return;

  event.respondWith(
    fetch(request)
      .then((response) => {
        // Opaque or errored responses cannot be reconstructed.
        if (response.status === 0) return response;
        const headers = new Headers(response.headers);
        headers.set('Cross-Origin-Opener-Policy', 'same-origin');
        headers.set('Cross-Origin-Embedder-Policy', 'require-corp');
        headers.set('Cross-Origin-Resource-Policy', 'same-origin');
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers,
        });
      })
      .catch(() => fetch(request)),
  );
});
