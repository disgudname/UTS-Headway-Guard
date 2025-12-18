const CACHE_NAME = 'uts-ops-v5';
const OFFLINE_URL = '/offline';

const STATIC_ASSETS = [
  '/offline',
  '/FGDC.ttf',
  '/ANTONIO.ttf',
  '/centurygothic.ttf',
  '/media/favicon.ico',
  '/media/icon-192.png',
  '/media/icon-512.png',
  '/media/dispatcher.svg',
  '/media/transloc.svg',
  '/media/map.svg',
  '/media/ridership.svg',
  '/media/headway.svg',
  '/media/replay.svg',
  '/media/downed.svg',
  '/media/driver.svg',
  '/media/home.svg',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS);
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name !== CACHE_NAME)
          .map((name) => caches.delete(name))
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET requests
  if (request.method !== 'GET') {
    return;
  }

  // Skip API calls and SSE streams - always use network
  if (url.pathname.startsWith('/api/') ||
      url.pathname.startsWith('/stream/') ||
      url.pathname.startsWith('/v1/')) {
    return;
  }

  // Skip external resources
  if (url.origin !== location.origin) {
    return;
  }

  // Navigation requests (HTML pages) - network first, fallback to offline page
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .catch(() => caches.match(OFFLINE_URL))
    );
    return;
  }

  // Static assets - cache first, fallback to network
  event.respondWith(
    caches.match(request).then((cachedResponse) => {
      if (cachedResponse) {
        return cachedResponse;
      }
      return fetch(request).then((response) => {
        // Cache successful responses for static assets
        if (response.ok && isStaticAsset(url.pathname)) {
          const responseClone = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(request, responseClone);
          });
        }
        return response;
      });
    })
  );
});

function isStaticAsset(pathname) {
  return (
    pathname.endsWith('.js') ||
    pathname.endsWith('.css') ||
    pathname.endsWith('.ttf') ||
    pathname.endsWith('.woff') ||
    pathname.endsWith('.woff2') ||
    pathname.endsWith('.png') ||
    pathname.endsWith('.svg') ||
    pathname.endsWith('.jpg') ||
    pathname.endsWith('.jpeg') ||
    pathname.endsWith('.ico') ||
    pathname.endsWith('.wav') ||
    pathname.endsWith('.mp3')
  );
}
