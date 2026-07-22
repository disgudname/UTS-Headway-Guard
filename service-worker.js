const CACHE_NAME = 'uvatransit-ops-v13';
const OFFLINE_URL = '/offline';

const STATIC_ASSETS = [
  '/offline',
  '/FGDC.ttf',
  '/ANTONIO.ttf',
  '/centurygothic.ttf',
  '/media/favicon.ico',
  '/media/icon-192.png',
  '/media/icon-512.png',
  '/media/notification-badge.png',
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

  // JS/CSS - network first, so code changes show up on a normal reload.
  // Falls back to the cached copy only if the network is unreachable.
  if (isCodeAsset(url.pathname)) {
    event.respondWith(
      fetch(request)
        .then((response) => {
          if (response.ok) {
            const responseClone = response.clone();
            caches.open(CACHE_NAME).then((cache) => {
              cache.put(request, responseClone);
            });
          }
          return response;
        })
        .catch(() => caches.match(request))
    );
    return;
  }

  // Other static assets (fonts/images) - cache first, fallback to network
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

function isCodeAsset(pathname) {
  return pathname.endsWith('.js') || pathname.endsWith('.css');
}

function isStaticAsset(pathname) {
  return (
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

// Push notification handler
self.addEventListener('push', (event) => {
  if (!event.data) return;

  let payload;
  try {
    payload = event.data.json();
  } catch (e) {
    payload = {
      title: 'UVATransit Service Alert',
      body: event.data.text()
    };
  }

  const options = {
    body: payload.body || '',
    icon: '/media/icon-192.png',
    badge: '/media/notification-badge.png',
    tag: payload.tag || 'uts-alert',
    renotify: true,
    requireInteraction: true,
    data: {
      url: payload.url || '/map'
    }
  };

  event.waitUntil(
    self.registration.showNotification(payload.title || 'UVATransit Service Alert', options)
  );
});

// Notification click handler
self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const url = event.notification.data?.url || '/map';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true })
      .then((clientList) => {
        // Focus existing window if available
        for (const client of clientList) {
          if (client.url.includes(self.location.origin) && 'focus' in client) {
            client.navigate(url);
            return client.focus();
          }
        }
        // Open new window if no existing one
        if (clients.openWindow) {
          return clients.openWindow(url);
        }
      })
  );
});
