// CHS Dashboard Service Worker
// Caches the app shell so it loads instantly and works offline
// Live data (KPIs, tasks, calendar) always fetches fresh from APIs

const CACHE_NAME = 'chs-dashboard-v1';
const CACHE_URLS = [
  '/',
  '/index.html',
  'https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Nunito:wght@400;500;600;700;800&display=swap'
];

// Install — cache app shell
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(CACHE_URLS).catch(function() {
        // Fonts may fail on first install — that's OK
        return cache.add('/');
      });
    })
  );
  self.skipWaiting();
});

// Activate — clean up old caches
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(keys) {
      return Promise.all(
        keys.filter(function(key) { return key !== CACHE_NAME; })
            .map(function(key) { return caches.delete(key); })
      );
    })
  );
  self.clients.claim();
});

// Fetch — network first for API calls, cache first for app shell
self.addEventListener('fetch', function(event) {
  var url = event.request.url;

  // Always go network-first for Google APIs and Jobber
  if (url.includes('googleapis.com') ||
      url.includes('getjobber.com') ||
      url.includes('workers.dev') ||
      url.includes('open-meteo.com') ||
      url.includes('fonts.gstatic.com')) {
    event.respondWith(
      fetch(event.request).catch(function() {
        return caches.match(event.request);
      })
    );
    return;
  }

  // Cache-first for the app shell (HTML, fonts)
  event.respondWith(
    caches.match(event.request).then(function(cached) {
      if (cached) {
        // Return cache but update in background
        fetch(event.request).then(function(fresh) {
          caches.open(CACHE_NAME).then(function(cache) {
            cache.put(event.request, fresh);
          });
        }).catch(function() {});
        return cached;
      }
      return fetch(event.request).then(function(fresh) {
        var clone = fresh.clone();
        caches.open(CACHE_NAME).then(function(cache) {
          cache.put(event.request, clone);
        });
        return fresh;
      });
    })
  );
});
