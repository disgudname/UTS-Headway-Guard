/**
 * Push Notifications Bell Icon Component
 *
 * Adds a floating bell button that allows users to subscribe/unsubscribe
 * from Web Push notifications for UTS service alerts.
 */
(function() {
  'use strict';

  const BELL_ID = 'uts-push-bell';
  const PANEL_ID = 'uts-push-prefs';

  // Avoid duplicate initialization
  if (document.getElementById(BELL_ID)) return;

  // Check for push support
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    console.log('[push] Push notifications not supported in this browser');
    return;
  }

  let isSubscribed = false;
  let isAuthenticated = false;
  let preferences = {};
  let currentEndpoint = null;
  let swRegistration = null;

  // Inject styles
  const style = document.createElement('style');
  style.textContent = `
    #${BELL_ID} {
      position: fixed;
      bottom: 20px;
      right: 20px;
      width: 56px;
      height: 56px;
      border-radius: 50%;
      background: linear-gradient(135deg, #E57200, #FF8C2A);
      border: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 4px 16px rgba(229, 114, 0, 0.4);
      transition: transform 0.2s ease, box-shadow 0.2s ease, opacity 0.2s ease;
      z-index: 1050;
      opacity: 0;
      pointer-events: none;
    }
    #${BELL_ID}.visible {
      opacity: 1;
      pointer-events: auto;
    }
    #${BELL_ID}:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(229, 114, 0, 0.5);
    }
    #${BELL_ID}:active {
      transform: translateY(0);
    }
    #${BELL_ID} svg {
      width: 28px;
      height: 28px;
      color: white;
      transition: transform 0.2s ease;
    }
    #${BELL_ID}.subscribed {
      background: linear-gradient(135deg, #232D4B, #3D4B6B);
      box-shadow: 0 4px 16px rgba(35, 45, 75, 0.4);
    }
    #${BELL_ID}.subscribed:hover {
      box-shadow: 0 6px 20px rgba(35, 45, 75, 0.5);
    }
    #${BELL_ID}.subscribed svg {
      color: #E57200;
    }
    #${BELL_ID} .badge {
      position: absolute;
      top: 6px;
      right: 6px;
      width: 14px;
      height: 14px;
      background: #10B981;
      border-radius: 50%;
      border: 2px solid white;
      display: none;
    }
    #${BELL_ID}.subscribed .badge {
      display: block;
    }
    @media (max-width: 768px) {
      #${BELL_ID} {
        bottom: 80px;
        right: 16px;
        width: 48px;
        height: 48px;
      }
      #${BELL_ID} svg {
        width: 24px;
        height: 24px;
      }
    }
    #${PANEL_ID} {
      position: fixed;
      bottom: 90px;
      right: 20px;
      width: 280px;
      background: white;
      border-radius: 12px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
      z-index: 1051;
      display: none;
      overflow: hidden;
    }
    #${PANEL_ID}.visible {
      display: block;
    }
    .prefs-header {
      padding: 16px;
      border-bottom: 1px solid #eee;
      font-weight: 600;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-family: system-ui, -apple-system, sans-serif;
    }
    .prefs-close {
      background: none;
      border: none;
      font-size: 20px;
      cursor: pointer;
      color: #666;
      padding: 0;
      line-height: 1;
    }
    .prefs-close:hover {
      color: #333;
    }
    .prefs-body {
      padding: 8px 0;
    }
    .pref-item {
      display: flex;
      align-items: flex-start;
      padding: 12px 16px;
      cursor: pointer;
      transition: background 0.15s ease;
    }
    .pref-item:hover {
      background: #f5f5f5;
    }
    .pref-item input {
      margin-right: 12px;
      margin-top: 2px;
      flex-shrink: 0;
    }
    .pref-item-content {
      display: flex;
      flex-direction: column;
      font-family: system-ui, -apple-system, sans-serif;
    }
    .pref-item-label {
      font-size: 14px;
      color: #333;
    }
    .pref-item-desc {
      font-size: 12px;
      color: #666;
      margin-top: 2px;
    }
    .prefs-footer {
      padding: 12px 16px;
      border-top: 1px solid #eee;
      display: flex;
      justify-content: center;
    }
    .prefs-unsubscribe {
      background: none;
      border: 1px solid #dc2626;
      color: #dc2626;
      padding: 8px 16px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 13px;
      font-family: system-ui, -apple-system, sans-serif;
      transition: background 0.15s ease;
    }
    .prefs-unsubscribe:hover {
      background: #fef2f2;
    }
    @media (max-width: 768px) {
      #${PANEL_ID} {
        bottom: 140px;
        right: 16px;
        width: calc(100% - 32px);
        max-width: 320px;
      }
    }
  `;
  document.head.appendChild(style);

  // Create bell button
  const bell = document.createElement('button');
  bell.id = BELL_ID;
  bell.type = 'button';
  bell.setAttribute('aria-label', 'Enable push notifications');
  bell.setAttribute('title', 'Get notified about service alerts');
  bell.innerHTML = `
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path>
      <path d="M13.73 21a2 2 0 0 1-3.46 0"></path>
    </svg>
    <span class="badge"></span>
  `;
  document.body.appendChild(bell);

  // Create preferences panel (for authenticated users)
  const panel = document.createElement('div');
  panel.id = PANEL_ID;
  panel.innerHTML = `
    <div class="prefs-header">
      <span>Notification Settings</span>
      <button class="prefs-close" aria-label="Close">&times;</button>
    </div>
    <div class="prefs-body">
      <label class="pref-item">
        <input type="checkbox" data-pref="service_alerts" checked disabled>
        <div class="pref-item-content">
          <span class="pref-item-label">Service Alerts</span>
          <span class="pref-item-desc">Always on for all subscribers</span>
        </div>
      </label>
      <label class="pref-item">
        <input type="checkbox" data-pref="low_soc">
        <div class="pref-item-content">
          <span class="pref-item-label">Low Battery Alerts</span>
          <span class="pref-item-desc">Electric buses below 35%</span>
        </div>
      </label>
      <label class="pref-item">
        <input type="checkbox" data-pref="headway">
        <div class="pref-item-content">
          <span class="pref-item-label">Headway Issues</span>
          <span class="pref-item-desc">Bunching and large gaps</span>
        </div>
      </label>
    </div>
    <div class="prefs-footer">
      <button class="prefs-unsubscribe">Unsubscribe</button>
    </div>
  `;
  document.body.appendChild(panel);

  // Panel event handlers
  panel.querySelector('.prefs-close').addEventListener('click', () => {
    panel.classList.remove('visible');
  });

  panel.querySelector('.prefs-unsubscribe').addEventListener('click', async () => {
    panel.classList.remove('visible');
    await unsubscribeUser();
  });

  // Close panel when clicking outside
  document.addEventListener('click', (e) => {
    if (!panel.contains(e.target) && e.target !== bell && !bell.contains(e.target)) {
      panel.classList.remove('visible');
    }
  });

  // Handle preference checkbox changes
  panel.querySelectorAll('input[data-pref]').forEach(checkbox => {
    checkbox.addEventListener('change', async (e) => {
      const pref = e.target.dataset.pref;
      if (pref === 'service_alerts') return; // Always on

      preferences[pref] = e.target.checked;
      await savePreferences();
    });
  });

  // Update panel checkboxes from current preferences
  function updatePanelCheckboxes() {
    panel.querySelectorAll('input[data-pref]').forEach(checkbox => {
      const pref = checkbox.dataset.pref;
      if (pref === 'service_alerts') return;
      checkbox.checked = preferences[pref] === true;
    });
  }

  // Save preferences to server
  async function savePreferences() {
    if (!currentEndpoint) return;
    try {
      const resp = await fetch('/api/push/preferences', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint: currentEndpoint, preferences })
      });
      if (!resp.ok) {
        console.error('[push] Failed to save preferences');
      }
    } catch (err) {
      console.error('[push] Error saving preferences:', err);
    }
  }

  // Show preferences panel
  function showPreferencesPanel() {
    updatePanelCheckboxes();
    panel.classList.add('visible');
  }

  // Helper: Convert URL-safe base64 to Uint8Array
  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
      .replace(/-/g, '+')
      .replace(/_/g, '/');
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; ++i) {
      outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
  }

  // Fetch VAPID public key from server
  async function getVapidPublicKey() {
    const resp = await fetch('/api/push/vapid-public-key');
    if (!resp.ok) {
      throw new Error('VAPID key not available');
    }
    const data = await resp.json();
    return data.publicKey;
  }

  // Update UI based on subscription state
  function updateUI() {
    bell.classList.toggle('subscribed', isSubscribed);
    bell.setAttribute('aria-label', isSubscribed
      ? 'Disable push notifications'
      : 'Enable push notifications');
    bell.setAttribute('title', isSubscribed
      ? 'You are subscribed to service alerts'
      : 'Get notified about service alerts');
  }

  // Subscribe to push notifications
  async function subscribeUser() {
    try {
      const vapidKey = await getVapidPublicKey();
      const subscription = await swRegistration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapidKey)
      });

      currentEndpoint = subscription.endpoint;

      // Send subscription to server
      const resp = await fetch('/api/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subscription.toJSON())
      });

      if (!resp.ok) {
        throw new Error('Failed to save subscription on server');
      }

      const data = await resp.json();
      isAuthenticated = data.authenticated === true;
      preferences = data.preferences || {};

      isSubscribed = true;
      updateUI();
      console.log('[push] Subscribed to push notifications', { authenticated: isAuthenticated });
    } catch (err) {
      console.error('[push] Subscribe error:', err);
      // If permission denied, show alert
      if (Notification.permission === 'denied') {
        alert('Notifications are blocked. Please enable them in your browser settings.');
      }
    }
  }

  // Unsubscribe from push notifications
  async function unsubscribeUser() {
    try {
      const subscription = await swRegistration.pushManager.getSubscription();
      if (subscription) {
        await subscription.unsubscribe();

        // Tell server to remove subscription
        await fetch('/api/push/unsubscribe', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ endpoint: subscription.endpoint })
        });
      }

      isSubscribed = false;
      isAuthenticated = false;
      preferences = {};
      currentEndpoint = null;
      updateUI();
      console.log('[push] Unsubscribed from push notifications');
    } catch (err) {
      console.error('[push] Unsubscribe error:', err);
    }
  }

  // Handle bell click
  bell.addEventListener('click', async () => {
    if (isSubscribed) {
      if (isAuthenticated) {
        // Show preferences panel for authenticated users
        showPreferencesPanel();
      } else {
        // Unauthenticated users just toggle subscription
        await unsubscribeUser();
      }
    } else {
      // Request permission first
      const permission = await Notification.requestPermission();
      if (permission === 'granted') {
        await subscribeUser();
      } else if (permission === 'denied') {
        alert('Notifications are blocked. Please enable them in your browser settings.');
      }
    }
  });

  // Initialize when service worker is ready
  navigator.serviceWorker.ready.then(async (registration) => {
    swRegistration = registration;

    // Check if VAPID is configured
    try {
      await getVapidPublicKey();
    } catch (e) {
      console.log('[push] Push notifications not configured on server');
      return; // Don't show bell if not configured
    }

    // Check current subscription state
    const subscription = await registration.pushManager.getSubscription();
    isSubscribed = subscription !== null;

    if (isSubscribed && subscription) {
      currentEndpoint = subscription.endpoint;
      // Fetch current preferences from server
      try {
        const resp = await fetch('/api/push/preferences', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ endpoint: subscription.endpoint })
        });
        if (resp.ok) {
          const data = await resp.json();
          isAuthenticated = data.authenticated === true;
          preferences = data.preferences || {};
        }
      } catch (e) {
        console.log('[push] Could not fetch preferences');
      }
    }

    updateUI();

    // Show the bell
    bell.classList.add('visible');
  }).catch(err => {
    console.error('[push] Service worker not ready:', err);
  });
})();
