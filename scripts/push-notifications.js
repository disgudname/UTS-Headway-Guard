/**
 * Push Notifications Bell Icon Component
 *
 * Adds a floating bell button that allows users to subscribe/unsubscribe
 * from Web Push notifications for UTS service alerts.
 */
(function() {
  'use strict';

  const BELL_ID = 'uts-push-bell';

  // Avoid duplicate initialization
  if (document.getElementById(BELL_ID)) return;

  // Check for push support
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    console.log('[push] Push notifications not supported in this browser');
    return;
  }

  let isSubscribed = false;
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

      // Send subscription to server
      const resp = await fetch('/api/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subscription.toJSON())
      });

      if (!resp.ok) {
        throw new Error('Failed to save subscription on server');
      }

      isSubscribed = true;
      updateUI();
      console.log('[push] Subscribed to push notifications');
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
      updateUI();
      console.log('[push] Unsubscribed from push notifications');
    } catch (err) {
      console.error('[push] Unsubscribe error:', err);
    }
  }

  // Handle bell click
  bell.addEventListener('click', async () => {
    if (isSubscribed) {
      await unsubscribeUser();
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
    updateUI();

    // Show the bell
    bell.classList.add('visible');
  }).catch(err => {
    console.error('[push] Service worker not ready:', err);
  });
})();
