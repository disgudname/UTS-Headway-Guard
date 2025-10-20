(function(){
  const NAV_ID = 'hg-nav';
  if (document.getElementById(NAV_ID)) return;

  const params = new URLSearchParams(window.location.search);
  if (params.get('dispatcher') === 'true') return;

  const kioskModeEnabled = (params.get('kioskMode') || '').toLowerCase() === 'true';
  const adminKioskModeEnabled = (params.get('adminKioskMode') || '').toLowerCase() === 'true';

  const path = window.location.pathname.replace(/\/+$/, '');
  const isMapLikePage = path === '/map' || path === '/testmap';
  const isDownedPage = path === '/downed';
  if ((isMapLikePage && (kioskModeEnabled || adminKioskModeEnabled)) || (isDownedPage && kioskModeEnabled)) {
    return;
  }

  const mobileQuery = window.matchMedia('(max-width: 768px)');

  const links = [
    {
      href: '/',
      label: 'Home',
      icon: '/media/home.svg'
    },
    {
      href: '/dispatcher',
      label: 'Dispatch',
      icon: '/media/dispatcher.svg'
    },
    {
      href: 'https://uva-uts.transloc.com/secure/dispatch/',
      label: 'TransLoc',
      icon: '/media/transloc.svg'
    },
    {
      href: '/map',
      label: 'Live Map',
      icon: '/media/map.svg'
    },
    {
      href: '/ridership',
      label: 'Ridership',
      icon: '/media/ridership.svg'
    },
    {
      href: '/replay',
      label: 'Replay',
      icon: '/media/replay.svg'
    },
    {
      href: '/downed',
      label: 'Downed Vehicles',
      icon: '/media/downed.svg'
    },
    {
      href: '/driver',
      label: 'Driver',
      icon: '/media/driver.svg'
    }
  ];

  const style = document.createElement('style');
  style.textContent = `
    @font-face{
      font-family:'FGDC';
      src:url('/fonts/FGDC.ttf') format('truetype');
      font-display:swap;
    }
    :root{
      --hg-nav-bottom-offset:0px;
      --hg-nav-left-offset:0px;
    }
    body.hg-nav-active{
      padding-bottom:var(--hg-nav-bottom-offset);
      padding-left:var(--hg-nav-left-offset);
    }
    body.hg-nav-active .hg-nav-scroll{
      padding-bottom:var(--hg-nav-bottom-offset);
      box-sizing:border-box;
    }
    body.hg-nav-active .leaflet-bottom{
      bottom:calc(var(--hg-nav-bottom-offset) + 4px);
    }
    #${NAV_ID}{
      position:fixed;
      background:#232D4B;
      color:#FFFFFF;
      z-index:1100;
      display:flex;
      border-radius:0;
      box-sizing:border-box;
      font-family:'FGDC' !important;
    }
    #${NAV_ID} *,
    #${NAV_ID} *::before,
    #${NAV_ID} *::after{
      font-family:'FGDC' !important;
    }
    #${NAV_ID} .hg-nav__inner{
      display:flex;
      gap:12px;
      padding:12px 16px 14px;
      scrollbar-width:none;
      box-sizing:border-box;
    }
    #${NAV_ID} .hg-nav__inner::-webkit-scrollbar{display:none;}
    #${NAV_ID} a{
      flex:0 0 auto;
      color:inherit;
      text-decoration:none;
      font:normal 12px/1.1 'FGDC';
      border-radius:18px;
      background:rgba(0,0,0,0.22);
      border:1px solid rgba(255,255,255,0.12);
      display:flex;
      flex-direction:column;
      align-items:center;
      justify-content:center;
      gap:8px;
      text-align:center;
      padding:10px 6px;
      box-sizing:border-box;
    }
    #${NAV_ID} .hg-nav__auth{
      flex:0 0 auto;
      color:inherit;
      border-radius:18px;
      background:rgba(0,0,0,0.22);
      border:1px solid rgba(255,255,255,0.12);
      display:flex;
      flex-direction:column;
      align-items:center;
      justify-content:center;
      gap:10px;
      text-align:center;
      padding:14px 12px;
      box-sizing:border-box;
    }
    #${NAV_ID} .hg-nav__auth-info{
      display:flex;
      flex-direction:column;
      gap:6px;
      align-items:center;
      font-size:12px;
      color:rgba(255,255,255,0.82);
      text-transform:uppercase;
      letter-spacing:0.08em;
    }
    #${NAV_ID} .hg-nav__auth-secret{
      display:block;
      font-size:15px;
      letter-spacing:0.12em;
      color:#FFFFFF;
    }
    #${NAV_ID} .hg-nav__auth-logout{
      background:rgba(0,0,0,0.25);
      border:1px solid rgba(255,255,255,0.2);
      border-radius:999px;
      color:inherit;
      font:inherit;
      padding:8px 18px;
      cursor:pointer;
      transition:background-color 0.2s ease,border-color 0.2s ease;
    }
    #${NAV_ID} .hg-nav__auth-logout:hover{
      background:rgba(0,0,0,0.32);
      border-color:rgba(255,255,255,0.28);
    }
    #${NAV_ID} .hg-nav__auth-logout:disabled{
      opacity:0.6;
      cursor:not-allowed;
    }
    #${NAV_ID} a:focus-visible{
      outline:2px solid #FFFFFF;
      outline-offset:2px;
    }
    #${NAV_ID} .hg-nav__icon{
      display:flex;
      align-items:center;
      justify-content:center;
      color:inherit;
    }
    #${NAV_ID} .hg-nav__icon-img{
      width:32px;
      height:32px;
      display:block;
    }
    #${NAV_ID} .hg-nav__label{
      display:block;
      color:inherit;
      text-decoration:none;
      word-break:break-word;
      white-space:normal;
    }
    .hg-nav-spacer-bottom{display:none;}
    @media (max-width: 768px){
      #${NAV_ID}{
        left:0;
        right:0;
        bottom:0;
        border-top:1px solid rgba(255,255,255,0.08);
        box-shadow:0 -6px 18px rgba(0,0,0,0.25);
        flex-direction:column;
      }
      #${NAV_ID} .hg-nav__inner{
        flex-direction:row;
        overflow-x:auto;
        -webkit-overflow-scrolling:touch;
      }
      #${NAV_ID} a{
        width:76px;
        aspect-ratio:1/1;
      }
      #${NAV_ID} .hg-nav__auth{
        flex-direction:row;
        gap:12px;
        padding:14px 18px;
        width:auto;
        min-width:200px;
        margin:8px 4px 4px;
      }
      #${NAV_ID} .hg-nav__auth-info{
        align-items:flex-start;
        text-align:left;
      }
      .hg-nav-spacer-bottom{display:block;}
    }
    @media (min-width: 769px){
      #${NAV_ID}{
        top:0;
        bottom:0;
        left:0;
        width:96px;
        border-right:1px solid rgba(255,255,255,0.08);
        box-shadow:6px 0 18px rgba(0,0,0,0.18);
        flex-direction:column;
        align-items:stretch;
      }
      #${NAV_ID} .hg-nav__inner{
        flex-direction:column;
        overflow-y:auto;
        padding:24px 12px;
        flex:1;
      }
      #${NAV_ID} a{
        width:100%;
        padding:16px 12px;
      }
      #${NAV_ID} .hg-nav__auth{
        width:100%;
        margin-top:12px;
      }
    }
  `;
  document.head.appendChild(style);

  const nav = document.createElement('nav');
  nav.id = NAV_ID;
  nav.className = 'hg-nav';
  nav.setAttribute('aria-label', 'UTS Operations Dashboard navigation');

  const inner = document.createElement('div');
  inner.className = 'hg-nav__inner';

  links.forEach(link => {
    const anchor = document.createElement('a');
    anchor.href = link.href;

    const icon = document.createElement('span');
    icon.className = 'hg-nav__icon';

    const img = document.createElement('img');
    img.src = link.icon;
    img.alt = '';
    img.className = 'hg-nav__icon-img';
    img.setAttribute('aria-hidden', 'true');

    icon.appendChild(img);

    const label = document.createElement('span');
    label.className = 'hg-nav__label';
    label.textContent = link.label;

    anchor.appendChild(icon);
    anchor.appendChild(label);
    inner.appendChild(anchor);
  });

  nav.appendChild(inner);
  document.body.appendChild(nav);

  const spacer = document.createElement('div');
  spacer.className = 'hg-nav-spacer-bottom';
  document.body.appendChild(spacer);

  let authSection = null;
  let authSecret = null;
  let logoutButton = null;

  const buildReturnTarget = () => {
    const path = window.location.pathname || '/';
    const search = window.location.search || '';
    const hash = window.location.hash || '';
    return `${path}${search}${hash}`;
  };

  const redirectToLogin = () => {
    const target = buildReturnTarget();
    window.location.href = `/login?return=${encodeURIComponent(target)}`;
  };

  const removeAuthSection = () => {
    if (logoutButton) {
      logoutButton.disabled = false;
      logoutButton.textContent = 'Log out';
    }
    if (authSection && authSection.parentNode) {
      authSection.parentNode.removeChild(authSection);
    }
    authSection = null;
    authSecret = null;
    logoutButton = null;
  };

  const ensureAuthSection = (secretLabel) => {
    if (!authSection) {
      authSection = document.createElement('div');
      authSection.className = 'hg-nav__auth';

      const info = document.createElement('div');
      info.className = 'hg-nav__auth-info';

      const label = document.createElement('span');
      label.textContent = 'Logged in as';

      authSecret = document.createElement('span');
      authSecret.className = 'hg-nav__auth-secret';

      info.appendChild(label);
      info.appendChild(authSecret);

      logoutButton = document.createElement('button');
      logoutButton.type = 'button';
      logoutButton.className = 'hg-nav__auth-logout';
      logoutButton.textContent = 'Log out';
      logoutButton.addEventListener('click', async () => {
        if (!logoutButton || logoutButton.disabled) return;
        const defaultLabel = logoutButton.textContent;
        logoutButton.disabled = true;
        logoutButton.textContent = 'Logging out...';
        try {
          const resp = await fetch('/api/dispatcher/logout', {
            method: 'POST',
            credentials: 'include',
          });
          if (!resp.ok) throw new Error('logout failed');
          if (typeof window.deleteCookieValue === 'function') {
            if (typeof window.BLOCK_LAYOUT_COOKIE_NAME === 'string') {
              try { window.deleteCookieValue(window.BLOCK_LAYOUT_COOKIE_NAME); } catch (err) {}
            }
            if (typeof window.LEFT_PANE_WIDTH_COOKIE_NAME === 'string') {
              try { window.deleteCookieValue(window.LEFT_PANE_WIDTH_COOKIE_NAME); } catch (err) {}
            }
          }
          removeAuthSection();
          redirectToLogin();
        } catch (err) {
          console.warn('Failed to log out', err);
          logoutButton.disabled = false;
          logoutButton.textContent = defaultLabel;
          alert('Unable to log out. Please try again.');
        }
      });

      authSection.appendChild(info);
      authSection.appendChild(logoutButton);
      inner.appendChild(authSection);
    }

    if (authSecret) {
      authSecret.textContent = secretLabel || 'Unknown';
    }
  };

  const updateAuthSection = async () => {
    let data = null;
    try {
      const resp = await fetch('/api/dispatcher/auth', {
        cache: 'no-store',
        credentials: 'include',
      });
      if (resp.ok) {
        data = await resp.json();
      }
    } catch (err) {
      console.warn('Failed to check dispatcher auth', err);
    }

    const authorized = data && data.authorized === true;
    const secretLabel = data && typeof data.secret === 'string' && data.secret.trim()
      ? data.secret.trim()
      : null;

    if (authorized) {
      ensureAuthSection(secretLabel || 'Unknown');
      if (typeof updateSpacerHeight === 'function') {
        updateSpacerHeight();
      }
    } else {
      removeAuthSection();
      if (typeof updateSpacerHeight === 'function') {
        updateSpacerHeight();
      }
    }
  };

  const markScrollableContainers = () => {
    const all = document.querySelectorAll('*');
    all.forEach(el => {
      if (el === nav || nav.contains(el)) return;
      const style = window.getComputedStyle(el);
      const overflowY = style.overflowY;
      const overflow = style.overflow;
      const isScrollableY = overflowY === 'auto' || overflowY === 'scroll' ||
        ((overflow === 'auto' || overflow === 'scroll') && overflowY === 'visible');
      if (!isScrollableY) {
        el.classList.remove('hg-nav-scroll');
        return;
      }
      if (el.scrollHeight > el.clientHeight) {
        el.classList.add('hg-nav-scroll');
      } else {
        el.classList.remove('hg-nav-scroll');
      }
    });
  };

  const updateSpacerHeight = () => {
    const style = window.getComputedStyle(nav);
    const isVisible = style.display !== 'none';
    if (!isVisible) {
      spacer.style.height = '0px';
      document.documentElement.style.setProperty('--hg-nav-bottom-offset', '0px');
      document.documentElement.style.setProperty('--hg-nav-left-offset', '0px');
      document.body.classList.remove('hg-nav-active');
      nav.removeAttribute('data-orientation');
      return;
    }

    const isMobile = mobileQuery.matches;
    if (isMobile) {
      nav.setAttribute('data-orientation', 'horizontal');
      const navHeight = nav.offsetHeight;
      spacer.style.height = `${navHeight}px`;
      document.documentElement.style.setProperty('--hg-nav-bottom-offset', `${navHeight}px`);
      document.documentElement.style.setProperty('--hg-nav-left-offset', '0px');
    } else {
      nav.setAttribute('data-orientation', 'vertical');
      spacer.style.height = '0px';
      document.documentElement.style.setProperty('--hg-nav-bottom-offset', '0px');
      const navWidth = nav.offsetWidth;
      document.documentElement.style.setProperty('--hg-nav-left-offset', `${navWidth}px`);
    }

    document.body.classList.add('hg-nav-active');
    markScrollableContainers();
  };

  if (mobileQuery.addEventListener) {
    mobileQuery.addEventListener('change', updateSpacerHeight);
  } else if (mobileQuery.addListener) {
    mobileQuery.addListener(updateSpacerHeight);
  }

  updateSpacerHeight();
  updateAuthSection();
  markScrollableContainers();
  setTimeout(markScrollableContainers, 500);
  setTimeout(markScrollableContainers, 1500);
  window.addEventListener('resize', updateSpacerHeight, { passive: true });
  if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', updateSpacerHeight, { passive: true });
  }
  window.addEventListener('orientationchange', updateSpacerHeight, { passive: true });
  if (window.ResizeObserver) {
    const observer = new ResizeObserver(updateSpacerHeight);
    observer.observe(nav);
  }
})();
