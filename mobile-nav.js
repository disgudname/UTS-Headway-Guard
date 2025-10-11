(function(){
  const NAV_ID = 'hg-mobile-nav';
  if (document.getElementById(NAV_ID)) return;

  const params = new URLSearchParams(window.location.search);
  if (params.get('dispatcher') === 'true') return;

  const links = [
    {
      href: '/',
      label: 'Home',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linejoin="round" stroke-linecap="round">
          <path d="M12 28 L32 12 52 28" />
          <path d="M20 28 V50 H44 V28" />
        </svg>
      `
    },
    {
      href: '/driver',
      label: 'Driver',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linejoin="round">
          <rect x="8" y="16" width="48" height="28" rx="4" />
          <line x1="8" y1="30" x2="56" y2="30" />
          <rect x="16" y="20" width="12" height="8" />
          <rect x="36" y="20" width="12" height="8" />
          <circle cx="20" cy="46" r="4" fill="currentColor" stroke="none" />
          <circle cx="44" cy="46" r="4" fill="currentColor" stroke="none" />
        </svg>
      `
    },
    {
      href: '/dispatcher',
      label: 'Dispatch',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <path d="M32 12 L22 52 h20 L32 12 z" />
          <line x1="24" y1="52" x2="40" y2="52" />
          <path d="M24 28c4-4 8-4 12 0" />
          <path d="M20 20c8-8 16-8 24 0" />
        </svg>
      `
    },
    {
      href: '/servicecrew',
      label: 'Service Crew',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linejoin="round">
          <rect x="16" y="20" width="20" height="28" rx="2" />
          <rect x="16" y="12" width="20" height="8" rx="2" />
          <path d="M36 24h8v20a4 4 0 0 1-4 4h-4" />
          <path d="M44 44h4a4 4 0 0 0 4-4V24" />
          <circle cx="24" cy="48" r="4" fill="currentColor" stroke="none" />
        </svg>
      `
    },
    {
      href: '/map',
      label: 'Live Map',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="8 20 24 12 40 20 56 12 56 44 40 52 24 44 8 52 8 20" />
          <line x1="24" y1="12" x2="24" y2="44" />
          <line x1="40" y1="20" x2="40" y2="52" />
        </svg>
      `
    },
    {
      href: '/ridership',
      label: 'Ridership',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <line x1="8" y1="52" x2="56" y2="52" />
          <rect x="12" y="32" width="8" height="20" stroke="none" fill="currentColor" />
          <rect x="28" y="24" width="8" height="28" stroke="none" fill="currentColor" />
          <rect x="44" y="16" width="8" height="36" stroke="none" fill="currentColor" />
        </svg>
      `
    },
    {
      href: '/replay',
      label: 'Replay',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <path d="M32 12a20 20 0 1 1-20 20" />
          <polyline points="12 24 12 12 24 12" />
        </svg>
      `
    },
    {
      href: '/downed',
      label: 'Downed Vehicles',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <path d="M32 12 L12 52h40L32 12z" />
          <line x1="32" y1="26" x2="32" y2="38" />
          <circle cx="32" cy="46" r="2.5" fill="currentColor" stroke="none" />
        </svg>
      `
    },
    {
      href: '/testmap',
      label: 'Test Map',
      icon: `
        <svg viewBox="0 0 64 64" stroke="currentColor" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 18l16-6 16 6 16-6v34l-16 6-16-6-16 6V18z" />
          <line x1="28" y1="12" x2="28" y2="46" />
          <line x1="44" y1="18" x2="44" y2="52" />
        </svg>
      `
    }
  ];

  const style = document.createElement('style');
  style.textContent = `
    :root{--hg-mobile-nav-offset:0px;}
    body.hg-mobile-nav-active{
      padding-bottom:var(--hg-mobile-nav-offset);
    }
    body.hg-mobile-nav-active .hg-mobile-nav-scroll{
      padding-bottom:var(--hg-mobile-nav-offset);
      box-sizing:border-box;
    }
    body.hg-mobile-nav-active .leaflet-bottom{
      bottom:calc(var(--hg-mobile-nav-offset) + 4px);
    }
    #${NAV_ID}{
      position:fixed;
      left:0;
      right:0;
      bottom:0;
      background:#232D4B;
      color:#E57200;
      border-top:1px solid rgba(255,255,255,0.08);
      box-shadow:0 -6px 18px rgba(0,0,0,0.25);
      z-index:1100;
      display:none;
    }
    #${NAV_ID} .hg-mobile-nav__inner{
      display:flex;
      gap:12px;
      overflow-x:auto;
      padding:12px 16px 14px;
      scrollbar-width:none;
      -webkit-overflow-scrolling: touch;
    }
    #${NAV_ID} .hg-mobile-nav__inner::-webkit-scrollbar{display:none;}
    #${NAV_ID} a{
      flex:0 0 auto;
      color:inherit;
      text-decoration:none;
      font:12px/1.1 'FGDC',system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      width:76px;
      aspect-ratio:1/1;
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
    }
    #${NAV_ID} a:focus-visible{
      outline:2px solid #E57200;
      outline-offset:2px;
    }
    #${NAV_ID} .hg-mobile-nav__icon{
      display:flex;
      align-items:center;
      justify-content:center;
      color:inherit;
    }
    #${NAV_ID} svg{
      width:32px;
      height:32px;
    }
    #${NAV_ID} .hg-mobile-nav__label{
      display:block;
      color:inherit;
      text-decoration:none;
      word-break:break-word;
      white-space:normal;
    }
    .hg-mobile-nav-spacer{display:none;}
    @media (max-width: 768px){
      #${NAV_ID}{display:block;}
      .hg-mobile-nav-spacer{display:block;}
    }
  `;
  document.head.appendChild(style);

  const nav = document.createElement('nav');
  nav.id = NAV_ID;
  nav.className = 'hg-mobile-nav';
  nav.setAttribute('aria-label', 'Headway Guard navigation');

  const inner = document.createElement('div');
  inner.className = 'hg-mobile-nav__inner';

  links.forEach(link => {
    const anchor = document.createElement('a');
    anchor.href = link.href;

    const icon = document.createElement('span');
    icon.className = 'hg-mobile-nav__icon';
    icon.innerHTML = link.icon;
    icon.setAttribute('aria-hidden', 'true');

    const label = document.createElement('span');
    label.className = 'hg-mobile-nav__label';
    label.textContent = link.label;

    anchor.appendChild(icon);
    anchor.appendChild(label);
    inner.appendChild(anchor);
  });

  nav.appendChild(inner);
  document.body.appendChild(nav);

  const spacer = document.createElement('div');
  spacer.className = 'hg-mobile-nav-spacer';
  document.body.appendChild(spacer);

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
        el.classList.remove('hg-mobile-nav-scroll');
        return;
      }
      if (el.scrollHeight > el.clientHeight) {
        el.classList.add('hg-mobile-nav-scroll');
      } else {
        el.classList.remove('hg-mobile-nav-scroll');
      }
    });
  };

  const updateSpacerHeight = () => {
    if (window.getComputedStyle(nav).display === 'none') {
      spacer.style.height = '0px';
      document.documentElement.style.setProperty('--hg-mobile-nav-offset', '0px');
      document.body.classList.remove('hg-mobile-nav-active');
      return;
    }
    const navHeight = nav.offsetHeight;
    spacer.style.height = `${navHeight}px`;
    document.documentElement.style.setProperty('--hg-mobile-nav-offset', `${navHeight}px`);
    document.body.classList.add('hg-mobile-nav-active');
    markScrollableContainers();
  };

  updateSpacerHeight();
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
