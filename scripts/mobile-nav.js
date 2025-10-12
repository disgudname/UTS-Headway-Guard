(function(){
  const NAV_ID = 'hg-mobile-nav';
  if (document.getElementById(NAV_ID)) return;

  const params = new URLSearchParams(window.location.search);
  if (params.get('dispatcher') === 'true') return;

  const links = [
    {
      href: '/',
      label: 'Home',
      icon: '/media/home.svg'
    },
    {
      href: '/driver',
      label: 'Driver',
      icon: '/media/driver.svg'
    },
    {
      href: '/dispatcher',
      label: 'Dispatch',
      icon: '/media/dispatcher.svg'
    },
    {
      href: '/servicecrew',
      label: 'Service Crew',
      icon: '/media/servicecrew.svg'
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
      href: '/testmap',
      label: 'Test Map',
      icon: '/media/testmap.svg'
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
      color:#FFFFFF;
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
      outline:2px solid #FFFFFF;
      outline-offset:2px;
    }
    #${NAV_ID} .hg-mobile-nav__icon{
      display:flex;
      align-items:center;
      justify-content:center;
      color:inherit;
    }
    #${NAV_ID} .hg-mobile-nav__icon-img{
      width:32px;
      height:32px;
      display:block;
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

    const img = document.createElement('img');
    img.src = link.icon;
    img.alt = '';
    img.className = 'hg-mobile-nav__icon-img';
    img.setAttribute('aria-hidden', 'true');

    icon.appendChild(img);

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
