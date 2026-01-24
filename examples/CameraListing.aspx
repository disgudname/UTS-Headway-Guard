

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml"  lang="en" xml:lang="en">
<head>    

<!-- Google Tag Manager -->
<script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
})(window,document,'script','dataLayer','GTM-K594K3L');</script>
<!-- End Google Tag Manager -->


<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-9EN3T0JE03"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-9EN3T0JE03');
</script>

<!-- end Google Analytics -->

    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1.0" /><meta name="msapplication-TileColor" content="#1b3c97" /><meta name="theme-color" content="#ffffff" /><link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png" /><link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png" /><link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png" /><link rel="manifest" href="/site.webmanifest" /><link rel="mask-icon" href="/safari-pinned-tab.svg" color="#5bbad5" />

    <!--<link rel="apple-touch-icon-precomposed" href="/images/favicon-57.png?fingerprint=20150512152026" />
    <link rel="shortcut icon" type="image/x-icon" href="/favicon.ico?fingerprint=20220308202954" />-->

    
    
    <link rel="stylesheet" href="/styles/main.min.css?fingerprint=20260106203650" />
    
    <title>
	Traffic Cameras
</title>
    <script type="text/javascript" src="/js/jquery/jquery-3.6.0.min.js"></script>
    <script src="/script/jqyery-easing-v1.3.js?fingerprint=20130927185225"></script>
    <script type="text/javascript" src="/script/jquery_webticker.js?fingerprint=20150512153527"></script>
    <script type="text/javascript">
        
        $(function () {
            GetTickerAlerts();
            $("#pAlert").hover(function () { $("#webticker").webTicker('stop'); });
            $("#pAlert").mouseout(function () { $("#webticker").webTicker('cont'); });
        });
        

        var tickerRefreshTimeout;
        function GetTickerAlertsRefreshed() {
            console.log('Ticker Refreshed');
            clearTimeout(tickerRefreshTimeout);
            tickerRefreshTimeout = setTimeout(function () { GetTickerAlerts() }, 180000);  // 3 minutes
        }

        function GetTickerAlerts() {
            jQuery.ajax(
                {
                    url: "/ticker.aspx",
                    type: "GET",
                    dataType: "html",
                    context: document.body,
                    async: true,
                    cache: false,
                    success: function (msg) {
                        try {
                            //populate
                            $('#txtTickerAlert').val(jQuery(msg).find('#litTicker').html());
                            
                            //if there are any ticker alerts, display the alert count
                            if ($('#txtTickerAlert').val() != "") {
                                var altcount = $('li', $('#txtTickerAlert').val()).length;
                                if (altcount == 1) {
                                    //do nothing.  This is just the default message that there are no alerts.
                                } else {
                                    $('#spnAlertCount').text(" (" + (altcount) + ")");
                                }
                            }

                            //format the text
                            var strText = $("#txtTickerAlert").val();

                            //set the text
                            $("#pAlert").html(strText);

                            //run
                            $("#webticker").webTicker();
                        } catch (e) {
                            //catch this
                        }
                    }
                });
            GetTickerAlertsRefreshed();
        }
    </script>
    <style type="text/css">
        .userInfoMain {
            position: unset !important;
            margin-left: -6px !important;
            width: fit-content !important;
        }
        .userInfoMain h4 {
            text-transform: unset !important;
        }
        @media only screen and (max-width: 750px) {
            .userInfoMain {
                width: 100% !important;
                position: relative !important;
            }
        }
    </style>
    
	<link href ="styles/start/jquery-ui-1.10.3.custom.min.css" rel ="stylesheet" type ="text/css" />
	<script type="text/javascript" src="/js/jquery/jquery-migrate-3.4.0.min.js"></script>
	<script type="text/javascript" src ="/js/jquery/jquery-ui-1.10.4.custom.diag.min.js"></script>
	<script type="text/javascript" src="/js/save.min.js?fingerprint=20220825064825"></script>
	<script type="text/javascript" src="/js/flowplayi.min.js?fingerprint=20230829062857"></script>
	<script type="text/javascript" src="/js/cam_stream_timeout.js?fingerprint=20220709052128"></script>
	<link type ="text/css" href ="/css/CameraListing.min.css?fingerprint=20251015141937" rel ="stylesheet" />
	<script type="text/javascript" src="/js/CameraListing.min.js?fingerprint=20251014173913"></script>
	<style type="text/css">

	</style>
</head>
<body>
<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-K594K3L"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->

    <!-- HEADER -->
    <header id="topHeader" aria-label="website header" tabindex="-1">
        <div class="skip-link" >
            Skip to <a href="#content-area" aria-label="skip to content link">content</a> or <a href="#side-nav-bar" aria-label="skip to main navigation link">navigation</a> or <a href="#footer" aria-label="skip to website footer link">footer</a>
        </div>
        <div id="top-logo" tabindex="-1" role="button" aria-labelledby="header-text">
            <h2 id="header-text">Travel Info to Go</h2>
        </div>
        <div class="right-top-nav" role="navigation" aria-label="Header navigation">
            <ul role="menu" aria-label="header menu">
                <li class="googleTranslate-top-btn"  role="none">
                    <div class="translate-mobile-btn" id="translateMobileBtn" role="button" aria-label="Google translate button" tabindex="0" aria-controls="google_translate_element">T</div>
                    <div class="googleTranslate-wrapper" id="googleEl">
                        <div class="google-mobile-close-btn" id="googleMobileCloseBtn" role="button" aria-label="close Google translate button" tabindex="0" aria-controls="google_translate_element">X</div>
                        <div id="google_translate_element" role="menuitem" aria-label="Google translate"></div>           
                    </div>
                </li>
                <li class="home-top-btn" role="none">
                    <a href="/" style="text-decoration: none;"  aria-label="home button" role="menuitem">
                        <svg xmlns="http://www.w3.org/2000/svg" 
                            width="24" 
                            height="24" 
                            viewBox="0 0 24 24"
                            role="presentation"
                            aria-label="home icon"
                            >
                            <path d="M12 6.453l9 8.375v9.172h-6v-6h-6v6h-6v-9.172l9-8.375zm12 5.695l-12-11.148-12 11.133 1.361 1.465 10.639-9.868 10.639 9.883 1.361-1.465z"/></svg>
                    </a>
                </li>
                <li class="tweeter-top-btn" role="none">
                    <a href="/twitter/default.aspx" style="text-decoration: none;"  aria-label="X button" role="menuitem">
                        <svg width="20" height="20" viewBox="0 0 1200 1227" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M714.163 519.284L1160.89 0H1055.03L667.137 450.887L357.328 0H0L468.492 681.821L0 1226.37H105.866L515.491 750.218L842.672 1226.37H1200L714.137 519.284H714.163ZM569.165 687.828L521.697 619.934L144.011 79.6944H306.615L611.412 515.685L658.88 583.579L1055.08 1150.3H892.476L569.165 687.854V687.828Z" fill="white"/>
                        </svg>  
                    </a>
                </li>
            </ul>
        </div>
        <div role="button" class="drop-top-nav" aria-label="main menu mobile button" aria-controls="side-nav-bar">
            <svg width="100" height="100" viewBox="0 0 100 100" role="presentation" aria-label="mobile menu icon">
                <path class="line line1" d="M 20,29.000046 H 80.000231 C 80.000231,29.000046 94.498839,28.817352 94.532987,66.711331 94.543142,77.980673 90.966081,81.670246 85.259173,81.668997 79.552261,81.667751 75.000211,74.999942 75.000211,74.999942 L 25.000021,25.000058" />
                <path class="line line2" d="M 20,50 H 80" />
                <path class="line line3" d="M 20,70.999954 H 80.000231 C 80.000231,70.999954 94.498839,71.182648 94.532987,33.288669 94.543142,22.019327 90.966081,18.329754 85.259173,18.331003 79.552261,18.332249 75.000211,25.000058 75.000211,25.000058 L 25.000021,74.999942" />
            </svg>
        </div>
    </header>
    <!-- End HEADER -->

    <!-- PAGE WRAPER -->
    <div id="page-wrap">
        <!-- PAGE nav -->
        <aside id="side-nav-bar" aria-label="website main navigation bar">
            <button class="hideNav-Btn" 
                aria-label="main menu toggle button" 
                aria-controls="side-nav-bar" 
                aria-expanded="true" 
                data-tooltip="close navigation" 
                data-position="right" tabindex="0"
                type="button">
                <svg xmlns="http://www.w3.org/2000/svg" 
                    width="24" 
                    height="24" 
                    viewBox="0 0 24 24"
                    role="presentation" 
                    aria-label="arrow icon"
                    >
                    <path d="M16.67 0l2.83 2.829-9.339 9.175 9.339 9.167-2.83 2.829-12.17-11.996z"/></svg>
            </button>

            <!-- Car Switch -->
            <div class="car-mode-toggle-switch" id="carModeContainer">
                <div class="switch-lables-sidenav-x">
                    <div class="passenger">
                        <svg xmlns="http://www.w3.org/2000/svg" 
                            width="20" 
                            height="20" 
                            viewBox="0 0 24 24"
                            role="presentation"
                            >
                            <path d="M11 11v-3h1.247c.882 0 1.235.297 1.828.909.452.465 1.925 2.091 1.925 2.091h-5zm-1-3h-2.243c-.688 0-1.051.222-1.377.581-.316.348-.895.948-1.506 1.671 1.719.644 4.055.748 5.126.748v-3zm14 5.161c0-2.823-2.03-3.41-2.794-3.631-1.142-.331-1.654-.475-3.031-.794-.55-.545-2.052-2.036-2.389-2.376l-.089-.091c-.666-.679-1.421-1.269-3.172-1.269h-7.64c-.547 0-.791.456-.254.944-.534.462-1.944 1.706-2.34 2.108-1.384 1.402-2.291 2.48-2.291 4.603 0 2.461 1.361 4.258 3.179 4.332.41 1.169 1.512 2.013 2.821 2.013 1.304 0 2.403-.838 2.816-2h6.367c.413 1.162 1.512 2 2.816 2 1.308 0 2.409-.843 2.82-2.01 1.934-.056 3.181-1.505 3.181-3.829zm-18 4.039c-.662 0-1.2-.538-1.2-1.2s.538-1.2 1.2-1.2 1.2.538 1.2 1.2-.538 1.2-1.2 1.2zm12 0c-.662 0-1.2-.538-1.2-1.2s.538-1.2 1.2-1.2 1.2.538 1.2 1.2-.538 1.2-1.2 1.2zm2.832-2.15c-.399-1.188-1.509-2.05-2.832-2.05-1.327 0-2.44.868-2.836 2.062h-6.328c-.396-1.194-1.509-2.062-2.836-2.062-1.319 0-2.426.857-2.829 2.04-.586-.114-1.171-1.037-1.171-2.385 0-1.335.47-1.938 1.714-3.199.725-.735 1.31-1.209 2.263-2.026.34-.291.774-.432 1.222-.43h5.173c1.22 0 1.577.385 2.116.892.419.393 2.682 2.665 2.682 2.665s2.303.554 3.48.895c.84.243 1.35.479 1.35 1.71 0 1.196-.396 1.826-1.168 1.888z"/></svg>
                    </div>
                    <div class="commercial">
                        <svg 
                            width="20" 
                            height="20" 
                            viewBox="0 0 24 24" 
                            xmlns="http://www.w3.org/2000/svg" 
                            fill-rule="evenodd" 
                            clip-rule="evenodd"
                            role="presentation"
                            >
                            <path d="M3 18h-2c-.552 0-1-.448-1-1v-13c0-.552.448-1 1-1h13c.552 0 1 .448 1 1v2h4.667c1.117 0 1.6.576 1.936 1.107.594.94 1.536 2.432 2.109 3.378.188.312.288.67.288 1.035v4.48c0 1.089-.743 2-2 2h-1c0 1.656-1.344 3-3 3s-3-1.344-3-3h-6c0 1.656-1.344 3-3 3s-3-1.344-3-3zm3-1.2c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm12 0c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm-3-2.8h-13v2h1.765c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h7.53c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h1.765v-4.575l-1.711-2.929c-.179-.307-.508-.496-.863-.496h-4.426v6zm-2-9h-11v7h11v-7zm3 4v3h5l-1.427-2.496c-.178-.312-.509-.504-.868-.504h-2.705z"/></svg>
                    </div>
                </div>
                <div class="switch-control">
                    <div class="label passenger" role="presentation">
                        <svg xmlns="http://www.w3.org/2000/svg" 
                            width="20" 
                            height="20" 
                            viewBox="0 0 24 24"
                            role="presentation">
                            <path d="M11 11v-3h1.247c.882 0 1.235.297 1.828.909.452.465 1.925 2.091 1.925 2.091h-5zm-1-3h-2.243c-.688 0-1.051.222-1.377.581-.316.348-.895.948-1.506 1.671 1.719.644 4.055.748 5.126.748v-3zm14 5.161c0-2.823-2.03-3.41-2.794-3.631-1.142-.331-1.654-.475-3.031-.794-.55-.545-2.052-2.036-2.389-2.376l-.089-.091c-.666-.679-1.421-1.269-3.172-1.269h-7.64c-.547 0-.791.456-.254.944-.534.462-1.944 1.706-2.34 2.108-1.384 1.402-2.291 2.48-2.291 4.603 0 2.461 1.361 4.258 3.179 4.332.41 1.169 1.512 2.013 2.821 2.013 1.304 0 2.403-.838 2.816-2h6.367c.413 1.162 1.512 2 2.816 2 1.308 0 2.409-.843 2.82-2.01 1.934-.056 3.181-1.505 3.181-3.829zm-18 4.039c-.662 0-1.2-.538-1.2-1.2s.538-1.2 1.2-1.2 1.2.538 1.2 1.2-.538 1.2-1.2 1.2zm12 0c-.662 0-1.2-.538-1.2-1.2s.538-1.2 1.2-1.2 1.2.538 1.2 1.2-.538 1.2-1.2 1.2zm2.832-2.15c-.399-1.188-1.509-2.05-2.832-2.05-1.327 0-2.44.868-2.836 2.062h-6.328c-.396-1.194-1.509-2.062-2.836-2.062-1.319 0-2.426.857-2.829 2.04-.586-.114-1.171-1.037-1.171-2.385 0-1.335.47-1.938 1.714-3.199.725-.735 1.31-1.209 2.263-2.026.34-.291.774-.432 1.222-.43h5.173c1.22 0 1.577.385 2.116.892.419.393 2.682 2.665 2.682 2.665s2.303.554 3.48.895c.84.243 1.35.479 1.35 1.71 0 1.196-.396 1.826-1.168 1.888z"/></svg>
                        <span>passenger</span>
                    </div>
                    <label class="switch" for="carSwitchCH" aria-labelledby="carSwitchCH">
                        <input type="checkbox" name="switch" id="carSwitchCH" aria-label="car mode switch">
                        <span class="slider round" id="carSwitch"></span>
                    </label>
                    <div class="label commercial" role="presentation">
                        <svg 
                            width="20" 
                            height="20" 
                            viewBox="0 0 24 24" 
                            xmlns="http://www.w3.org/2000/svg" 
                            fill-rule="evenodd"
                            clip-rule="evenodd"
                            role="presentation">
                            <path d="M3 18h-2c-.552 0-1-.448-1-1v-13c0-.552.448-1 1-1h13c.552 0 1 .448 1 1v2h4.667c1.117 0 1.6.576 1.936 1.107.594.94 1.536 2.432 2.109 3.378.188.312.288.67.288 1.035v4.48c0 1.089-.743 2-2 2h-1c0 1.656-1.344 3-3 3s-3-1.344-3-3h-6c0 1.656-1.344 3-3 3s-3-1.344-3-3zm3-1.2c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm12 0c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm-3-2.8h-13v2h1.765c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h7.53c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h1.765v-4.575l-1.711-2.929c-.179-.307-.508-.496-.863-.496h-4.426v6zm-2-9h-11v7h11v-7zm3 4v3h5l-1.427-2.496c-.178-.312-.509-.504-.868-.504h-2.705z"/></svg>
                        <span>commercial</span>
                    </div>
                </div>
            </div>
            <!-- End Car Switch -->

            <div class="sideMenuWrapper"  role="navigation" aria-label="website side navigation">
                <ul class="side-nav" role="menu" aria-label="main website menu">
                <li role="menuitem">
                        <span class="main-nav-icon" role="presentation">
                            <svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" fill-rule="evenodd" clip-rule="evenodd" role="presentation" aria-label="Flood Closures">
                                <path d="M8,18.51c-3.18,0-3.77-2-8-2v2c3.62,0,4.31,2,8,2s4.96-2,8-2,4.11,2,8,2v-2c-3.65,0-4.44-2-8-2s-4.98,2-8,2Z"/>
                                <path d="M16,12.81c-3.58,0-4.98,2-8,2-3.18,0-3.77-2-8-2v2c3.62,0,4.31,2,8,2s4.96-2,8-2,4.11,2,8,2v-2c-3.65,0-4.44-2-8-2Z"/>
                                <path d="M3.09,5.15l.93-1.72h-1.89c-.24,0-.43.19-.43.43v.44c0,.68.79.85,1.39.85Z"/>
                                <path d="M22.3,4.3v-.44c0-.24-.19-.43-.43-.43h-1.89l.93,1.72c.59,0,1.39-.17,1.39-.85Z"/>
                                <path d="M2.55,9.59c0,.73.14,1.39.37,2.08,1.87.59,2.82,1.59,5.08,1.59,3.03,0,4.42-2,8-2,2.2,0,3.34.76,4.75,1.34.42-1.06.7-1.97.7-3.01,0-1.21-.26-2.13-.81-3.15-.71-1.32-1.59-2.96-2.52-4.51-.71-1.19-1.08-1.47-1.96-1.62-1.32-.24-2.51-.31-4.16-.31s-2.85.08-4.16.31c-.87.16-1.24.44-1.96,1.62-.93,1.54-1.8,3.19-2.52,4.51-.55,1.02-.81,1.93-.81,3.15ZM15,10.3h-6.01c-.24,0-.43-.19-.43-.43s.19-.43.43-.43h6.01c.24,0,.43.19.43.43s-.19.43-.43.43ZM18.44,7.73c.71,0,1.29.58,1.29,1.29s-.58,1.29-1.29,1.29-1.29-.58-1.29-1.29.58-1.29,1.29-1.29ZM7.44,2.43c.17-.26.44-.43.75-.49,1.3-.23,2.43-.31,3.81-.31s2.52.07,3.81.31c.3.05.57.23.75.49.76,1.13,1.4,2.7,1.4,2.7-1.41.27-3.7.43-5.96.43s-4.55-.16-5.96-.43c0,0,.64-1.57,1.4-2.7ZM5.56,7.73c.71,0,1.29.58,1.29,1.29s-.58,1.29-1.29,1.29-1.29-.58-1.29-1.29.58-1.29,1.29-1.29Z"/>
                            </svg>
                        </span>
                        <a href="/emergencyClosures.aspx" aria-label="emergency closures link">Emergency Closures</a>
                    </li>
                    <li role="menuitem" class="slideNav" tabindex="0" aria-label="travel conditions menu">
                        <span  class="main-nav-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" 
                                width="24"
                                height="24" 
                                viewBox="0 0 24 24"
                                role="presentation" 
                                aria-label="travel conditions icon">
                                <path d="M12 1c-3.148 0-6 2.553-6 5.702 0 3.148 2.602 6.907 6 12.298 3.398-5.391 6-9.15 6-12.298 0-3.149-2.851-5.702-6-5.702zm0 8c-1.105 0-2-.895-2-2s.895-2 2-2 2 .895 2 2-.895 2-2 2zm8 6h-3.135c-.385.641-.798 1.309-1.232 2h3.131l.5 1h-4.264l-.344.544-.289.456h.558l.858 2h-7.488l.858-2h.479l-.289-.456-.343-.544h-2.042l-1.011-1h2.42c-.435-.691-.848-1.359-1.232-2h-3.135l-4 8h24l-4-8zm-12.794 6h-3.97l1.764-3.528 1.516 1.528h1.549l-.859 2zm8.808-2h3.75l1 2h-3.892l-.858-2z"/>
                            </svg>
                        </span>
                        <a href="javascript:void(0);" data-btn="subNav" tabindex="-1">
                            Travel Conditions
                            <div class="nav-icon-arrow">
                                <svg xmlns="http://www.w3.org/2000/svg" 
                                    width="24" 
                                    height="24" 
                                    viewBox="0 0 24 24"
                                    role="presentation" 
                                    aria-label="arrow icon">
                                    <path d="M16.67 0l2.83 2.829-9.339 9.175 9.339 9.167-2.83 2.829-12.17-11.996z"/></svg>
                            </div>
                        </a>
                        
                        <div class="navL2">
                            <ul role="menu" aria-label="travel conditions submenu">
                                <li role="none"><a href="/" role="menuitem" aria-label="Traffic Map link">Traffic Map</a></li>
                                <li role="none"><a href="/TravelConditions/TravelDelay.aspx" role="menuitem" aria-label="Possible Travel Delays link">Possible Travel Delays</a></li>
                                <li role="none"><a href="/TravelConditions/RoadConditions.aspx" role="menuitem" aria-label="Road Conditions link">Road Conditions</a></li>
                                <li role="none"><a href="/TravelConditions/Weather.aspx" role="menuitem" aria-label="Weather Alerts link">Weather Alerts</a></li>
                                <li role="none"><a href="/TravelConditions/WeatherForecast.aspx" role="menuitem" aria-label="Weather Forecast link">Weather Forecast</a></li>
                               
                                <li role="none"><a href="/CameraListing.aspx" role="menuitem" aria-label="Traffic Cameras link">Traffic Cameras</a></li>
                                <li role="none"><a href="/dms.aspx" role="menuitem" aria-label="Signs link">Signs</a></li>
                                
									<!-- <li role="none"><a href="/Winter.aspx" role="menuitem" aria-label="Road Conditions link">Road Conditions</a></li> -->
								 
                                
								<li role="none"><a href="/Facilities.aspx" role="menuitem" aria-label="Rest Areas link">Rest Areas</a></li>                            
                            </ul>
                        </div>
                    </li>
                    <li role="menuitem" class="slideNav personalAlerts-Icon" tabindex="0" aria-label="Personal Alerts menu">
                        <span class="main-nav-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="personal alerts icon">
                                <path d="M16 0c-3.169 0-6 2.113-6 5.003 0 1.025.369 2.032 1.023 2.812.027.916-.512 2.228-.996 3.184 1.301-.235 3.15-.754 3.988-1.268.708.172 1.387.251 2.028.251 3.543 0 5.956-2.418 5.956-4.98.001-2.904-2.85-5.002-5.999-5.002zm-2.5 5.75c-.414 0-.75-.336-.75-.75s.336-.75.75-.75.75.336.75.75-.336.75-.75.75zm2.5 0c-.414 0-.75-.336-.75-.75s.336-.75.75-.75.75.336.75.75-.336.75-.75.75zm2.5 0c-.414 0-.75-.336-.75-.75s.336-.75.75-.75.75.336.75.75-.336.75-.75.75zm-6.5 6.854v5.396h-8v-10h4.766c-.649-1.216-.913-2.564-.674-4h-4.092c-1.105 0-2 .896-2 2v14.678c-.002 2.213 2.503 3.322 6.006 3.322 3.498 0 5.994-1.106 5.994-3.322v-8.725c-.635.257-1.34.476-2 .651zm-4 9.396c-.553 0-1-.448-1-1s.447-1 1-1 1 .448 1 1-.447 1-1 1z"/>
                            </svg>
                        </span>
                        <a href="javascript:void(0);" data-btn="subNav" tabindex="-1">
                            My WV511
                            <div class="nav-icon-arrow">
                                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="arrow icon">
                                    <path d="M16.67 0l2.83 2.829-9.339 9.175 9.339 9.167-2.83 2.829-12.17-11.996z"/>
                                </svg>
                            </div>
                        </a>
                        <div class="navL2">
                            <ul role="menu" aria-label="Personal Alerts submenu">
                                
                                <li role="none"><a href="/personalAlerts/personalAlerts.aspx" role="menuitem" aria-label="Login link">Login</a></li>
                                <li role="none"><a href="/personalAlerts/newUser.aspx" role="menuitem" aria-label="new user link">New User</a></li>    
                                
                            </ul>
                        </div>
                    </li>
                    <li role="menuitem" class="slideNav" tabindex="0" aria-label="using 511 menu">
                        <span class="main-nav-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="using 511 icon">
                                <path d="M21 14.655l-5.799.908 3.393 6.917-3.111 1.52-3.413-6.982-4.07 3.651v-15.857l13 9.843m-12.792-10.905l-1.042.519-1.536-3.087 1.043-.519 1.535 3.087zm2.599-3.577l-1.152-.173-.513 3.409 1.152.173.513-3.409zm-4.779 5.848l.557-1.023-3.028-1.648-.557 1.023 3.028 1.648z"/></svg>
                        </span>
                        <a href="javascript:void(0);" data-btn="subNav" tabindex="-1">
                            Using 511
                            <div class="nav-icon-arrow">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="arrow icon">
                                        <path d="M16.67 0l2.83 2.829-9.339 9.175 9.339 9.167-2.83 2.829-12.17-11.996z"/>
                                    </svg>
                                </div>
                        </a>
                        <div class="navL2">
                            <ul role="menu" aria-label="Using 511 submenu">
                                <li role="none"><a href="/NetworkCoverage/calling511.aspx" role="menuitem" aria-label="calling 511 link">Calling 511</a></li>
                                <li role="none"><a href="/NetworkCoverage/usingWebsite.aspx" role="menuitem" aria-label="using the website link">Using the Website</a></li>
                                <li role="none"><a href="/NetworkCoverage/usingAlertingService.aspx" role="menuitem" aria-label="using the alerting service link">Using the Alerting Service</a></li>
                                <li role="none"><a href="/NetworkCoverage/usingApp.aspx" role="menuitem" aria-label="using the 511 mobile app link">Using the 511 Mobile App</a></li>
                                <li role="none"><a href="/NetworkCoverage/whatComing.aspx" role="menuitem" aria-label="What's Coming to 511">What's Coming to 511</a></li>
                            </ul>
                        </div>
                    </li>
                    <li role="menuitem" class="slideNav" tabindex="0" aria-label="resources menu">
                        <span class="main-nav-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="Resources icon">
                                <path d="M12 0c-6.627 0-12 5.373-12 12s5.373 12 12 12 12-5.373 12-12-5.373-12-12-12zm-2.033 16.01c.564-1.789 1.632-3.932 1.821-4.474.273-.787-.211-1.136-1.74.209l-.34-.64c1.744-1.897 5.335-2.326 4.113.613-.763 1.835-1.309 3.074-1.621 4.03-.455 1.393.694.828 1.819-.211.153.25.203.331.356.619-2.498 2.378-5.271 2.588-4.408-.146zm4.742-8.169c-.532.453-1.32.443-1.761-.022-.441-.465-.367-1.208.164-1.661.532-.453 1.32-.442 1.761.022.439.466.367 1.209-.164 1.661z"/></svg>
                        </span>
                        <a href="javascript:void(0);" data-btn="subNav" tabindex="-1">
                            Resources
                            <div class="nav-icon-arrow">
                                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation" aria-label="arrow icon">
                                    <path d="M16.67 0l2.83 2.829-9.339 9.175 9.339 9.167-2.83 2.829-12.17-11.996z"/>
                                </svg>
                            </div>    
                        </a>
                        <div class="navL2">
                            <ul role="menu" aria-label="Resources submenu">
                                <li role="none"><a href="https://transportation.wv.gov/Pages/default.aspx" target="_blank" role="menuitem" aria-label="department of transportation link">Department of Transportation</a></li>
                                <li role="none"><a href="https://wvtourism.com/" target="_blank" role="menuitem" aria-label="gotowv.com link">GoToWV.com</a></li>
                                <li role="none"><a href="https://www.wvsp.gov/Pages/default.aspx" target="_blank" role="menuitem" aria-label="state police link">State Police</a></li>
                                <li role="none"><a href="/twitter/default.aspx" role="menuitem" aria-label="X link">X (Twitter)</a></li>
                                <li role="none"><a href="/NetworkCoverage/usingApp.aspx" role="menuitem" aria-label="mobile link">Mobile</a></li>
                                <li role="none"><a href="/faq.aspx" role="menuitem" aria-label="faq link">FAQ</a></li>
                                <li role="none"><a href="/Resources.aspx" role="menuitem" aria-label="more link">More</a></li>
                            </ul>
                        </div>
                    </li>
                    <li role="menuitem">
                        <span class="main-nav-icon" role="presentation">
                            <svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" fill-rule="evenodd" clip-rule="evenodd" role="presentation" aria-label="roadwork by districk icon">
                                <path d="M21.167 21h1.833v2h-22v-2h1.833l7.334-20h3.666l7.334 20zm-4.527-7h-9.274l-1.005 3h11.328l-1.049-3zm-2.919-8h-3.471l-1.005 3h5.525l-1.049-3z"/>
                            </svg>
                        </span>
                        <a href="/districtRoadwork.aspx" aria-label="roadwork by district link">Roadwork by District</a>
                    </li>
                    <li role="menuitem" class="commercial_nav" id="com_restrictions">
                        <span class="main-nav-icon" role="presentation">
                            <svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" fill-rule="evenodd" clip-rule="evenodd" role="presentation" aria-label="commercial restrictions icon">
                                <path d="M3 18h-2c-.552 0-1-.448-1-1v-13c0-.552.448-1 1-1h13c.552 0 1 .448 1 1v2h4.667c1.117 0 1.6.576 1.936 1.107.594.94 1.536 2.432 2.109 3.378.188.312.288.67.288 1.035v4.48c0 1.089-.743 2-2 2h-1c0 1.656-1.344 3-3 3s-3-1.344-3-3h-6c0 1.656-1.344 3-3 3s-3-1.344-3-3zm3-1.2c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm12 0c.662 0 1.2.538 1.2 1.2 0 .662-.538 1.2-1.2 1.2-.662 0-1.2-.538-1.2-1.2 0-.662.538-1.2 1.2-1.2zm-3-2.8h-13v2h1.765c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h7.53c.549-.614 1.347-1 2.235-1 .888 0 1.686.386 2.235 1h1.765v-4.575l-1.711-2.929c-.179-.307-.508-.496-.863-.496h-4.426v6zm-2-9h-11v7h11v-7zm3 4v3h5l-1.427-2.496c-.178-.312-.509-.504-.868-.504h-2.705z"></path>
                            </svg>
                        </span>
                        <a href="/com_restrictions.aspx" aria-label="commercial restrictions link">Commercial Restrictions</a>
                    </li>
                </ul>
            </div>
            <div class="banner-wraper" id="banner">
                    <div class="banner">
    
    <a href="/NetworkCoverage/usingApp.aspx" id="mobileAppBannerHome" aria-label="mobile app link">
        <img style="margin-bottom: 2px;" src="/images/511PA_app_ad.png" alt="Mobile App Banner" role="presentation" />
    </a>
</div>
<div class="appBtns">                    
    <a href="https://apps.apple.com/us/app/wv-511-drive-safe/id580605639" target="_blank" id="appleAppStoreBannerHome" aria-label="apple app link">
        <img  src="/images/Apple_appstore.png" alt="apple app store" role="presentation" />
    </a>
    <a href="https://play.google.com/store/apps/details?id=com.infolog.WVDriveSafe" target="_blank" id="googlePlayStoreBannerHome" aria-label="google play store link">
        <img src="/images/google_play.png" alt="google play store" role="presentation" />
    </a>
</div>               
            </div>
        </div>
        <!-- END PAGE nav -->
        <!-- PAGE CONTENT -->
        <main id="content-area">
            <div class="alerts-header-wrapper" role="group" aria-label="Current Alerts">
                <div class="alerts-icon-holder" role="group">
                    <a href="/current_alerts.aspx" title="Current Alerts">
                        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" role="presentation">
                            <path d="M12 0c-6.627 0-12 5.373-12 12s5.373 12 12 12 12-5.373 12-12-5.373-12-12-12zm-1 5h2v10h-2v-10zm1 14.25c-.69 0-1.25-.56-1.25-1.25s.56-1.25 1.25-1.25 1.25.56 1.25 1.25-.56 1.25-1.25 1.25z"/>
                        </svg>
                        <span id="spnAlertCount"></span>
                    </a>
                </div>
                <div class="alert-text" id="divTickerAlerts">
                    <span id="pAlert"></span>
                </div>
            </div>
            <form method="post" action="./CameraListing.aspx" id="form1">
<div class="aspNetHidden">
<input type="hidden" name="__EVENTTARGET" id="__EVENTTARGET" value="" />
<input type="hidden" name="__EVENTARGUMENT" id="__EVENTARGUMENT" value="" />
<input type="hidden" name="__LASTFOCUS" id="__LASTFOCUS" value="" />
<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="/wEPDwUKMTA1OTE2NTI5Nw9kFgJmD2QWAgIBD2QWAgIDD2QWBAIBDxAPFgYeDURhdGFUZXh0RmllbGQFCVJvdXRlTmFtZR4ORGF0YVZhbHVlRmllbGQFCVJvdXRlTmFtZR4LXyFEYXRhQm91bmRnZBAVGQ5TZWxlY3QgYSBSb3V0ZQRJLTY0BEktNjgESS03MARJLTc3BEktNzkESS04MQVJLTQ3MAVVUy0xOQVVUy0zNQVVUy01MAVVUy02MAZVUy0xMTkGVVMtMjE5BlVTLTM0MAZVUy00NjAEV1YtNwRXVi05BVdWLTQzBVdWLTQ2BVdWLTQ4BldWLTcwNQ1DaGVzdG51dFJpZGdlDEVsbWVyIFByaW5jZQ5Vbml2ZXJzaXR5IERyLhUZAARJLTY0BEktNjgESS03MARJLTc3BEktNzkESS04MQVJLTQ3MAVVUy0xOQVVUy0zNQVVUy01MAVVUy02MAZVUy0xMTkGVVMtMjE5BlVTLTM0MAZVUy00NjAEV1YtNwRXVi05BVdWLTQzBVdWLTQ2BVdWLTQ4BldWLTcwNQ1DaGVzdG51dFJpZGdlDEVsbWVyIFByaW5jZQ5Vbml2ZXJzaXR5IERyLhQrAxlnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnFgECAWQCAw8WAh4LXyFJdGVtQ291bnQCHxY+Zg9kFgYCAQ9kFgJmDxUCATAVSS02NDogW0NBQl1JLTY0IEAgNi42ZAIDDxYCHgdWaXNpYmxlaBYCZg8VARVJLTY0OiBbQ0FCXUktNjQgQCA2LjZkAgUPFgIfBGgWAmYPFQMESS02NAZDQU0wMzMVSS02NDogW0NBQl1JLTY0IEAgNi42ZAIBD2QWBgIBD2QWAmYPFQIBMSJJLTY0OiBbQ0FCXUktNjQgQCAxMSBIYWwgR3JlZXIgSS9DZAIDDxYCHwRoFgJmDxUBIkktNjQ6IFtDQUJdSS02NCBAIDExIEhhbCBHcmVlciBJL0NkAgUPFgIfBGgWAmYPFQMESS02NAZDQU0wMTEiSS02NDogW0NBQl1JLTY0IEAgMTEgSGFsIEdyZWVyIEkvQ2QCAg9kFgYCAQ9kFgJmDxUCATIoSS02NDogW0NBQl1JLTY0IEAgMTUgMjl0aCBTdCBleGl0KFJUIDYwKWQCAw8WAh8EaBYCZg8VAShJLTY0OiBb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" />
</div>

<script type="text/javascript">
//<![CDATA[
var theForm = document.forms['form1'];
if (!theForm) {
    theForm = document.form1;
}
function __doPostBack(eventTarget, eventArgument) {
    if (!theForm.onsubmit || (theForm.onsubmit() != false)) {
        theForm.__EVENTTARGET.value = eventTarget;
        theForm.__EVENTARGUMENT.value = eventArgument;
        theForm.submit();
    }
}
//]]>
</script>


<script src="/WebResource.axd?d=Vlyy0_znF8n_FRte7AuWl1xmL3YKLsHZOWgsIgsm9ZhPUupvOsD4lK7LHto-boCMjZNRyQDeMMMQLb75m9p8RW7nabhjZ-gS2QdMm5Pa40A1&amp;t=638901536248157332" type="text/javascript"></script>


<script type="text/javascript">
//<![CDATA[
var myCams = new Array();
myCams[0] = "I-64: [CAB]I-64 @ 6.6||/images/cam_unavailable.jpg|CAM033|1|0|0|";
myCams[1] = "I-64: [CAB]I-64 @ 11 Hal Greer I/C||/images/cam_unavailable.jpg|CAM011|1|0|0|";
myCams[2] = "I-64: [CAB]I-64 @ 15 29th St exit(RT 60)||/images/cam_unavailable.jpg|CAM036|1|0|0|";
myCams[3] = "I-64: [CAB]I-64 @ 15B ||/images/cam_unavailable.jpg|CAM135|1|0|0|";
myCams[4] = "I-64: [CAB]I-64 @ 18.6||/images/cam_unavailable.jpg|CAM091|1|0|0|";
myCams[5] = "I-64: [CAB]I-64 @ 20 Barboursville I/C||/images/cam_unavailable.jpg|CAM045|1|0|0|";
myCams[6] = "I-64: [CAB]I-64 @ 28 Milton I/C||/images/cam_unavailable.jpg|CAM012|1|0|0|";
myCams[7] = "I-64: [PUT]I-64 @ 34 Hurricane Exit||/images/cam_unavailable.jpg|CAM071|1|0|0|";
myCams[8] = "I-64: [PUT]I-64 @ 39 Teays Valley I/C||/images/cam_unavailable.jpg|CAM015|1|0|0|";
myCams[9] = "Junction|||CAM085|0|1|0|<p class='cam_junc_head'>select junction to continue on<small>click prev or next camera to stay on current road</small></p><p class='cam_junc_body'><a href='CameraListing.aspx?ROUTE=US-35&CAM=CAM085'>US-35: [PUT]US-35 @ WV-34 I/C</a></p>";
myCams[10] = "I-64: [PUT]I-64 @ 43||/images/cam_unavailable.jpg|CAM115|1|0|0|";
myCams[11] = "I-64: [PUT]I-64 @ 44 St. Albans||/images/cam_unavailable.jpg|CAM062|1|0|0|";
myCams[12] = "I-64: [PUT]I-64 @ 45 Nitro||/images/cam_unavailable.jpg|CAM027|1|0|0|";
myCams[13] = "I-64: [KAN]I-64 @ 46||/images/cam_unavailable.jpg|CAM120|1|0|0|";
myCams[14] = "I-64: [KAN]I-64 WB @ MM 46.6||/images/cam_unavailable.jpg|CAM136|1|0|0|";
myCams[15] = "I-64: [KAN]I-64 @ 47 Crosslanes||/images/cam_unavailable.jpg|CAM022|1|0|0|";
myCams[16] = "I-64: [KAN]I-64 @ 50 Institute||/images/cam_unavailable.jpg|CAM097|1|0|0|";
myCams[17] = "I-64: [KAN]I-64 @ 53 Dunbar||/images/cam_unavailable.jpg|CAM009|1|0|0|";
myCams[18] = "I-64: [KAN]I-64 @ 55||/images/cam_unavailable.jpg|CAM122|1|0|0|";
myCams[19] = "I-64: [KAN]I-64 @ 55.2 Montrose I/C||/images/cam_unavailable.jpg|CAM013|1|0|0|";
myCams[20] = "I-64: [KAN]I-64 MM 56.4||/images/cam_unavailable.jpg|CAM048|1|0|0|";
myCams[21] = "I-64: [KAN]I-64 @ 58 Oakwood I/C||/images/cam_unavailable.jpg|CAM014|1|0|0|";
myCams[22] = "Junction|||CAM121|0|1|0|<p class='cam_junc_head'>select junction to continue on<small>click prev or next camera to stay on current road</small></p><p class='cam_junc_body'><a href='CameraListing.aspx?ROUTE=US-119&CAM=CAM121'>US-119: [KAN]US-119 to I-64 I/C </a></p><p class='cam_junc_body'><a href='CameraListing.aspx?ROUTE=US-119&CAM=CAM113'>US-119: [KAN]US-119 @ Eureka Rd</a></p><p class='cam_junc_body'><a href='CameraListing.aspx?ROUTE=US-119&CAM=CAM057'>US-119: [KAN]US119 @ Trace Fork</a></p>";
myCams[23] = "I-64: [KAN]I-64 @ 58.2 Lotto bld 1||/images/cam_unavailable.jpg|CAM038|1|0|0|";
myCams[24] = "I-64: [KAN]I-64 @ 58.3 Lotto bld 2||/images/cam_unavailable.jpg|CAM042|1|0|0|";
myCams[25] = "Junction|||CAM079|0|1|0|<p class='cam_junc_head'>select junction to continue on<small>click prev or next camera to stay on current road</small></p><p class='cam_junc_body'><a href='CameraListing.aspx?ROUTE=I-77&CAM=CAM079'>I-77: [KAN]I-77 @ 100</a></p>";
myCams[26] = "I-64: [KAN]I-64 @ 58.5 Bigley I/C||/images/cam_unavailable.jpg|CAM089|1|0|0|";
myCams[27] = "I-64: [RAL]I-64 @ 124||/images/cam_unavailable.jpg|CAM082|1|0|0|";
myCams[28] = "I-64: [GRE]I-64 @ 179||/images/cam_unavailable.jpg|CAM083|1|0|0|";
//]]>
</script>

<script src="/ScriptResource.axd?d=0aMCMFMiEOkUJq9VoWGiaMoGtkXOlc8r7xVkafNe-evyqg_U5s6bUS66MKM_HXSk7LsLur72MwyBTS2c1UL4Zv_tydcBU0nLzHT5NhFKFj7hb1W3tymP8ElmjKpFg1fS5BVwhI2WrlfOBIkT3jmVPKYmhdekcm-Y_DeHQWPId3A1&amp;t=5c0e0825" type="text/javascript"></script>
<script src="/ScriptResource.axd?d=A6OJjPx-QbbnGAuWpKkCZ6gji75DlnBWpev_Z1Ch0LyPX89qJfVvoBeSOoPn02C6zEOLKrBs7NqIHElztAe8PtYlbfMys4S7u0BWvEcSGSAyuhMdXBjiwwbFM5gU0N-VKB9K7fOUMHnIHZN6In7kJgrx4yDPIc7vFrMUyNz8mGFG0a1Mvsc_tZMJxVcR_QFZ0&amp;t=5c0e0825" type="text/javascript"></script>
<div class="aspNetHidden">

	<input type="hidden" name="__VIEWSTATEGENERATOR" id="__VIEWSTATEGENERATOR" value="EA939269" />
	<input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="/wEdABtZCllf+NM0VAhnmt6a6icfzPol72o2whk5u11LZ7vEYKpW0AIzgfVlsKvv+mKXjlI2x331bt7J4CS7+Cr7MUi0chXdVnSnEIx7KDQTlCYisz8EsEwf5wtDaSH+hKDaK6oU5OWnXYEQ8dfXwJQ2L1OnwK4CHvFzxgGfWYmcNNkLgwYZkJcL8jvL8wDTgLnIuJ5gtisI6ob+IjKKOtPaI/7IyQa931gKgjzBMlR51GbFrhp1+C8pTP73A8UFg9rJT7xGca2EC6vxZ0U+38f4SaIWyYMZgnnaS96T2XPl9YbjVzpACpyDzvCrj3o8VpRsaeTRmrFuZyD/g3Xl51oYoHxbVMpYeYhkxdbf5aI8OWfVIjI5QR9CGmyMhjDRLx/dDSQ5ZtOzqGGjLde7+G04rMPMhsfWtL0EVU4L9+7iqfrBApj9eQNbApNpuZmw89LntJxzUBRhOCBrz1Yt/9hmZ8vh7xlVawdXgGMKkgxLyHSGnpEIY2RCsptfvNikVa/uck+8jM5qCVDH1k0ol73GkpcyElGt6SUyAEAkO6UGIi6/Ixb6EmOAvybWnfZzEmoJZhqY2ktkJKi5kERMgSn92l6s783/9NPCsDndtKs0rlSvvQ==" />
</div>
                <script type="text/javascript">
//<![CDATA[
Sys.WebForms.PageRequestManager._initialize('ctl00$smMain', 'form1', [], [], [], 90, 'ctl00');
//]]>
</script>

                

	<section class="content-wrapper">  
		<h1> Traffic Cameras </h1>

		<div id ="weather-alert-section">
		
			<div class="travel-controls">
				<div><h5>Select Route:</h5><select name="ctl00$ContentPlaceHolder1$ddlRoutes" onchange="javascript:setTimeout(&#39;__doPostBack(\&#39;ctl00$ContentPlaceHolder1$ddlRoutes\&#39;,\&#39;\&#39;)&#39;, 0)" id="ddlRoutes">
	<option value="">Select a Route</option>
	<option selected="selected" value="I-64">I-64</option>
	<option value="I-68">I-68</option>
	<option value="I-70">I-70</option>
	<option value="I-77">I-77</option>
	<option value="I-79">I-79</option>
	<option value="I-81">I-81</option>
	<option value="I-470">I-470</option>
	<option value="US-19">US-19</option>
	<option value="US-35">US-35</option>
	<option value="US-50">US-50</option>
	<option value="US-60">US-60</option>
	<option value="US-119">US-119</option>
	<option value="US-219">US-219</option>
	<option value="US-340">US-340</option>
	<option value="US-460">US-460</option>
	<option value="WV-7">WV-7</option>
	<option value="WV-9">WV-9</option>
	<option value="WV-43">WV-43</option>
	<option value="WV-46">WV-46</option>
	<option value="WV-48">WV-48</option>
	<option value="WV-705">WV-705</option>
	<option value="ChestnutRidge">ChestnutRidge</option>
	<option value="Elmer Prince">Elmer Prince</option>
	<option value="University Dr.">University Dr.</option>

</select></div>
				<div>
					<ul class="control-buttons">
						<li  data-tooltip="Save selections" data-position="bottom">
							<a href ="javascript:void(0);" aria-label="save selections" onclick ="savePageView('PAGEVIEW_CAMS');">
								<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="M13 3h2.996v5h-2.996v-5zm11 1v20h-24v-24h20l4 4zm-17 5h10v-7h-10v7zm15-4.171l-2.828-2.829h-.172v9h-14v-9h-3v20h20v-17.171z"/></svg>
							</a>
						</li>
						<li data-tooltip="Clear saved selections" data-position="bottom">
							<a href ="javascript:void(0);" aria-label="Clear saved selections" onclick ="clearPageView('PAGEVIEW_CAMS');">
								<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="M9 19c0 .552-.448 1-1 1s-1-.448-1-1v-10c0-.552.448-1 1-1s1 .448 1 1v10zm4 0c0 .552-.448 1-1 1s-1-.448-1-1v-10c0-.552.448-1 1-1s1 .448 1 1v10zm4 0c0 .552-.448 1-1 1s-1-.448-1-1v-10c0-.552.448-1 1-1s1 .448 1 1v10zm5-17v2h-20v-2h5.711c.9 0 1.631-1.099 1.631-2h5.315c0 .901.73 2 1.631 2h5.712zm-3 4v16h-14v-16h-2v18h18v-18h-2z"/></svg>
							</a>
						</li>
					</ul>
					<div id="alertTxt" class="alert-msg"></div>
				</div>
			</div>


			<div class="content-background">
				
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(0);return false;">I-64: [CAB]I-64 @ 6.6</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(1);return false;">I-64: [CAB]I-64 @ 11 Hal Greer I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(2);return false;">I-64: [CAB]I-64 @ 15 29th St exit(RT 60)</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(3);return false;">I-64: [CAB]I-64 @ 15B </a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(4);return false;">I-64: [CAB]I-64 @ 18.6</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(5);return false;">I-64: [CAB]I-64 @ 20 Barboursville I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(6);return false;">I-64: [CAB]I-64 @ 28 Milton I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(7);return false;">I-64: [PUT]I-64 @ 34 Hurricane Exit</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(8);return false;">I-64: [PUT]I-64 @ 39 Teays Valley I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							
							
							<div id="Div3">
								&nbsp; &nbsp; Junction &nbsp; &gt;&gt;&gt; &nbsp; <a href="CameraListing.aspx?ROUTE=US-35&CAM=CAM085">US-35: [PUT]US-35 @ WV-34 I/C</a>
							</div>
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(10);return false;">I-64: [PUT]I-64 @ 43</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(11);return false;">I-64: [PUT]I-64 @ 44 St. Albans</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(12);return false;">I-64: [PUT]I-64 @ 45 Nitro</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(13);return false;">I-64: [KAN]I-64 @ 46</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(14);return false;">I-64: [KAN]I-64 WB @ MM 46.6</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(15);return false;">I-64: [KAN]I-64 @ 47 Crosslanes</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(16);return false;">I-64: [KAN]I-64 @ 50 Institute</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(17);return false;">I-64: [KAN]I-64 @ 53 Dunbar</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(18);return false;">I-64: [KAN]I-64 @ 55</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(19);return false;">I-64: [KAN]I-64 @ 55.2 Montrose I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(20);return false;">I-64: [KAN]I-64 MM 56.4</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(21);return false;">I-64: [KAN]I-64 @ 58 Oakwood I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							
							
							<div id="Div3">
								&nbsp; &nbsp; Junction &nbsp; &gt;&gt;&gt; &nbsp; <a href="CameraListing.aspx?ROUTE=US-119&CAM=CAM121">US-119: [KAN]US-119 to I-64 I/C </a>
							</div>
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							
							
							<div id="Div3">
								&nbsp; &nbsp; Junction &nbsp; &gt;&gt;&gt; &nbsp; <a href="CameraListing.aspx?ROUTE=US-119&CAM=CAM113">US-119: [KAN]US-119 @ Eureka Rd</a>
							</div>
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							
							
							<div id="Div3">
								&nbsp; &nbsp; Junction &nbsp; &gt;&gt;&gt; &nbsp; <a href="CameraListing.aspx?ROUTE=US-119&CAM=CAM057">US-119: [KAN]US119 @ Trace Fork</a>
							</div>
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(23);return false;">I-64: [KAN]I-64 @ 58.2 Lotto bld 1</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(24);return false;">I-64: [KAN]I-64 @ 58.3 Lotto bld 2</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							
							
							<div id="Div3">
								&nbsp; &nbsp; Junction &nbsp; &gt;&gt;&gt; &nbsp; <a href="CameraListing.aspx?ROUTE=I-77&CAM=CAM079">I-77: [KAN]I-77 @ 100</a>
							</div>
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(26);return false;">I-64: [KAN]I-64 @ 58.5 Bigley I/C</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(27);return false;">I-64: [RAL]I-64 @ 124</a>
							</div>
							
							
						</div>
					
						<div style=" padding-top :10px;padding-bottom :10px;">
							<div id="Div1">
								<a href="#" onclick="LoadCamera(28);return false;">I-64: [GRE]I-64 @ 179</a>
							</div>
							
							
						</div>
					
			</div>

		</div>
		
	</section>

                <div style="display: none;">
                    <input name="ctl00$txtTickerAlert" type="text" value="&lt;ul id=&quot;webticker&quot;>&lt;li id=&quot;item0&quot;>&lt;a href=&quot;/webmap_ticker.aspx?VIEWTYPE=I&amp;amp;LAT=39.33133000000000&amp;amp;LNG=-80.34114000000000&amp;amp;ZOOM=13&amp;amp;ID=eb12f2d3fc3d67310a31f5a009c64a59&quot;>Attention Travelers: Road Closure on CR-19/31 (Hepzibah Heights Road) . The northbound road is closed. The southbound road is closed.&lt;/a>&lt;/li>&lt;/ul>" id="txtTickerAlert" />
                </div>
            </form>
        </main>
        <!-- END PAGE CONTENT -->
    </div>
    <!-- END PAGE WRAPER -->

    <!-- FOOTER -->
    <footer id="footer" tabindex="0" aria-label="website footer">
        <ul class="footer-nav" role="menu" aria-label="footer menu">
            <li role="none"><a href="/default.aspx" role="menuitem">Home</a></li>
            <li role="none"><a href="/aboutUs.aspx" role="menuitem">About</a></li>
            <li role="none"><a href="/FeedBack.aspx" role="menuitem">Contact</a></li>
            <li role="none"><a href="/faq.aspx" role="menuitem">FAQ</a></li>
            <li role="none"><a href="https://www.wv.gov/policies/Pages/Accessibility.aspx" target="_blank" role="menuitem">Accessibility</a></li>
            <li role="none"><a href="http://www.wv.gov/" target="_blank" role="menuitem">WV.GOV</a></li>    
        </ul>
    </footer>
    <!-- END FOOTER -->
    
    
    <script type="text/javascript"  src="/script/main2.min.js"></script>
    <span style="display:none;">511WVProd_web01_OHI</span>
	<script type="text/javascript" src="/js/awstats_misc_tracker.js?fingerprint=20220623181121"></script>
	<noscript><img src="/js/awstats_misc_tracker.js?nojs=y" alt="hidden image"  aria-label="hidden image" role="presentation" height="0" width="0" border="0" style="display: none"  /></noscript>
    <script type="text/javascript">
        (function (i, s, o, g, r, a, m) {
            i['GoogleAnalyticsObject'] = r; i[r] = i[r] || function () {
                (i[r].q = i[r].q || []).push(arguments)
            }, i[r].l = 1 * new Date(); a = s.createElement(o),
            m = s.getElementsByTagName(o)[0]; a.async = 1; a.src = g; m.parentNode.insertBefore(a, m)
        })(window, document, 'script', '//www.google-analytics.com/analytics.js', 'ga');

		$(window).on('load', function() {
			// Call analytics after page load to prevent page blocking
			ga('create', 'UA-47268490-2', 'www.wv511.org');
			ga('send', 'pageview');
		});

    </script>
	<script type="text/javascript">
		function googleTranslateElementInit() {
			new google.translate.TranslateElement({ pageLanguage: 'en'}, 'google_translate_element');
		}
	</script>
	<script async=async type="text/javascript" src="//translate.google.com/translate_a/element.js?cb=googleTranslateElementInit"></script>

<script type="text/javascript" src="/script/passengerCommercialSwitch.min.js"></script>

    <script type="text/javascript">
        $(function () {
            try { toggleCommercialNav(); } catch (err) { }
        });
        function toggleCommercialNav() {
            if ($('#carSwitchCH').is(':checked')) {
                //try { $('.commercial_nav').css('display', 'block'); } catch (err) { }
                try { $('#com_restrictions').show(); } catch (err) { }
            } else {
                //try { $('.commercial_nav').css('display', 'none'); } catch (err) { }
                try { $('#com_restrictions').hide(); } catch (err) { }
            }
        }
</script>

</body>
</html>