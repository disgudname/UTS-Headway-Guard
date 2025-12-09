// marker-selection-menu.js
// Handles overlapping marker selection with circular menu

(function(global) {
    'use strict';

    // Configuration
    const OVERLAP_THRESHOLD_PX = 40; // Pixels to consider markers overlapping
    const MENU_ITEM_SIZE = 120; // Size of each menu circle
    const MENU_RADIUS = 140; // Radius of the circular menu
    const MIN_ITEMS_FOR_MENU = 2; // Minimum overlapping items to show menu
    const TOO_MANY_ITEMS_THRESHOLD = 8; // Show "zoom in" message when this many or more items
    const DEFAULT_MAX_ZOOM = 19; // Default max zoom level for Leaflet maps

    let map = null;
    let currentMenu = null;
    let currentZoomMessage = null; // Track the "zoom in" message element
    let markerRegistry = new Map(); // Maps marker layer IDs to metadata

    /**
     * Initialize the marker selection menu system
     * @param {L.Map} leafletMap - The Leaflet map instance
     */
    function init(leafletMap) {
        if (!leafletMap) {
            throw new Error('MarkerSelectionMenu.init requires a Leaflet map instance');
        }
        map = leafletMap;

        // Close menu when map is clicked
        map.on('click', function(e) {
            if (currentMenu && !e.originalEvent.target.closest('.marker-selection-menu')) {
                closeMenu();
            }
        });

        // Close menu on escape key
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && currentMenu) {
                closeMenu();
            }
        });
    }

    /**
     * Register a marker for overlap detection
     * @param {L.Marker} marker - Leaflet marker
     * @param {Object} metadata - Marker metadata
     * @param {string} metadata.type - 'stop', 'incident', or 'vehicle'
     * @param {string} metadata.color - Marker color (CSS color, fallback if no routes)
     * @param {Array} metadata.routeIds - Array of route IDs (for pie chart)
     * @param {Array} metadata.catRouteKeys - Array of CAT route keys (for pie chart)
     * @param {string} metadata.label - Text label for menu
     * @param {Function} metadata.onClick - Function to call when selected
     */
    function registerMarker(marker, metadata) {
        if (!marker || !metadata) return;

        const id = L.Util.stamp(marker);
        markerRegistry.set(id, {
            marker: marker,
            type: metadata.type || 'unknown',
            color: metadata.color || '#0f172a',
            routeIds: metadata.routeIds || [],
            catRouteKeys: metadata.catRouteKeys || [],
            label: metadata.label || 'Unknown',
            onClick: metadata.onClick || (() => {})
        });
    }

    /**
     * Unregister a marker
     * @param {L.Marker} marker - Leaflet marker to unregister
     */
    function unregisterMarker(marker) {
        if (!marker) return;
        const id = L.Util.stamp(marker);
        markerRegistry.delete(id);
    }

    /**
     * Check if the map is at its maximum zoom level
     * @returns {boolean} - True if at max zoom
     */
    function isAtMaxZoom() {
        if (!map) return false;
        const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : 0;
        const maxZoom = typeof map.getMaxZoom === 'function' ? map.getMaxZoom() : DEFAULT_MAX_ZOOM;
        return currentZoom >= maxZoom;
    }

    /**
     * Handle marker click - detect overlaps and show menu if needed
     * @param {L.LatLng} latlng - Click location
     * @param {Array} items - Array of clickable items at this location (already filtered for overlap)
     * @returns {boolean} - True if menu was shown, false otherwise
     */
    function handleMarkerClick(latlng, items) {
        if (!map || !items || items.length < MIN_ITEMS_FOR_MENU) {
            return false;
        }

        // Items are already filtered for overlap by the caller
        // Convert latlng to container point for menu positioning
        const clickPoint = map.latLngToContainerPoint(latlng);

        // Check if there are too many items and user should zoom in
        // Exception: if already at max zoom, show the menu anyway
        if (items.length >= TOO_MANY_ITEMS_THRESHOLD && !isAtMaxZoom()) {
            showZoomInMessage(latlng, clickPoint);
            return true;
        }

        // Show selection menu
        showSelectionMenu(latlng, clickPoint, items);
        return true;
    }

    /**
     * Find all markers overlapping at a given point
     * @param {L.Point} point - Container point to check
     * @param {Array} items - Items to check for overlap
     * @returns {Array} - Overlapping items
     */
    function findOverlappingMarkers(point, items) {
        const overlapping = [];

        items.forEach(item => {
            const itemPoint = map.latLngToContainerPoint(item.latlng);
            const distance = point.distanceTo(itemPoint);

            if (distance <= OVERLAP_THRESHOLD_PX) {
                overlapping.push(item);
            }
        });

        return overlapping;
    }

    /**
     * Show the circular selection menu
     * @param {L.LatLng} latlng - Map coordinates
     * @param {L.Point} point - Container coordinates
     * @param {Array} items - Items to show in menu
     */
    function showSelectionMenu(latlng, point, items) {
        closeMenu(); // Close any existing menu

        const menuContainer = document.createElement('div');
        menuContainer.className = 'marker-selection-menu';
        menuContainer.style.cssText = `
            position: absolute;
            left: ${point.x}px;
            top: ${point.y}px;
            transform: translate(-50%, -50%);
            z-index: 10000;
            pointer-events: none;
        `;

        // Create menu items in a circle
        items.forEach((item, index) => {
            const angle = (index / items.length) * 2 * Math.PI;
            const x = Math.cos(angle) * MENU_RADIUS;
            const y = Math.sin(angle) * MENU_RADIUS;

            const menuItem = createMenuItem(item, x, y);
            menuContainer.appendChild(menuItem);
        });

        // Add to map container
        const mapContainer = map.getContainer();
        mapContainer.appendChild(menuContainer);
        currentMenu = menuContainer;

        // Animate in
        requestAnimationFrame(() => {
            menuContainer.classList.add('visible');
        });

        // Update position on map move
        const updatePosition = () => {
            if (!currentMenu) return;
            const newPoint = map.latLngToContainerPoint(latlng);
            menuContainer.style.left = `${newPoint.x}px`;
            menuContainer.style.top = `${newPoint.y}px`;
        };

        map.on('move', updatePosition);
        map.on('zoom', closeMenu);

        // Store cleanup function
        menuContainer._cleanup = () => {
            map.off('move', updatePosition);
            map.off('zoom', closeMenu);
        };
    }

    /**
     * Collect colors from route IDs and CAT route keys
     * @param {Array} routeIds - Array of route IDs
     * @param {Array} catRouteKeys - Array of CAT route keys
     * @returns {Array} - Array of CSS colors
     */
    function collectRouteColors(routeIds, catRouteKeys) {
        const colors = [];

        // Collect colors from regular routes
        if (Array.isArray(routeIds)) {
            routeIds.forEach(routeId => {
                if (typeof window.getRouteColor === 'function') {
                    const color = window.getRouteColor(routeId);
                    if (color) {
                        const normalized = color.startsWith('#') ? color : `#${color}`;
                        colors.push(normalized);
                    }
                }
            });
        }

        // Collect colors from CAT routes
        if (Array.isArray(catRouteKeys)) {
            catRouteKeys.forEach(routeKey => {
                if (typeof window.getCatRouteColor === 'function') {
                    const color = window.getCatRouteColor(routeKey);
                    if (color) {
                        const normalized = color.startsWith('#') ? color : `#${color}`;
                        colors.push(normalized);
                    }
                }
            });
        }

        return colors;
    }

    /**
     * Build a conic gradient from multiple colors for pie chart effect
     * @param {Array} colors - Array of CSS colors
     * @returns {string} - CSS conic-gradient string
     */
    function buildPieChartGradient(colors) {
        if (!colors || colors.length === 0) {
            return '#0f172a';
        }

        if (colors.length === 1) {
            return colors[0];
        }

        const segmentSize = 360 / colors.length;
        const segments = colors.map((color, index) => {
            const start = segmentSize * index;
            const end = segmentSize * (index + 1);
            return `${color} ${start}deg ${end}deg`;
        });

        return `conic-gradient(${segments.join(', ')})`;
    }

    /**
     * Create a single menu item
     * @param {Object} item - Item data
     * @param {number} x - X offset from center
     * @param {number} y - Y offset from center
     * @returns {HTMLElement} - Menu item element
     */
    function createMenuItem(item, x, y) {
        // Determine background: use pie chart if multiple routes, otherwise use single color
        let background = item.color || '#0f172a';

        // Check if we have route information for pie chart
        const hasRoutes = (Array.isArray(item.routeIds) && item.routeIds.length > 0) ||
                          (Array.isArray(item.catRouteKeys) && item.catRouteKeys.length > 0);

        if (hasRoutes) {
            const colors = collectRouteColors(item.routeIds || [], item.catRouteKeys || []);
            if (colors.length > 0) {
                background = buildPieChartGradient(colors);
            }
        }

        // Use larger font for buses (vehicles with routes), smaller for ondemand/stops
        const fontSize = (item.type === 'vehicle' && hasRoutes) ? '15px' : '12px';

        const itemEl = document.createElement('button');
        itemEl.className = 'marker-selection-menu-item';
        itemEl.style.cssText = `
            position: absolute;
            left: ${x}px;
            top: ${y}px;
            transform: translate(-50%, -50%);
            width: ${MENU_ITEM_SIZE}px;
            height: ${MENU_ITEM_SIZE}px;
            border-radius: 50%;
            background: ${background};
            border: 3px solid rgba(255, 255, 255, 0.9);
            color: #ffffff;
            font-family: 'FGDC', sans-serif;
            font-size: ${fontSize};
            font-weight: 700;
            text-align: center;
            cursor: pointer;
            pointer-events: auto;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.3);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 8px;
            line-height: 1.2;
            word-wrap: break-word;
            word-break: break-word;
            overflow-wrap: break-word;
            overflow: hidden;
        `;

        // Add label
        // Ensure button has no textContent to prevent duplicate text
        itemEl.textContent = '';
        const label = document.createElement('span');
        label.textContent = item.label || '';
        label.style.cssText = `
            display: block;
            max-width: 100%;
            width: 100%;
            overflow: hidden;
            word-wrap: break-word;
            word-break: break-word;
            overflow-wrap: break-word;
            hyphens: auto;
            line-height: 1.2;
        `;
        itemEl.appendChild(label);

        // Add hover effect
        itemEl.addEventListener('mouseenter', () => {
            itemEl.style.transform = 'translate(-50%, -50%) scale(1.1)';
            itemEl.style.boxShadow = '0 6px 16px rgba(15, 23, 42, 0.4)';
        });

        itemEl.addEventListener('mouseleave', () => {
            itemEl.style.transform = 'translate(-50%, -50%)';
            itemEl.style.boxShadow = '0 4px 12px rgba(15, 23, 42, 0.3)';
        });

        // Handle click
        itemEl.addEventListener('click', (e) => {
            e.stopPropagation();
            if (item.onClick) {
                item.onClick();
            }
            closeMenu();
        });

        return itemEl;
    }

    /**
     * Close the current selection menu
     */
    function closeMenu() {
        if (currentMenu) {
            if (currentMenu._cleanup) {
                currentMenu._cleanup();
            }
            currentMenu.remove();
            currentMenu = null;
        }

        // Also close any zoom message
        closeZoomMessage();
    }

    /**
     * Close the current zoom-in message
     */
    function closeZoomMessage() {
        if (!currentZoomMessage) return;

        if (currentZoomMessage._cleanup) {
            currentZoomMessage._cleanup();
        }

        currentZoomMessage.remove();
        currentZoomMessage = null;
    }

    /**
     * Show a "zoom in to interact" message
     * @param {L.LatLng} latlng - Map coordinates
     * @param {L.Point} point - Container coordinates
     */
    function showZoomInMessage(latlng, point) {
        closeMenu(); // Close any existing menu or message

        const messageContainer = document.createElement('div');
        messageContainer.className = 'marker-zoom-message';
        messageContainer.style.cssText = `
            position: absolute;
            left: ${point.x}px;
            top: ${point.y}px;
            transform: translate(-50%, -50%);
            z-index: 10000;
            pointer-events: none;
            background: rgba(15, 23, 42, 0.9);
            color: #ffffff;
            font-family: 'FGDC', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 14px;
            font-weight: 500;
            padding: 12px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.3);
            white-space: nowrap;
            opacity: 0;
            transition: opacity 0.2s ease;
        `;

        messageContainer.textContent = 'Zoom in to interact with markers';

        // Add to map container
        const mapContainer = map.getContainer();
        mapContainer.appendChild(messageContainer);
        currentZoomMessage = messageContainer;

        // Animate in
        requestAnimationFrame(() => {
            messageContainer.style.opacity = '1';
        });

        // Update position on map move
        const updatePosition = () => {
            if (!currentZoomMessage) return;
            const newPoint = map.latLngToContainerPoint(latlng);
            messageContainer.style.left = `${newPoint.x}px`;
            messageContainer.style.top = `${newPoint.y}px`;
        };

        // Close on zoom (user is zooming in as requested)
        const handleZoom = () => {
            closeZoomMessage();
        };

        map.on('move', updatePosition);
        map.on('zoom', handleZoom);

        // Store cleanup function
        messageContainer._cleanup = () => {
            map.off('move', updatePosition);
            map.off('zoom', handleZoom);
        };

        // Auto-dismiss after 3 seconds
        const dismissTimeout = setTimeout(() => {
            closeZoomMessage();
        }, 3000);

        // Clear timeout on manual close
        const originalCleanup = messageContainer._cleanup;
        messageContainer._cleanup = () => {
            clearTimeout(dismissTimeout);
            originalCleanup();
        };
    }

    /**
     * Check if markers at a location overlap
     * @param {L.LatLng} latlng - Location to check
     * @param {Array} markers - Array of markers to check
     * @returns {Array} - Overlapping markers
     */
    function getOverlappingMarkers(latlng, markers) {
        if (!map || !markers || markers.length === 0) {
            return [];
        }

        const point = map.latLngToContainerPoint(latlng);
        const overlapping = [];

        markers.forEach(marker => {
            const markerLatLng = marker.getLatLng ? marker.getLatLng() : latlng;
            const markerPoint = map.latLngToContainerPoint(markerLatLng);
            const distance = point.distanceTo(markerPoint);

            if (distance <= OVERLAP_THRESHOLD_PX) {
                overlapping.push(marker);
            }
        });

        return overlapping;
    }

    // Export API
    global.MarkerSelectionMenu = {
        init: init,
        registerMarker: registerMarker,
        unregisterMarker: unregisterMarker,
        handleMarkerClick: handleMarkerClick,
        getOverlappingMarkers: getOverlappingMarkers,
        closeMenu: closeMenu
    };

})(typeof window !== 'undefined' ? window : globalThis);
