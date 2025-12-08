// marker-selection-menu.js
// Handles overlapping marker selection with circular menu

(function(global) {
    'use strict';

    // Configuration
    const OVERLAP_THRESHOLD_PX = 40; // Pixels to consider markers overlapping
    const MENU_ITEM_SIZE = 50; // Size of each menu circle
    const MENU_RADIUS = 80; // Radius of the circular menu
    const MIN_ITEMS_FOR_MENU = 2; // Minimum overlapping items to show menu

    let map = null;
    let currentMenu = null;
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
     * @param {string} metadata.color - Marker color (CSS color)
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
     * Create a single menu item
     * @param {Object} item - Item data
     * @param {number} x - X offset from center
     * @param {number} y - Y offset from center
     * @returns {HTMLElement} - Menu item element
     */
    function createMenuItem(item, x, y) {
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
            background: ${item.color || '#0f172a'};
            border: 3px solid rgba(255, 255, 255, 0.9);
            color: #ffffff;
            font-family: 'FGDC', sans-serif;
            font-size: 11px;
            font-weight: 700;
            text-align: center;
            cursor: pointer;
            pointer-events: auto;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.3);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 4px;
            line-height: 1.1;
            word-wrap: break-word;
            overflow: hidden;
        `;

        // Add label
        const label = document.createElement('span');
        label.textContent = item.label || '';
        label.style.cssText = `
            display: block;
            max-width: 100%;
            overflow: hidden;
            text-overflow: ellipsis;
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
        if (!currentMenu) return;

        if (currentMenu._cleanup) {
            currentMenu._cleanup();
        }

        currentMenu.remove();
        currentMenu = null;
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
