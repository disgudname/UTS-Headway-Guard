'use strict';

(function() {
  function initializeTrainsFeature(options) {
    const {
      getMap,
      state,
      adminFeaturesAllowed,
      updateToggleButton,
      onVisibilityChange,
      onFetchPromiseChange,
      TRAINS_ENDPOINT,
      TRAIN_TARGET_STATION_CODE
    } = options || {};

    if (typeof adminFeaturesAllowed !== 'function') {
      throw new Error('adminFeaturesAllowed callback is required');
    }
    if (!state) {
      throw new Error('Trains feature state is required');
    }

    const moduleState = state;

    function getMapInstance() {
      return typeof getMap === 'function' ? getMap() : null;
    }

    function setFetchPromise(promise) {
      moduleState.fetchPromise = promise || null;
      if (typeof onFetchPromiseChange === 'function') {
        try {
          onFetchPromiseChange(moduleState.fetchPromise);
        } catch (error) {
          console.error('Error notifying fetch promise change:', error);
        }
      }
    }

    function getTrainNameBubbleKey(trainID) {
      if (trainID === null || trainID === undefined) {
        return 'train:';
      }
      const text = `${trainID}`;
      return text.startsWith('train:') ? text : `train:${text}`;
    }

    function removeTrainNameBubble(trainID) {
      if (trainID === null || trainID === undefined) {
        return;
      }
      const key = getTrainNameBubbleKey(trainID);
      const bubble = moduleState.nameBubbles[key];
      const map = getMapInstance();
      if (bubble?.nameMarker && map) {
        if (typeof map.hasLayer === 'function' && map.hasLayer(bubble.nameMarker)) {
          map.removeLayer(bubble.nameMarker);
        } else if (typeof bubble.nameMarker.remove === 'function') {
          bubble.nameMarker.remove();
        }
      }
      delete moduleState.nameBubbles[key];
    }

    function clearAllTrainNameBubbles() {
      const map = getMapInstance();
      Object.keys(moduleState.nameBubbles).forEach(key => {
        const bubble = moduleState.nameBubbles[key];
        if (bubble?.nameMarker && map) {
          if (typeof map.hasLayer === 'function' && map.hasLayer(bubble.nameMarker)) {
            map.removeLayer(bubble.nameMarker);
          } else if (typeof bubble.nameMarker.remove === 'function') {
            bubble.nameMarker.remove();
          }
        }
      });
      moduleState.nameBubbles = {};
    }

    function ensureTrainMarkerState(trainID) {
      if (trainID === null || trainID === undefined) {
        return null;
      }
      const key = `${trainID}`;
      const existing = moduleState.markerStates[key];
      if (existing) {
        if (!existing.markerId) {
          existing.markerId = `train-${key.replace(/\s+/g, '-')}`;
        }
        return existing;
      }
      const defaultFill = typeof BUS_MARKER_DEFAULT_ROUTE_COLOR === 'string'
        ? BUS_MARKER_DEFAULT_ROUTE_COLOR
        : '#0f172a';
      const newState = {
        trainID: key,
        markerId: `train-${key.replace(/\s+/g, '-')}`,
        positionHistory: [],
        headingDeg: typeof BUS_MARKER_DEFAULT_HEADING === 'number' ? BUS_MARKER_DEFAULT_HEADING : 0,
        fillColor: defaultFill,
        glyphColor: typeof computeBusMarkerGlyphColor === 'function'
          ? computeBusMarkerGlyphColor(defaultFill)
          : '#ffffff',
        accessibleLabel: 'Amtrak train',
        isStale: false,
        isStopped: false,
        lastLatLng: null,
        marker: null,
        size: null,
        lastUpdateTimestamp: 0,
        routeName: '',
        trainNum: '',
        trainNumRaw: '',
        trainTimely: '',
        stations: [],
        markerEventsBound: false
      };
      moduleState.markerStates[key] = newState;
      return newState;
    }

    function clearTrainMarker(trainID) {
      if (trainID === null || trainID === undefined) {
        return;
      }
      const key = `${trainID}`;
      const map = getMapInstance();
      const marker = moduleState.markers[key];
      if (marker) {
        if (map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
          map.removeLayer(marker);
        } else if (typeof marker.remove === 'function') {
          marker.remove();
        }
      }
      delete moduleState.markers[key];
      delete moduleState.markerStates[key];
      removeTrainNameBubble(key);
    }

    function clearAllMarkers() {
      const map = getMapInstance();
      Object.keys(moduleState.markers).forEach(trainID => {
        const marker = moduleState.markers[trainID];
        if (!marker) {
          return;
        }
        if (map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
          map.removeLayer(marker);
        } else if (typeof marker.remove === 'function') {
          marker.remove();
        }
      });
      moduleState.markers = {};
      moduleState.markerStates = {};
      clearAllTrainNameBubbles();
    }

    function updateVisibilityState(visible) {
      moduleState.visible = !!visible;
      if (typeof onVisibilityChange === 'function') {
        try {
          onVisibilityChange(moduleState.visible);
        } catch (error) {
          console.error('Error notifying train visibility change:', error);
        }
      }
      if (typeof updateToggleButton === 'function') {
        try {
          updateToggleButton();
        } catch (error) {
          console.error('Error updating train toggle button:', error);
        }
      }
    }

    async function updateTrainMarkersVisibility() {
      if (!adminFeaturesAllowed()) {
        clearAllMarkers();
        return;
      }
      if (!moduleState.visible) {
        const map = getMapInstance();
        Object.keys(moduleState.markers).forEach(trainID => {
          const marker = moduleState.markers[trainID];
          if (!marker) {
            return;
          }
          if (map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
            map.removeLayer(marker);
          } else if (typeof marker.remove === 'function') {
            marker.remove();
          }
          const state = moduleState.markerStates[trainID];
          if (state) {
            state.marker = marker || null;
          }
        });
        clearAllTrainNameBubbles();
        return;
      }
      const map = getMapInstance();
      if (!map || typeof map.getBounds !== 'function') {
        return;
      }
      const bounds = map.getBounds();
      if (!bounds || typeof bounds.contains !== 'function') {
        return;
      }
      const zoom = typeof map.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM;
      const metrics = typeof computeBusMarkerMetrics === 'function' ? computeBusMarkerMetrics(zoom) : null;
      for (const trainID of Object.keys(moduleState.markerStates)) {
        const stateEntry = moduleState.markerStates[trainID];
        if (!stateEntry) {
          continue;
        }
        const latLng = stateEntry.lastLatLng;
        const marker = moduleState.markers[trainID];
        if (!latLng || !Number.isFinite(latLng.lat) || !Number.isFinite(latLng.lng)) {
          if (marker && map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
            map.removeLayer(marker);
          }
          stateEntry.marker = marker || null;
          removeTrainNameBubble(trainID);
          continue;
        }
        if (!bounds.contains(latLng)) {
          if (marker && map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
            map.removeLayer(marker);
          }
          stateEntry.marker = marker || null;
          removeTrainNameBubble(trainID);
          continue;
        }
        if (metrics && typeof setBusMarkerSize === 'function') {
          setBusMarkerSize(stateEntry, metrics);
        }
        let icon = null;
        try {
          if (typeof createBusMarkerDivIcon === 'function') {
            icon = await createBusMarkerDivIcon(stateEntry.markerId || `train-${trainID}`, stateEntry);
          }
        } catch (error) {
          console.error('Failed to create train marker icon:', error);
          icon = null;
        }
        if (!icon) {
          continue;
        }
        let trainMarker = marker;
        if (!trainMarker) {
          if (typeof L === 'undefined' || typeof L.marker !== 'function') {
            continue;
          }
          trainMarker = L.marker(latLng, {
            icon,
            pane: 'busesPane',
            interactive: true,
            keyboard: false
          });
          moduleState.markers[trainID] = trainMarker;
        } else if (typeof trainMarker.setIcon === 'function') {
          trainMarker.setIcon(icon);
        }
        stateEntry.marker = trainMarker;
        if (map && typeof map.hasLayer === 'function' && typeof trainMarker.addTo === 'function' && !map.hasLayer(trainMarker)) {
          trainMarker.addTo(map);
        }
        if (typeof animateMarkerTo === 'function') {
          animateMarkerTo(trainMarker, latLng);
        }
        // Re-enable pointer events after icon rebuild
        if (stateEntry.markerEventsBound) {
          enableTrainMarkerPointerEvents(trainMarker);
        }

        const routeColor = stateEntry.fillColor || BUS_MARKER_DEFAULT_ROUTE_COLOR;
        const trainNumDisplay = typeof stateEntry.trainNumRaw === 'string' && stateEntry.trainNumRaw.length > 0
          ? stateEntry.trainNumRaw
          : (typeof stateEntry.trainNum === 'string' ? stateEntry.trainNum.replace(/^[a-zA-Z]/, '') : '');
        const routeNamePart = typeof stateEntry.routeName === 'string' ? stateEntry.routeName.trim() : '';
        const labelText = trainNumDisplay && routeNamePart ? `${trainNumDisplay} ${routeNamePart}` : (routeNamePart || trainNumDisplay);
        const trainNumStr = typeof stateEntry.trainNum === 'string' ? stateEntry.trainNum : '';
        const companyPrefix = trainNumStr.charAt(0) === 'v' ? 'V'
          : trainNumStr.charAt(0) === 'b' ? 'B'
          : 'A';
        const bubbleKey = getTrainNameBubbleKey(trainID);
        if (typeof adminMode !== 'undefined' && typeof kioskMode !== 'undefined' && adminMode && !kioskMode && labelText) {
          let nameIcon = null;
          if (typeof createTrainNameBubbleDivIcon === 'function') {
            nameIcon = createTrainNameBubbleDivIcon(companyPrefix, labelText, routeColor, metrics ? metrics.scale : 1, stateEntry.headingDeg);
          } else if (typeof createNameBubbleDivIcon === 'function') {
            nameIcon = createNameBubbleDivIcon(labelText, routeColor, metrics ? metrics.scale : 1, stateEntry.headingDeg);
          }
          if (nameIcon) {
            const bubble = moduleState.nameBubbles[bubbleKey] || { trainID };
            if (bubble.nameMarker && typeof animateMarkerTo === 'function') {
              animateMarkerTo(bubble.nameMarker, latLng);
              if (typeof bubble.nameMarker.setIcon === 'function') {
                bubble.nameMarker.setIcon(nameIcon);
              }
            } else if (typeof L !== 'undefined' && typeof L.marker === 'function') {
              bubble.nameMarker = L.marker(latLng, { icon: nameIcon, interactive: false, pane: 'busesPane' });
              if (typeof bubble.nameMarker.addTo === 'function' && map) {
                bubble.nameMarker.addTo(map);
              }
            }
            bubble.trainID = trainID;
            bubble.lastScale = metrics ? metrics.scale : 1;
            moduleState.nameBubbles[bubbleKey] = bubble;
          } else {
            removeTrainNameBubble(trainID);
          }
        } else {
          removeTrainNameBubble(trainID);
        }

        // Bind click handler for popup
        if (!stateEntry.markerEventsBound && trainMarker) {
          attachTrainMarkerInteractions(trainID, trainMarker, stateEntry);
        }
      }
    }

    function enableTrainMarkerPointerEvents(marker) {
      if (!marker) {
        return;
      }
      if (marker.options) {
        marker.options.interactive = true;
      }
      const iconEl = typeof marker.getElement === 'function' ? marker.getElement() : marker._icon;
      if (!iconEl) {
        return;
      }
      iconEl.style.pointerEvents = 'auto';
      if (!iconEl.classList.contains('leaflet-interactive')) {
        iconEl.classList.add('leaflet-interactive');
      }
      const root = iconEl.querySelector('.bus-marker__root');
      if (root) {
        root.style.pointerEvents = 'auto';
        root.style.cursor = 'pointer';
      }
      const svg = iconEl.querySelector('.bus-marker__svg');
      if (svg) {
        svg.style.pointerEvents = 'auto';
      }
    }

    function attachTrainMarkerInteractions(trainID, marker, stateEntry) {
      if (!marker || stateEntry.markerEventsBound) {
        return;
      }
      enableTrainMarkerPointerEvents(marker);
      const popupOptions = {
        className: 'ondemand-driver-popup',
        closeButton: false,
        autoClose: true,
        autoPan: false,
        offset: [0, -20]
      };

      // Use DOM-level click on the icon element for reliability
      const bindDomClick = () => {
        const iconEl = typeof marker.getElement === 'function' ? marker.getElement() : marker._icon;
        if (!iconEl) {
          return;
        }
        if (iconEl._trainClickBound) {
          return;
        }
        iconEl._trainClickBound = true;
        iconEl.addEventListener('click', (e) => {
          e.stopPropagation();
          const popupHtml = buildTrainPopupContent(trainID, stateEntry);
          if (!popupHtml) {
            return;
          }
          if (typeof marker.unbindPopup === 'function') {
            marker.unbindPopup();
          }
          if (typeof marker.bindPopup === 'function') {
            marker.bindPopup(popupHtml, popupOptions);
            if (typeof marker.openPopup === 'function') {
              marker.openPopup();
            }
          }
        });
      };

      // Try immediately, and also on next add (in case element isn't ready)
      bindDomClick();
      marker.on('add', () => {
        enableTrainMarkerPointerEvents(marker);
        bindDomClick();
      });
      stateEntry.markerEventsBound = true;
    }

    function formatStationTime(isoString, tz) {
      if (!isoString) {
        return '';
      }
      try {
        const date = new Date(isoString);
        if (isNaN(date.getTime())) {
          return '';
        }
        const options = { hour: 'numeric', minute: '2-digit' };
        if (typeof tz === 'string' && tz.length > 0) {
          options.timeZone = tz;
        }
        return date.toLocaleTimeString('en-US', options);
      } catch (e) {
        return '';
      }
    }

    function getMinutesFromNow(isoString) {
      if (!isoString) {
        return null;
      }
      try {
        const date = new Date(isoString);
        if (isNaN(date.getTime())) {
          return null;
        }
        const diffMs = date.getTime() - Date.now();
        return Math.round(diffMs / 60000);
      } catch (e) {
        return null;
      }
    }

    function buildTrainPopupContent(trainID, stateEntry) {
      if (!stateEntry) {
        return null;
      }
      const popupSections = [];
      const esc = typeof escapeHtml === 'function'
        ? escapeHtml
        : (typeof window !== 'undefined' && window.TestMap && typeof window.TestMap.utils?.escapeHtml === 'function')
          ? window.TestMap.utils.escapeHtml
          : (s => `${s}`);

      // Info card: route name, train number, timeliness
      const routeName = typeof stateEntry.routeName === 'string' ? stateEntry.routeName.trim() : '';
      const trainNumDisplay = typeof stateEntry.trainNumRaw === 'string' && stateEntry.trainNumRaw.length > 0
        ? stateEntry.trainNumRaw
        : (typeof stateEntry.trainNum === 'string' ? stateEntry.trainNum.replace(/^[a-zA-Z]/, '') : '');
      const trainNumStr = typeof stateEntry.trainNum === 'string' ? stateEntry.trainNum : '';
      const providerLabel = trainNumStr.charAt(0) === 'v' ? 'VIA Rail'
        : trainNumStr.charAt(0) === 'b' ? 'Brightline'
        : 'Amtrak';
      const routeColor = stateEntry.fillColor || (typeof BUS_MARKER_DEFAULT_ROUTE_COLOR === 'string' ? BUS_MARKER_DEFAULT_ROUTE_COLOR : '#0f172a');
      const trainTimely = typeof stateEntry.trainTimely === 'string' ? stateEntry.trainTimely.trim() : '';

      const cardLines = [];
      if (routeName) {
        cardLines.push(`<div class="bus-popup__info-line bus-popup__info-line--route">${esc(routeName)}</div>`);
      }
      const metaParts = [];
      if (providerLabel && trainNumDisplay) {
        metaParts.push(`${esc(providerLabel)} ${esc(trainNumDisplay)}`);
      } else if (trainNumDisplay) {
        metaParts.push(`Train ${esc(trainNumDisplay)}`);
      } else if (providerLabel) {
        metaParts.push(esc(providerLabel));
      }
      if (metaParts.length > 0) {
        cardLines.push(`<div class="bus-popup__info-line bus-popup__info-line--block">${metaParts.join(' • ')}</div>`);
      }

      if (cardLines.length > 0) {
        popupSections.push([
          '<div class="ondemand-driver-popup__section">',
          '<div class="bus-popup__drivers-list">',
          `<div class="bus-popup__driver-row bus-popup__info-card" style="border-left-color: ${routeColor};">`,
          cardLines.join(''),
          '</div>',
          '</div>',
          '</div>'
        ].join(''));
      }

      // Timeliness section
      if (trainTimely) {
        popupSections.push([
          '<div class="ondemand-driver-popup__section">',
          '<div class="ondemand-driver-popup__label">Status</div>',
          `<div class="ondemand-driver-popup__value">${esc(trainTimely)}</div>`,
          '</div>'
        ].join(''));
      }

      // Next stops section (cap at 3)
      const stations = Array.isArray(stateEntry.stations) ? stateEntry.stations : [];
      const upcomingStations = stations.filter(s => {
        if (!s) return false;
        const status = typeof s.status === 'string' ? s.status.trim().toLowerCase() : '';
        return status !== 'departed';
      });
      const nextStops = upcomingStations.slice(0, 3);

      if (nextStops.length > 0) {
        const stopsHtml = nextStops.map(stop => {
          const name = stop.name || stop.code || 'Unknown';
          const arrTime = stop.arr || stop.schArr;
          const clockTime = formatStationTime(arrTime, stop.tz);
          const minutes = getMinutesFromNow(arrTime);
          let etaText = '';
          if (minutes !== null) {
            if (minutes <= 0) {
              etaText = clockTime ? `Arriving • ${clockTime}` : 'Arriving';
            } else {
              etaText = clockTime ? `${minutes} min • ${clockTime}` : `${minutes} min`;
            }
          } else if (clockTime) {
            etaText = clockTime;
          }

          return [
            '<div class="bus-popup__stop">',
            `<div class="bus-popup__stop-name">${esc(name)}</div>`,
            `<div class="bus-popup__stop-eta">${esc(etaText)}</div>`,
            '</div>'
          ].join('');
        }).join('');

        popupSections.push([
          '<div class="ondemand-driver-popup__section">',
          '<div class="ondemand-driver-popup__label">Next Stops</div>',
          '<div class="bus-popup__stops">',
          stopsHtml,
          '</div>',
          '</div>'
        ].join(''));
      }

      if (popupSections.length === 0) {
        return null;
      }

      return [
        '<div class="ondemand-driver-popup__content">',
        popupSections.join('<div class="ondemand-driver-popup__divider" aria-hidden="true"></div>'),
        '</div>'
      ].join('');
    }

    function getStationCodeFilter() {
      if (typeof TRAIN_TARGET_STATION_CODE === 'string') {
        return TRAIN_TARGET_STATION_CODE.trim().toUpperCase();
      }
      return '';
    }

    function setVisibility(visible) {
      const allowTrains = adminFeaturesAllowed();
      const desiredVisibility = allowTrains && !!visible;
      const previousVisibility = !!moduleState.visible;
      updateVisibilityState(desiredVisibility);
      const updatePromise = updateTrainMarkersVisibility();
      if (updatePromise && typeof updatePromise.catch === 'function') {
        updatePromise.catch(error => console.error('Error updating train markers visibility:', error));
      }
      if (desiredVisibility && !previousVisibility) {
        fetchTrains().catch(error => console.error('Failed to fetch trains:', error));
      }
      return updatePromise;
    }

    function toggleVisibility() {
      return setVisibility(!moduleState.visible);
    }

    async function fetchTrains() {
      if (moduleState.fetchPromise) {
        return moduleState.fetchPromise;
      }
      if (!adminFeaturesAllowed()) {
        return Promise.resolve();
      }
      if (!moduleState.visible) {
        return Promise.resolve();
      }
      const fetchTask = (async () => {
        if (!moduleState.visible) {
          return;
        }
        const stationCode = getStationCodeFilter();
        let payload;
        try {
          const response = await fetch(TRAINS_ENDPOINT, { cache: 'no-store' });
          if (!response || !response.ok) {
            const statusText = response ? `${response.status} ${response.statusText}` : 'No response';
            throw new Error(statusText);
          }
          payload = await response.json();
        } catch (error) {
          console.error('Failed to fetch trains:', error);
          return;
        }
        if (!moduleState.visible) {
          return;
        }
        const seenTrainIds = new Set();
        const timestamp = Date.now();
        if (payload && typeof payload === 'object') {
          Object.values(payload).forEach(group => {
            if (!Array.isArray(group)) {
              return;
            }
            group.forEach(train => {
              if (stationCode && typeof trainIncludesStation === 'function' && !trainIncludesStation(train, stationCode)) {
                return;
              }
              const identifier = typeof getTrainIdentifier === 'function'
                ? getTrainIdentifier(train)
                : (train?.trainID ?? train?.trainId ?? train?.trainNumRaw ?? train?.trainNum);
              if (!identifier) {
                return;
              }
              seenTrainIds.add(identifier);
              const stateEntry = ensureTrainMarkerState(identifier);
              if (!stateEntry) {
                return;
              }
              const lat = Number(train?.lat);
              const lon = Number(train?.lon);
              const fillColor = typeof normalizeRouteColor === 'function'
                ? normalizeRouteColor(train?.iconColor)
                : train?.iconColor;
              const rawTextColor = typeof train?.textColor === 'string' ? train.textColor.trim() : '';
              const glyphColor = rawTextColor.length > 0 && typeof normalizeGlyphColor === 'function'
                ? normalizeGlyphColor(rawTextColor, fillColor)
                : (typeof computeBusMarkerGlyphColor === 'function'
                  ? computeBusMarkerGlyphColor(fillColor)
                  : '#ffffff');
              stateEntry.fillColor = fillColor;
              stateEntry.glyphColor = glyphColor;
              stateEntry.accessibleLabel = typeof buildTrainAccessibleLabel === 'function'
                ? buildTrainAccessibleLabel(train)
                : 'Amtrak train';
              stateEntry.isStale = false;
              stateEntry.isStopped = false;
              stateEntry.lastUpdateTimestamp = timestamp;
              stateEntry.routeName = typeof train?.routeName === 'string' ? train.routeName.trim() : '';
              stateEntry.trainNum = (train?.trainNum !== undefined && train?.trainNum !== null) ? `${train.trainNum}`.trim() : '';
              stateEntry.trainNumRaw = (train?.trainNumRaw !== undefined && train?.trainNumRaw !== null) ? `${train.trainNumRaw}`.trim() : '';
              stateEntry.trainTimely = typeof train?.trainTimely === 'string' ? train.trainTimely.trim() : '';
              stateEntry.stations = Array.isArray(train?.stations) ? train.stations : [];
              const headingValue = typeof getTrainHeadingValue === 'function'
                ? getTrainHeadingValue(train)
                : train?.heading;
              if (Number.isFinite(lat) && Number.isFinite(lon) && typeof L !== 'undefined' && typeof L.latLng === 'function') {
                const latLng = L.latLng(lat, lon);
                stateEntry.lastLatLng = latLng;
                if (typeof updateTrainMarkerHeading === 'function') {
                  stateEntry.headingDeg = updateTrainMarkerHeading(stateEntry, latLng, headingValue);
                }
              } else {
                stateEntry.lastLatLng = null;
                if (typeof updateTrainMarkerHeading === 'function') {
                  stateEntry.headingDeg = updateTrainMarkerHeading(stateEntry, null, headingValue);
                }
              }
            });
          });
        }
        const TRAIN_STALE_TIMEOUT_MS = 60000; // Remove trains that haven't been seen for 60 seconds
        Object.keys(moduleState.markerStates).forEach(trainID => {
          if (!seenTrainIds.has(trainID)) {
            const stateEntry = moduleState.markerStates[trainID];
            if (stateEntry && stateEntry.lastUpdateTimestamp) {
              const age = timestamp - stateEntry.lastUpdateTimestamp;
              if (age > TRAIN_STALE_TIMEOUT_MS) {
                clearTrainMarker(trainID);
              }
            } else {
              // If there's no timestamp, remove it immediately (old/invalid state)
              clearTrainMarker(trainID);
            }
          }
        });
        try {
          await updateTrainMarkersVisibility();
        } catch (error) {
          console.error('Error updating train markers visibility:', error);
        }
      })();
      setFetchPromise(fetchTask.finally(() => {
        setFetchPromise(null);
      }));
      return moduleState.fetchPromise;
    }

    updateVisibilityState(!!moduleState.visible);

    return {
      setVisibility,
      toggleVisibility,
      updateTrainMarkersVisibility,
      fetchTrains,
      clearAllMarkers,
      removeTrainNameBubble
    };
  }

  window.initializeTrainsFeature = initializeTrainsFeature;
})();
