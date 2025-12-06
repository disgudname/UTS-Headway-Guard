(() => {
  const STOP_APPROACH_ENDPOINT = '/api/stop-approach';
  const DEFAULT_RADIUS_M = 100;
  const DEFAULT_TOLERANCE = 70;
  const DEFAULT_BEARING = 0;

  const hasLeaflet = typeof L !== 'undefined';
  const map = hasLeaflet
    ? L.map('map', {
        zoomControl: true,
      })
    : null;
  if (map) {
    const tiles = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19,
    });
    tiles.addTo(map);
    map.setView([38.0336, -78.508], 14);
  } else {
    console.error('Leaflet failed to load; map features disabled');
  }

  const stopSelect = document.getElementById('stopSelect');
  const radiusInput = document.getElementById('radiusInput');
  const toleranceInput = document.getElementById('toleranceInput');
  const bearingInput = document.getElementById('bearingInput');
  const radiusValue = document.getElementById('radiusValue');
  const toleranceValue = document.getElementById('toleranceValue');
  const radius100Button = document.getElementById('radius100Button');
  const stopMeta = document.getElementById('stopMeta');
  const saveButton = document.getElementById('saveButton');
  const saveStatus = document.getElementById('saveStatus');
  const refreshButton = document.getElementById('refreshButton');
  const resetAllButton = document.getElementById('resetAllButton');

  let stops = [];
  let dedupedStops = [];
  const stopGroupsByKey = new Map();
  const stopMarkers = new Map();
  let selectedStopId = null;
  let hasUnsavedChanges = false;
  let circleLayer = null;
  let coneLayer = null;
  let handleMarker = null;

  function normalizeBearing(value) {
    const num = Number(value);
    if (Number.isNaN(num)) return DEFAULT_BEARING;
    return ((num % 360) + 360) % 360;
  }

  function toRad(deg) {
    return (deg * Math.PI) / 180;
  }

  function toDeg(rad) {
    return (rad * 180) / Math.PI;
  }

  function destinationPoint(center, bearingDeg, distanceMeters) {
    const R = 6371000;
    const bearingRad = toRad(bearingDeg);
    const lat1 = toRad(center.lat);
    const lon1 = toRad(center.lng);
    const ratio = distanceMeters / R;

    const lat2 = Math.asin(
      Math.sin(lat1) * Math.cos(ratio) + Math.cos(lat1) * Math.sin(ratio) * Math.cos(bearingRad)
    );
    const lon2 =
      lon1 +
      Math.atan2(
        Math.sin(bearingRad) * Math.sin(ratio) * Math.cos(lat1),
        Math.cos(ratio) - Math.sin(lat1) * Math.sin(lat2)
      );

    return L.latLng(toDeg(lat2), toDeg(lon2));
  }

  function bearingBetween(a, b) {
    const lat1 = toRad(a.lat);
    const lat2 = toRad(b.lat);
    const dLon = toRad(b.lng - a.lng);
    const y = Math.sin(dLon) * Math.cos(lat2);
    const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
    return normalizeBearing(toDeg(Math.atan2(y, x)));
  }

  function buildConePoints(center, bearingDeg, toleranceDeg, radiusMeters) {
    const coneStart = destinationPoint(center, bearingDeg, radiusMeters);
    const targetHeading = normalizeBearing(bearingDeg + 180);
    const start = targetHeading - toleranceDeg;
    const end = targetHeading + toleranceDeg;
    const step = Math.max(6, Math.round(toleranceDeg / 2));
    const points = [coneStart];

    for (let angle = start; angle <= end; angle += step) {
      points.push(destinationPoint(coneStart, angle, radiusMeters));
    }
    points.push(destinationPoint(coneStart, end, radiusMeters));
    points.push(coneStart);
    return points;
  }

  function updateDisplayValues(radius, tolerance, bearing) {
    radiusValue.textContent = `${radius.toFixed(0)} m`;
    toleranceValue.textContent = `${tolerance.toFixed(0)}°`;
    bearingInput.value = bearing.toFixed(0);
  }

  function syncGraphicsFromInputs() {
    const radius = Number(radiusInput.value) || DEFAULT_RADIUS_M;
    const tolerance = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(bearingInput.value);
    updateDisplayValues(radius, tolerance, bearing);
    const group = stopGroupsByKey.get(selectedStopId);
    const stop = group ? pickStopWithConfig(group.stops) : null;
    if (stop) {
      updateConeGraphics(stop, bearing, tolerance, radius);
    }
  }

  function updateConeGraphics(stop, bearingDeg, toleranceDeg, radiusMeters) {
    if (!map || !hasLeaflet) return;
    const center = L.latLng(stop.Latitude || stop.Lat || stop.lat, stop.Longitude || stop.Lon || stop.lng);
    const conePoints = buildConePoints(center, bearingDeg, toleranceDeg, radiusMeters);

    if (!circleLayer) {
      circleLayer = L.circle(center, {
        radius: radiusMeters,
        color: '#10b981',
        weight: 2,
        fillOpacity: 0.08,
      }).addTo(map);
    } else {
      circleLayer.setLatLng(center);
      circleLayer.setRadius(radiusMeters);
    }

    if (!coneLayer) {
      coneLayer = L.polygon(conePoints, {
        color: '#22d3ee',
        weight: 2,
        fillColor: '#22d3ee',
        fillOpacity: 0.18,
      }).addTo(map);
    } else {
      coneLayer.setLatLngs(conePoints);
    }

    const handlePoint = destinationPoint(center, bearingDeg, radiusMeters);
    if (!handleMarker) {
      handleMarker = L.marker(handlePoint, {
        draggable: true,
        title: 'Drag to rotate cone',
      }).addTo(map);
    } else {
      handleMarker.setLatLng(handlePoint);
    }

    handleMarker.off('drag');
    handleMarker.on('drag', (e) => {
      const newBearing = bearingBetween(center, e.latlng);
      const toleranceVal = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
      const radiusVal = Number(radiusInput.value) || DEFAULT_RADIUS_M;
      bearingInput.value = newBearing.toFixed(0);
      hasUnsavedChanges = true;
      updateConeGraphics(stop, newBearing, toleranceVal, radiusVal);
    });

    map.flyTo(center, Math.max(map.getZoom(), 16));
  }

  function getStopName(stop) {
    return stop.StopName || stop.Name || stop.Description || '';
  }

  function getStopCoords(stop) {
    const lat = Number(stop.Latitude ?? stop.Lat ?? stop.lat);
    const lng = Number(stop.Longitude ?? stop.Lon ?? stop.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    return { lat, lng };
  }

  function formatRoutes(stop) {
    const ids = stop.RouteIDs || stop.RouteIds || stop.RouteId || stop.RouteID || stop.Routes;
    if (!ids) return '';
    if (Array.isArray(ids)) {
      return ids.join(', ');
    }
    if (Array.isArray(stop.Routes)) {
      return stop.Routes.map((r) => r.RouteID || r.RouteId || r.id).filter(Boolean).join(', ');
    }
    return ids.toString();
  }

  function applyStopToControls(stop, group) {
    const radius = Number(stop.ApproachRadiusM) || DEFAULT_RADIUS_M;
    const tolerance = Number(stop.ApproachToleranceDeg) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(stop.ApproachBearingDeg ?? DEFAULT_BEARING);
    radiusInput.value = radius;
    toleranceInput.value = tolerance;
    bearingInput.value = bearing.toFixed(0);
    const linkedCount = group && group.ids ? group.ids.length : 1;
    const linkedText = linkedCount > 1 ? ` • Linked stops: ${linkedCount}` : '';
    stopMeta.textContent = `${stop.Name || stop.StopName || 'Stop'} • Routes: ${
      formatRoutes(stop) || 'unknown'
    }${linkedText}`;
    updateDisplayValues(radius, tolerance, bearing);
    updateConeGraphics(stop, bearing, tolerance, radius);
    hasUnsavedChanges = false;
    setStatus('Ready');
  }

  function pickStopWithConfig(stopsList) {
    return (
      stopsList.find(
        (s) => s.ApproachRadiusM !== undefined || s.ApproachToleranceDeg !== undefined || s.ApproachBearingDeg !== undefined
      ) || stopsList[0]
    );
  }

  function onStopSelected(key) {
    selectedStopId = key;
    const group = stopGroupsByKey.get(key);
    if (!group) return;
    const stop = pickStopWithConfig(group.stops);
    if (!stop) return;
    applyStopToControls(stop, group);
  }

  function populateStops(list) {
    stopSelect.innerHTML = '';
    list
      .slice()
      .sort((a, b) => (a.label || '').localeCompare(b.label || ''))
      .forEach((entry) => {
        const option = document.createElement('option');
        option.value = entry.key;
        const suffix = entry.ids.length > 1 ? ` (${entry.ids.length} stops)` : '';
        option.textContent = `${entry.label}${suffix}`;
        stopSelect.appendChild(option);
      });
  }

  function setStatus(text, isError = false) {
    saveStatus.textContent = text;
    saveStatus.style.color = isError ? '#f87171' : '#9ca3af';
  }

  function dedupeStops(rawStops) {
    stopGroupsByKey.clear();
    const groups = [];

    rawStops.forEach((stop) => {
      const name = getStopName(stop).trim();
      const coords = getStopCoords(stop);
      const id = stop.StopID || stop.StopId;
      const key = name && coords ? `${name.toLowerCase()}|${coords.lat.toFixed(6)}|${coords.lng.toFixed(6)}` : `${id}`;
      const group = stopGroupsByKey.get(key);
      if (!group) {
        const newGroup = {
          key,
          label: name || `${id || 'Stop'}`,
          coords,
          ids: id ? [id] : [],
          stops: [stop],
        };
        stopGroupsByKey.set(key, newGroup);
        groups.push(newGroup);
      } else {
        if (id && !group.ids.includes(id)) {
          group.ids.push(id);
        }
        group.stops.push(stop);
      }
    });

    return groups;
  }

  async function fetchStops() {
    setStatus('Loading stops…');
    try {
      const response = await fetch(STOP_APPROACH_ENDPOINT, { cache: 'no-store' });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const payload = await response.json();
      const data = Array.isArray(payload.stops) ? payload.stops : [];
      stops = data;
      dedupedStops = dedupeStops(stops);
      populateStops(dedupedStops);
      if (dedupedStops.length > 0) {
        const targetKey = selectedStopId || dedupedStops[0].key;
        stopSelect.value = targetKey;
        onStopSelected(targetKey);
        if (map && hasLeaflet) {
          dedupedStops.forEach((group) => {
            const id = group.key;
            if (!group.coords) return;
            const marker = L.circleMarker([group.coords.lat, group.coords.lng], {
              radius: 4,
              color: '#c084fc',
              fillOpacity: 0.7,
            });
            marker.on('click', () => {
              stopSelect.value = id;
              handleStopChange(id);
            });
            marker.addTo(map);
            stopMarkers.set(id.toString(), marker);
          });
          map.fitBounds(L.featureGroup(Array.from(stopMarkers.values())).getBounds().pad(0.15));
        }
      }
      setStatus('Ready');
    } catch (error) {
      console.error('Failed to load stops', error);
      setStatus('Failed to load stops', true);
    }
  }

  async function saveStop() {
    if (!selectedStopId) {
      setStatus('Select a stop first', true);
      return false;
    }
    const group = stopGroupsByKey.get(selectedStopId);
    if (!group) {
      setStatus('Stop not found', true);
      return false;
    }

    const radiusVal = Number(radiusInput.value) || DEFAULT_RADIUS_M;
    const toleranceVal = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
    const bearingVal = normalizeBearing(bearingInput.value);

    const payloads = group.ids.map((id) => ({
      stop_id: id,
      radius_m: radiusVal,
      tolerance_deg: toleranceVal,
      bearing_deg: bearingVal,
    }));
    setStatus('Saving…');
    saveButton.disabled = true;
    try {
      await Promise.all(
        payloads.map(async (payload) => {
          const response = await fetch(STOP_APPROACH_ENDPOINT, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
          });
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const result = await response.json();
          group.stops
            .filter((s) => `${s.StopID || s.StopId}` === `${payload.stop_id}`)
            .forEach((s) => {
              s.ApproachBearingDeg = result.bearing_deg;
              s.ApproachToleranceDeg = result.tolerance_deg;
              s.ApproachRadiusM = result.radius_m;
            });
        })
      );
      const representativeStop = pickStopWithConfig(group.stops);
      applyStopToControls(representativeStop, group);
      setStatus('Saved');
      hasUnsavedChanges = false;
      return true;
    } catch (error) {
      console.error('Save failed', error);
      setStatus('Save failed', true);
      return false;
    } finally {
      saveButton.disabled = false;
    }
  }

  async function resetAllStops() {
    if (!dedupedStops.length) {
      setStatus('Load stops before resetting', true);
      return;
    }

    const uniqueIds = new Set();
    dedupedStops.forEach((group) => {
      group.ids.forEach((id) => uniqueIds.add(id));
    });

    const validIds = Array.from(uniqueIds).filter((id) => {
      if (id === null || id === undefined) return false;
      const idStr = `${id}`.trim();
      return idStr.length > 0 && idStr.toLowerCase() !== 'undefined';
    });

    if (validIds.length === 0) {
      setStatus('No stop IDs available to reset', true);
      return;
    }

    const payloadTemplate = {
      radius_m: DEFAULT_RADIUS_M,
      tolerance_deg: DEFAULT_TOLERANCE,
      bearing_deg: DEFAULT_BEARING,
    };

    setStatus('Resetting all stops…');
    resetAllButton.disabled = true;
    try {
      await Promise.all(
        validIds.map(async (id) => {
          const response = await fetch(STOP_APPROACH_ENDPOINT, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ stop_id: id, ...payloadTemplate }),
          });

          if (!response.ok) {
            const errorText = await response.text().catch(() => '');
            const detail = errorText ? `: ${errorText}` : '';
            throw new Error(`HTTP ${response.status}${detail}`);
          }

          stops
            .filter((stop) => `${stop.StopID || stop.StopId}` === `${id}`)
            .forEach((stop) => {
              stop.ApproachRadiusM = payloadTemplate.radius_m;
              stop.ApproachToleranceDeg = payloadTemplate.tolerance_deg;
              stop.ApproachBearingDeg = payloadTemplate.bearing_deg;
            });
        })
      );

      if (selectedStopId) {
        const group = stopGroupsByKey.get(selectedStopId);
        const stop = group ? pickStopWithConfig(group.stops) : null;
        if (stop) {
          applyStopToControls(stop, group);
        }
      }

      setStatus('All stops reset');
    } catch (error) {
      console.error('Reset failed', error);
      setStatus('Reset failed', true);
    } finally {
      resetAllButton.disabled = false;
    }
  }

  async function saveCurrentStopIfNeeded() {
    if (!hasUnsavedChanges) return true;
    return saveStop();
  }

  async function handleStopChange(nextKey) {
    if (!nextKey || nextKey === selectedStopId) return;
    const previousKey = selectedStopId;
    const saveSucceeded = await saveCurrentStopIfNeeded();
    if (!saveSucceeded) {
      if (previousKey !== null && stopSelect.value !== previousKey) {
        stopSelect.value = previousKey;
      }
      return;
    }
    onStopSelected(nextKey);
  }

  stopSelect.addEventListener('change', (e) => {
    handleStopChange(e.target.value);
  });
  radiusInput.addEventListener('input', () => {
    hasUnsavedChanges = true;
    syncGraphicsFromInputs();
  });
  toleranceInput.addEventListener('input', () => {
    hasUnsavedChanges = true;
    syncGraphicsFromInputs();
  });
  bearingInput.addEventListener('input', () => {
    hasUnsavedChanges = true;
    syncGraphicsFromInputs();
  });

  if (radius100Button) {
    radius100Button.addEventListener('click', () => {
      radiusInput.value = 100;
      hasUnsavedChanges = true;
      syncGraphicsFromInputs();
    });
  }

  saveButton.addEventListener('click', saveStop);
  refreshButton.addEventListener('click', () => {
    stopMarkers.forEach((marker) => marker.remove());
    stopMarkers.clear();
    fetchStops();
  });

  if (resetAllButton) {
    resetAllButton.addEventListener('click', resetAllStops);
  }

  fetchStops();
})();
