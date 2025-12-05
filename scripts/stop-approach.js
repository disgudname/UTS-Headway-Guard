(() => {
  const STOP_APPROACH_ENDPOINT = '/api/stop-approach';
  const DEFAULT_RADIUS_M = 60;
  const DEFAULT_TOLERANCE = 30;
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
  const stopMeta = document.getElementById('stopMeta');
  const saveButton = document.getElementById('saveButton');
  const saveStatus = document.getElementById('saveStatus');
  const refreshButton = document.getElementById('refreshButton');

  let stops = [];
  const stopMarkers = new Map();
  let selectedStopId = null;
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
    const points = [center];
    const start = bearingDeg - toleranceDeg;
    const end = bearingDeg + toleranceDeg;
    const step = Math.max(6, Math.round(toleranceDeg / 2));
    for (let angle = start; angle <= end; angle += step) {
      points.push(destinationPoint(center, angle, radiusMeters));
    }
    points.push(destinationPoint(center, end, radiusMeters));
    points.push(center);
    return points;
  }

  function updateDisplayValues(radius, tolerance, bearing) {
    radiusValue.textContent = `${radius.toFixed(0)} m`;
    toleranceValue.textContent = `${tolerance.toFixed(0)}°`;
    bearingInput.value = bearing.toFixed(0);
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
      handleMarker.on('drag', (e) => {
        const newBearing = bearingBetween(center, e.latlng);
        const toleranceVal = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
        const radiusVal = Number(radiusInput.value) || DEFAULT_RADIUS_M;
        bearingInput.value = newBearing.toFixed(0);
        updateConeGraphics(stop, newBearing, toleranceVal, radiusVal);
      });
    } else {
      handleMarker.setLatLng(handlePoint);
    }

    map.flyTo(center, Math.max(map.getZoom(), 16));
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

  function applyStopToControls(stop) {
    const radius = Number(stop.ApproachRadiusM) || DEFAULT_RADIUS_M;
    const tolerance = Number(stop.ApproachToleranceDeg) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(stop.ApproachBearingDeg ?? DEFAULT_BEARING);
    radiusInput.value = radius;
    toleranceInput.value = tolerance;
    bearingInput.value = bearing.toFixed(0);
    stopMeta.textContent = `${stop.Name || stop.StopName || 'Stop'} • Routes: ${formatRoutes(stop) || 'unknown'}`;
    updateDisplayValues(radius, tolerance, bearing);
    updateConeGraphics(stop, bearing, tolerance, radius);
  }

  function onStopSelected(id) {
    selectedStopId = id;
    const stop = stops.find((s) => `${s.StopID || s.StopId}` === `${id}`);
    if (!stop) return;
    applyStopToControls(stop);
  }

  function populateStops(list) {
    stopSelect.innerHTML = '';
    list
      .slice()
      .sort((a, b) => (a.StopName || a.Name || '').localeCompare(b.StopName || b.Name || ''))
      .forEach((stop) => {
        const option = document.createElement('option');
        option.value = stop.StopID || stop.StopId;
        option.textContent = `${stop.StopName || stop.Name || stop.Description || option.value}`;
        stopSelect.appendChild(option);
      });
  }

  function setStatus(text, isError = false) {
    saveStatus.textContent = text;
    saveStatus.style.color = isError ? '#f87171' : '#9ca3af';
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
      populateStops(stops);
      if (stops.length > 0) {
        const targetId = selectedStopId || stops[0].StopID || stops[0].StopId;
        stopSelect.value = targetId;
        onStopSelected(targetId);
        if (map && hasLeaflet) {
          stops.forEach((stop) => {
            const id = stop.StopID || stop.StopId;
            if (!id) return;
            const marker = L.circleMarker([stop.Latitude || stop.Lat || 0, stop.Longitude || stop.Lon || 0], {
              radius: 4,
              color: '#c084fc',
              fillOpacity: 0.7,
            });
            marker.on('click', () => {
              stopSelect.value = id;
              onStopSelected(id);
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
      return;
    }
    const stop = stops.find((s) => `${s.StopID || s.StopId}` === `${selectedStopId}`);
    if (!stop) {
      setStatus('Stop not found', true);
      return;
    }

    const payload = {
      stop_id: selectedStopId,
      radius_m: Number(radiusInput.value) || DEFAULT_RADIUS_M,
      tolerance_deg: Number(toleranceInput.value) || DEFAULT_TOLERANCE,
      bearing_deg: normalizeBearing(bearingInput.value),
    };
    setStatus('Saving…');
    saveButton.disabled = true;
    try {
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
      stop.ApproachBearingDeg = result.bearing_deg;
      stop.ApproachToleranceDeg = result.tolerance_deg;
      stop.ApproachRadiusM = result.radius_m;
      applyStopToControls(stop);
      setStatus('Saved');
    } catch (error) {
      console.error('Save failed', error);
      setStatus('Save failed', true);
    } finally {
      saveButton.disabled = false;
    }
  }

  stopSelect.addEventListener('change', (e) => onStopSelected(e.target.value));
  radiusInput.addEventListener('input', () => {
    const radius = Number(radiusInput.value) || DEFAULT_RADIUS_M;
    const tolerance = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(bearingInput.value);
    updateDisplayValues(radius, tolerance, bearing);
    const stop = stops.find((s) => `${s.StopID || s.StopId}` === `${selectedStopId}`);
    if (stop) {
      updateConeGraphics(stop, bearing, tolerance, radius);
    }
  });
  toleranceInput.addEventListener('input', () => {
    const radius = Number(radiusInput.value) || DEFAULT_RADIUS_M;
    const tolerance = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(bearingInput.value);
    updateDisplayValues(radius, tolerance, bearing);
    const stop = stops.find((s) => `${s.StopID || s.StopId}` === `${selectedStopId}`);
    if (stop) {
      updateConeGraphics(stop, bearing, tolerance, radius);
    }
  });
  bearingInput.addEventListener('input', () => {
    const radius = Number(radiusInput.value) || DEFAULT_RADIUS_M;
    const tolerance = Number(toleranceInput.value) || DEFAULT_TOLERANCE;
    const bearing = normalizeBearing(bearingInput.value);
    updateDisplayValues(radius, tolerance, bearing);
    const stop = stops.find((s) => `${s.StopID || s.StopId}` === `${selectedStopId}`);
    if (stop) {
      updateConeGraphics(stop, bearing, tolerance, radius);
    }
  });

  saveButton.addEventListener('click', saveStop);
  refreshButton.addEventListener('click', () => {
    stopMarkers.forEach((marker) => marker.remove());
    stopMarkers.clear();
    fetchStops();
  });

  fetchStops();
})();
