(() => {
  const STOP_APPROACH_ENDPOINT = '/api/stop-approach';
  const DEFAULT_RADIUS_M = 100;
  const DEFAULT_TOLERANCE = 70;
  const DEFAULT_BEARING = 0;
  const DEFAULT_BUBBLE_RADIUS_M = 25;

  const hasLeaflet = typeof L !== 'undefined';
  const map = hasLeaflet
    ? L.map('map', {
        zoomControl: true,
      })
    : null;
  if (map) {
    const streetLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19,
    });
    const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
      attribution: '&copy; Esri',
      maxZoom: 19,
    });
    streetLayer.addTo(map);
    L.control.layers(
      { 'Street': streetLayer, 'Satellite': satelliteLayer },
      null,
      { position: 'topright' }
    ).addTo(map);
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

  // Bubble UI elements
  const approachSetTabs = document.getElementById('approachSetTabs');
  const addApproachSetBtn = document.getElementById('addApproachSetBtn');
  const noBubblesMessage = document.getElementById('noBubblesMessage');
  const activeSetControls = document.getElementById('activeSetControls');
  const approachSetNameInput = document.getElementById('approachSetNameInput');
  const deleteApproachSetBtn = document.getElementById('deleteApproachSetBtn');
  const bubbleList = document.getElementById('bubbleList');
  const addBubbleBtn = document.getElementById('addBubbleBtn');
  const bubbleCountBadge = document.getElementById('bubbleCount');

  let stops = [];
  let dedupedStops = [];
  const stopGroupsByKey = new Map();
  const stopMarkers = new Map();
  let selectedStopId = null;
  let hasUnsavedChanges = false;
  let circleLayer = null;
  let coneLayer = null;
  let handleMarker = null;

  // Bubble state
  let approachSets = []; // Array of {name: string, bubbles: [{lat, lng, radius_m, order}]}
  let activeSetIndex = -1;
  let isPlacingBubble = false;
  let bubbleLayers = []; // Leaflet layers for bubbles
  let bubbleMarkers = []; // Leaflet markers for bubble centers

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
    const arcPoints = buildConeArcPoints(center, bearingDeg, toleranceDeg, radiusMeters);
    const apex = destinationPoint(center, normalizeBearing(bearingDeg + 180), radiusMeters);
    return [apex, ...arcPoints, apex];
  }

  function buildConeArcPoints(center, bearingDeg, toleranceDeg, radiusMeters) {
    const arcPoints = [];
    const startBearing = bearingDeg - toleranceDeg;
    const endBearing = bearingDeg + toleranceDeg;
    const span = Math.max(0, endBearing - startBearing);
    const step = Math.max(2, Math.min(10, toleranceDeg / 2 || 5));
    const segments = Math.max(1, Math.ceil(span / step));
    const actualStep = segments > 0 ? span / segments : 0;

    for (let i = 0; i <= segments; i += 1) {
      const currentBearing = startBearing + actualStep * i;
      arcPoints.push(destinationPoint(center, currentBearing, radiusMeters));
    }

    return arcPoints;
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

  // ==================== BUBBLE MANAGEMENT ====================

  function clearBubbleLayers() {
    bubbleLayers.forEach((layer) => {
      if (map) map.removeLayer(layer);
    });
    bubbleLayers = [];
    bubbleMarkers.forEach((marker) => {
      if (map) map.removeLayer(marker);
    });
    bubbleMarkers = [];
  }

  function renderBubbleLayers() {
    clearBubbleLayers();
    if (!map || !hasLeaflet || activeSetIndex < 0 || !approachSets[activeSetIndex]) return;

    const set = approachSets[activeSetIndex];
    const colors = ['#3b82f6', '#8b5cf6', '#ec4899', '#f97316', '#eab308'];

    set.bubbles.forEach((bubble, idx) => {
      const color = colors[idx % colors.length];
      const circle = L.circle([bubble.lat, bubble.lng], {
        radius: bubble.radius_m,
        color: color,
        weight: 2,
        fillColor: color,
        fillOpacity: 0.15,
        dashArray: '5, 5',
      }).addTo(map);
      bubbleLayers.push(circle);

      // Add numbered marker at center
      const icon = L.divIcon({
        className: 'bubble-marker-label',
        html: `<div style="background:${color};color:white;border-radius:50%;width:20px;height:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);">${bubble.order}</div>`,
        iconSize: [20, 20],
        iconAnchor: [10, 10],
      });
      const marker = L.marker([bubble.lat, bubble.lng], {
        icon: icon,
        draggable: true,
        title: `Bubble ${bubble.order} - Drag to move`,
      }).addTo(map);

      marker.on('drag', (e) => {
        const latlng = e.target.getLatLng();
        bubble.lat = latlng.lat;
        bubble.lng = latlng.lng;
        circle.setLatLng(latlng);
        hasUnsavedChanges = true;
        renderBubbleList();
      });

      bubbleMarkers.push(marker);
    });
  }

  function updateBubbleCount() {
    let total = 0;
    approachSets.forEach((set) => {
      total += set.bubbles.length;
    });
    bubbleCountBadge.textContent = total.toString();
  }

  function renderApproachSetTabs() {
    // Clear existing tabs except the add button
    const existingTabs = approachSetTabs.querySelectorAll('.approach-set-tab:not(.add-new)');
    existingTabs.forEach((tab) => tab.remove());

    // Add tabs for each set
    approachSets.forEach((set, idx) => {
      const tab = document.createElement('button');
      tab.className = 'approach-set-tab' + (idx === activeSetIndex ? ' active' : '');
      tab.type = 'button';
      tab.textContent = set.name || `Set ${idx + 1}`;
      tab.addEventListener('click', () => {
        activeSetIndex = idx;
        renderApproachSetTabs();
        renderActiveSetUI();
        renderBubbleLayers();
      });
      approachSetTabs.insertBefore(tab, addApproachSetBtn);
    });

    updateBubbleCount();
  }

  function renderBubbleList() {
    bubbleList.innerHTML = '';
    if (activeSetIndex < 0 || !approachSets[activeSetIndex]) return;

    const set = approachSets[activeSetIndex];
    set.bubbles.sort((a, b) => a.order - b.order);

    set.bubbles.forEach((bubble, idx) => {
      const item = document.createElement('div');
      item.className = 'bubble-item';
      item.draggable = true;
      item.dataset.index = idx.toString();

      item.innerHTML = `
        <span class="drag-handle" title="Drag to reorder">&#x2630;</span>
        <span class="order-badge">${bubble.order}</span>
        <div class="bubble-info">
          <div class="coords">${bubble.lat.toFixed(6)}, ${bubble.lng.toFixed(6)}</div>
        </div>
        <div class="bubble-radius">
          <input type="number" value="${bubble.radius_m}" min="5" max="200" step="5" title="Radius in meters" />
          <span>m</span>
          <div class="radius-presets">
            <button type="button" class="preset-15" title="15 mph zone">25</button>
            <button type="button" class="preset-25" title="25 mph zone">40</button>
            <button type="button" class="preset-35" title="35 mph zone">50</button>
          </div>
        </div>
        <div class="bubble-actions">
          <button type="button" class="delete" title="Remove bubble">&#x2715;</button>
        </div>
      `;

      // Radius change
      const radiusInputEl = item.querySelector('input[type="number"]');
      radiusInputEl.addEventListener('change', (e) => {
        bubble.radius_m = Math.max(5, Math.min(200, Number(e.target.value) || DEFAULT_BUBBLE_RADIUS_M));
        e.target.value = bubble.radius_m;
        hasUnsavedChanges = true;
        renderBubbleLayers();
      });

      // Radius preset buttons (based on 5s refresh rate + 10m buffer)
      // 15 mph = 33m/5s → ~17m + 10m = 27m → 25m, 25 mph = 56m/5s → ~28m + 10m = 38m → 40m, 35 mph = 78m/5s → ~39m + 10m = 49m → 50m
      const setRadius = (r) => {
        bubble.radius_m = r;
        radiusInputEl.value = r;
        hasUnsavedChanges = true;
        renderBubbleLayers();
      };
      item.querySelector('.preset-15').addEventListener('click', () => setRadius(25));
      item.querySelector('.preset-25').addEventListener('click', () => setRadius(40));
      item.querySelector('.preset-35').addEventListener('click', () => setRadius(50));

      // Delete button
      const deleteBtn = item.querySelector('.delete');
      deleteBtn.addEventListener('click', () => {
        set.bubbles.splice(idx, 1);
        // Renumber remaining bubbles
        set.bubbles.forEach((b, i) => {
          b.order = i + 1;
        });
        hasUnsavedChanges = true;
        renderBubbleList();
        renderBubbleLayers();
        updateBubbleCount();
      });

      // Drag and drop for reordering
      item.addEventListener('dragstart', (e) => {
        item.classList.add('dragging');
        e.dataTransfer.setData('text/plain', idx.toString());
      });

      item.addEventListener('dragend', () => {
        item.classList.remove('dragging');
      });

      item.addEventListener('dragover', (e) => {
        e.preventDefault();
      });

      item.addEventListener('drop', (e) => {
        e.preventDefault();
        const fromIdx = parseInt(e.dataTransfer.getData('text/plain'), 10);
        const toIdx = idx;
        if (fromIdx !== toIdx) {
          const [moved] = set.bubbles.splice(fromIdx, 1);
          set.bubbles.splice(toIdx, 0, moved);
          // Renumber
          set.bubbles.forEach((b, i) => {
            b.order = i + 1;
          });
          hasUnsavedChanges = true;
          renderBubbleList();
          renderBubbleLayers();
        }
      });

      bubbleList.appendChild(item);
    });
  }

  function renderActiveSetUI() {
    if (activeSetIndex < 0 || !approachSets[activeSetIndex]) {
      noBubblesMessage.style.display = 'block';
      activeSetControls.style.display = 'none';
      return;
    }

    noBubblesMessage.style.display = 'none';
    activeSetControls.style.display = 'block';

    const set = approachSets[activeSetIndex];
    approachSetNameInput.value = set.name || '';

    renderBubbleList();
  }

  function addNewApproachSet() {
    const newSet = {
      name: `Approach ${approachSets.length + 1}`,
      bubbles: [],
    };
    approachSets.push(newSet);
    activeSetIndex = approachSets.length - 1;
    hasUnsavedChanges = true;
    renderApproachSetTabs();
    renderActiveSetUI();
    renderBubbleLayers();
    approachSetNameInput.focus();
    approachSetNameInput.select();
  }

  function deleteCurrentApproachSet() {
    if (activeSetIndex < 0 || !approachSets[activeSetIndex]) return;

    const setName = approachSets[activeSetIndex].name || `Set ${activeSetIndex + 1}`;
    if (!confirm(`Delete approach set "${setName}"? This cannot be undone.`)) return;

    approachSets.splice(activeSetIndex, 1);
    activeSetIndex = approachSets.length > 0 ? Math.max(0, activeSetIndex - 1) : -1;
    hasUnsavedChanges = true;
    renderApproachSetTabs();
    renderActiveSetUI();
    renderBubbleLayers();
  }

  function startBubblePlacement() {
    if (activeSetIndex < 0) {
      setStatus('Create an approach set first', true);
      return;
    }
    isPlacingBubble = !isPlacingBubble;
    addBubbleBtn.classList.toggle('placing', isPlacingBubble);
    addBubbleBtn.textContent = isPlacingBubble ? 'Click on map...' : '+ Click map to add bubble';

    if (map) {
      map.getContainer().style.cursor = isPlacingBubble ? 'crosshair' : '';
    }
  }

  function handleMapClick(e) {
    if (!isPlacingBubble || activeSetIndex < 0) return;

    const set = approachSets[activeSetIndex];
    // Use the radius of the previous #1 bubble (which will become #2), or default
    const previousRadius = set.bubbles.length > 0 ? set.bubbles[0].radius_m : DEFAULT_BUBBLE_RADIUS_M;
    // Shift all existing bubbles down (increment their order)
    set.bubbles.forEach((b) => {
      b.order += 1;
    });
    // New bubble becomes #1
    const newBubble = {
      lat: e.latlng.lat,
      lng: e.latlng.lng,
      radius_m: previousRadius,
      order: 1,
    };
    set.bubbles.unshift(newBubble);
    hasUnsavedChanges = true;

    // Exit placement mode
    isPlacingBubble = false;
    addBubbleBtn.classList.remove('placing');
    addBubbleBtn.textContent = '+ Click map to add bubble';
    if (map) {
      map.getContainer().style.cursor = '';
    }

    renderBubbleList();
    renderBubbleLayers();
    updateBubbleCount();
  }

  // Wire up bubble UI event handlers
  if (addApproachSetBtn) {
    addApproachSetBtn.addEventListener('click', addNewApproachSet);
  }

  if (deleteApproachSetBtn) {
    deleteApproachSetBtn.addEventListener('click', deleteCurrentApproachSet);
  }

  if (approachSetNameInput) {
    approachSetNameInput.addEventListener('input', (e) => {
      if (activeSetIndex >= 0 && approachSets[activeSetIndex]) {
        approachSets[activeSetIndex].name = e.target.value;
        hasUnsavedChanges = true;
        renderApproachSetTabs();
      }
    });
  }

  if (addBubbleBtn) {
    addBubbleBtn.addEventListener('click', startBubblePlacement);
  }

  if (map) {
    map.on('click', handleMapClick);
  }

  // ==================== END BUBBLE MANAGEMENT ====================

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

    // Load approach sets for this stop
    approachSets = [];
    if (stop.ApproachSets && Array.isArray(stop.ApproachSets)) {
      approachSets = stop.ApproachSets.map((set) => ({
        name: set.name || '',
        bubbles: (set.bubbles || []).map((b, idx) => ({
          lat: b.lat,
          lng: b.lng,
          radius_m: b.radius_m || DEFAULT_BUBBLE_RADIUS_M,
          order: b.order || idx + 1,
        })),
      }));
    }
    activeSetIndex = approachSets.length > 0 ? 0 : -1;
    renderApproachSetTabs();
    renderActiveSetUI();
    renderBubbleLayers();

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

    // Prepare approach sets for saving
    const approachSetsPayload = approachSets.map((set) => ({
      name: set.name,
      bubbles: set.bubbles.map((b) => ({
        lat: b.lat,
        lng: b.lng,
        radius_m: b.radius_m,
        order: b.order,
      })),
    }));

    const payloads = group.ids.map((id) => ({
      stop_id: id,
      radius_m: radiusVal,
      tolerance_deg: toleranceVal,
      bearing_deg: bearingVal,
      approach_sets: approachSetsPayload,
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
              s.ApproachSets = result.approach_sets || [];
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
      approach_sets: [],
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
              stop.ApproachSets = [];
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
    clearBubbleLayers();
    fetchStops();
  });

  if (resetAllButton) {
    resetAllButton.addEventListener('click', resetAllStops);
  }

  fetchStops();
})();
