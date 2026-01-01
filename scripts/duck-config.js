/**
 * DUCK Config Editor
 * Handles polygon drawing, config loading/saving, and validation
 */

(function () {
  'use strict';

  // Default config values
  const DEFAULTS = {
    volume: 80,
    demo_mode: false,
    thresholds: {
      moving_speed_mph: 3.0,
      gps_debounce_ms: 1000,
      power_debounce_ms: 500,
      shutdown_timeout_sec: 60,
      config_timeout_sec: 900
    },
    audio: {
      alarm: '/duck/audio/alarm.mp3',
      boot_music: '/duck/audio/boot_music.mp3',
      boot_complete: '/duck/audio/boot_complete.mp3',
      gps_fix_acquired: '/duck/audio/gps_fix.mp3',
      shutdown_safe: '/duck/audio/safe_to_power_down.mp3'
    },
    wifi: {
      ssid: 'DUCK-CONFIG',
      password: ''
    }
  };

  // Limits from spec
  const LIMITS = {
    maxHazards: 32,
    maxPolygonPoints: 16,
    minPolygonPoints: 3,
    maxHazardNameLength: 32,
    maxAudioPathLength: 64,
    maxSsidLength: 32,
    maxPasswordLength: 64,
    movingSpeedMin: 0.5,
    movingSpeedMax: 15.0,
    shutdownTimeoutMin: 10,
    shutdownTimeoutMax: 300,
    volumeMin: 0,
    volumeMax: 100
  };

  // State
  let map = null;
  let drawnItems = null;
  let drawControl = null;
  let currentDrawMode = null; // 'yard', 'enter', 'exit'

  // Polygon layers
  let yardLayer = null;
  let hazards = []; // Array of { name, voicePrompt, enterLayer, exitLayer }
  let activeHazardIndex = -1;

  // Polygon styles
  const STYLES = {
    yard: { color: '#3b82f6', fillColor: '#3b82f6', fillOpacity: 0.2, weight: 2 },
    enter: { color: '#ef4444', fillColor: '#ef4444', fillOpacity: 0.2, weight: 2 },
    exit: { color: '#22c55e', fillColor: '#22c55e', fillOpacity: 0.2, weight: 2 }
  };

  // DOM elements
  const elements = {};

  function init() {
    cacheElements();
    initMap();
    bindEvents();
    updateUI();
  }

  function cacheElements() {
    elements.loadConfigFile = document.getElementById('loadConfigFile');
    elements.createDefaultBtn = document.getElementById('createDefaultBtn');
    elements.downloadBtn = document.getElementById('downloadBtn');

    elements.volumeSlider = document.getElementById('volumeSlider');
    elements.volumeValue = document.getElementById('volumeValue');
    elements.demoModeCheckbox = document.getElementById('demoModeCheckbox');

    elements.movingSpeedInput = document.getElementById('movingSpeedInput');
    elements.gpsDebounceInput = document.getElementById('gpsDebounceInput');
    elements.powerDebounceInput = document.getElementById('powerDebounceInput');
    elements.shutdownTimeoutInput = document.getElementById('shutdownTimeoutInput');
    elements.configTimeoutInput = document.getElementById('configTimeoutInput');

    elements.wifiSsidInput = document.getElementById('wifiSsidInput');
    elements.wifiPasswordInput = document.getElementById('wifiPasswordInput');

    elements.audioAlarmInput = document.getElementById('audioAlarmInput');
    elements.audioBootMusicInput = document.getElementById('audioBootMusicInput');
    elements.audioBootCompleteInput = document.getElementById('audioBootCompleteInput');
    elements.audioGpsFixInput = document.getElementById('audioGpsFixInput');
    elements.audioShutdownInput = document.getElementById('audioShutdownInput');

    elements.drawYardBtn = document.getElementById('drawYardBtn');
    elements.clearYardBtn = document.getElementById('clearYardBtn');
    elements.yardPointCount = document.getElementById('yardPointCount');
    elements.yardValidation = document.getElementById('yardValidation');

    elements.hazardTabs = document.getElementById('hazardTabs');
    elements.addHazardBtn = document.getElementById('addHazardBtn');
    elements.hazardCount = document.getElementById('hazardCount');
    elements.noHazardsMessage = document.getElementById('noHazardsMessage');
    elements.activeHazardControls = document.getElementById('activeHazardControls');

    elements.hazardNameInput = document.getElementById('hazardNameInput');
    elements.hazardVoicePromptInput = document.getElementById('hazardVoicePromptInput');
    elements.drawEnterBtn = document.getElementById('drawEnterBtn');
    elements.clearEnterBtn = document.getElementById('clearEnterBtn');
    elements.enterPointCount = document.getElementById('enterPointCount');
    elements.drawExitBtn = document.getElementById('drawExitBtn');
    elements.clearExitBtn = document.getElementById('clearExitBtn');
    elements.exitPointCount = document.getElementById('exitPointCount');
    elements.deleteHazardBtn = document.getElementById('deleteHazardBtn');

    elements.validationSummary = document.getElementById('validationSummary');
  }

  function initMap() {
    // Center on Charlottesville, VA by default
    map = L.map('map').setView([38.0293, -78.4767], 14);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    // Initialize FeatureGroup for drawn items
    drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    // Initialize draw control (hidden by default, we control drawing programmatically)
    drawControl = new L.Control.Draw({
      position: 'topright',
      draw: {
        polyline: false,
        rectangle: false,
        circle: false,
        circlemarker: false,
        marker: false,
        polygon: {
          allowIntersection: false,
          showArea: true,
          shapeOptions: STYLES.yard
        }
      },
      edit: {
        featureGroup: drawnItems,
        remove: false
      }
    });
    map.addControl(drawControl);

    // Handle draw events
    map.on(L.Draw.Event.CREATED, onPolygonCreated);
    map.on(L.Draw.Event.EDITED, onPolygonsEdited);
  }

  function bindEvents() {
    // File loading
    elements.loadConfigFile.addEventListener('change', handleFileLoad);
    elements.createDefaultBtn.addEventListener('click', createDefaultConfig);
    elements.downloadBtn.addEventListener('click', downloadConfig);

    // Volume slider
    elements.volumeSlider.addEventListener('input', () => {
      elements.volumeValue.textContent = elements.volumeSlider.value;
    });

    // Collapsible sections
    document.querySelectorAll('.collapsible-header').forEach(header => {
      header.addEventListener('click', () => {
        const targetId = header.dataset.target;
        const content = document.getElementById(targetId);
        const icon = header.querySelector('.collapse-icon');
        if (content.style.display === 'none') {
          content.style.display = 'block';
          icon.textContent = '-';
        } else {
          content.style.display = 'none';
          icon.textContent = '+';
        }
      });
    });

    // Yard polygon controls
    elements.drawYardBtn.addEventListener('click', () => startDrawing('yard'));
    elements.clearYardBtn.addEventListener('click', clearYardPolygon);

    // Hazard management
    elements.addHazardBtn.addEventListener('click', addHazard);
    elements.deleteHazardBtn.addEventListener('click', deleteActiveHazard);
    elements.hazardNameInput.addEventListener('input', updateActiveHazardName);
    elements.hazardVoicePromptInput.addEventListener('input', updateActiveHazardVoicePrompt);

    // Hazard polygon controls
    elements.drawEnterBtn.addEventListener('click', () => startDrawing('enter'));
    elements.clearEnterBtn.addEventListener('click', () => clearHazardPolygon('enter'));
    elements.drawExitBtn.addEventListener('click', () => startDrawing('exit'));
    elements.clearExitBtn.addEventListener('click', () => clearHazardPolygon('exit'));
  }

  function startDrawing(mode) {
    currentDrawMode = mode;

    // Update button states
    [elements.drawYardBtn, elements.drawEnterBtn, elements.drawExitBtn].forEach(btn => {
      btn.classList.remove('active');
    });

    let style = STYLES[mode];
    let btn;
    switch (mode) {
      case 'yard':
        btn = elements.drawYardBtn;
        break;
      case 'enter':
        btn = elements.drawEnterBtn;
        break;
      case 'exit':
        btn = elements.drawExitBtn;
        break;
    }
    if (btn) btn.classList.add('active');

    // Start polygon draw
    const drawHandler = new L.Draw.Polygon(map, {
      allowIntersection: false,
      showArea: true,
      shapeOptions: style
    });
    drawHandler.enable();
  }

  function onPolygonCreated(e) {
    const layer = e.layer;

    // Check point count limit
    const coords = layer.getLatLngs()[0];
    if (coords.length > LIMITS.maxPolygonPoints) {
      alert(`Polygon cannot have more than ${LIMITS.maxPolygonPoints} points. You drew ${coords.length} points.`);
      return;
    }

    switch (currentDrawMode) {
      case 'yard':
        if (yardLayer) {
          drawnItems.removeLayer(yardLayer);
        }
        layer.setStyle(STYLES.yard);
        yardLayer = layer;
        drawnItems.addLayer(layer);
        break;

      case 'enter':
        if (activeHazardIndex >= 0 && hazards[activeHazardIndex]) {
          if (hazards[activeHazardIndex].enterLayer) {
            drawnItems.removeLayer(hazards[activeHazardIndex].enterLayer);
          }
          layer.setStyle(STYLES.enter);
          hazards[activeHazardIndex].enterLayer = layer;
          drawnItems.addLayer(layer);
        }
        break;

      case 'exit':
        if (activeHazardIndex >= 0 && hazards[activeHazardIndex]) {
          if (hazards[activeHazardIndex].exitLayer) {
            drawnItems.removeLayer(hazards[activeHazardIndex].exitLayer);
          }
          layer.setStyle(STYLES.exit);
          hazards[activeHazardIndex].exitLayer = layer;
          drawnItems.addLayer(layer);
        }
        break;
    }

    // Reset draw mode
    currentDrawMode = null;
    [elements.drawYardBtn, elements.drawEnterBtn, elements.drawExitBtn].forEach(btn => {
      btn.classList.remove('active');
    });

    updateUI();
  }

  function onPolygonsEdited() {
    updateUI();
  }

  function clearYardPolygon() {
    if (yardLayer) {
      drawnItems.removeLayer(yardLayer);
      yardLayer = null;
      updateUI();
    }
  }

  function clearHazardPolygon(type) {
    if (activeHazardIndex < 0 || !hazards[activeHazardIndex]) return;

    const hazard = hazards[activeHazardIndex];
    if (type === 'enter' && hazard.enterLayer) {
      drawnItems.removeLayer(hazard.enterLayer);
      hazard.enterLayer = null;
    } else if (type === 'exit' && hazard.exitLayer) {
      drawnItems.removeLayer(hazard.exitLayer);
      hazard.exitLayer = null;
    }
    updateUI();
  }

  function addHazard() {
    if (hazards.length >= LIMITS.maxHazards) {
      alert(`Maximum of ${LIMITS.maxHazards} hazards allowed.`);
      return;
    }

    const hazard = {
      name: `Hazard ${hazards.length + 1}`,
      voicePrompt: '/duck/audio/hazards/',
      enterLayer: null,
      exitLayer: null
    };
    hazards.push(hazard);
    activeHazardIndex = hazards.length - 1;
    updateUI();
  }

  function deleteActiveHazard() {
    if (activeHazardIndex < 0) return;

    const hazard = hazards[activeHazardIndex];
    if (hazard.enterLayer) drawnItems.removeLayer(hazard.enterLayer);
    if (hazard.exitLayer) drawnItems.removeLayer(hazard.exitLayer);

    hazards.splice(activeHazardIndex, 1);
    activeHazardIndex = hazards.length > 0 ? 0 : -1;
    updateUI();
  }

  function updateActiveHazardName() {
    if (activeHazardIndex >= 0 && hazards[activeHazardIndex]) {
      hazards[activeHazardIndex].name = elements.hazardNameInput.value;
      updateHazardTabs();
    }
  }

  function updateActiveHazardVoicePrompt() {
    if (activeHazardIndex >= 0 && hazards[activeHazardIndex]) {
      hazards[activeHazardIndex].voicePrompt = elements.hazardVoicePromptInput.value;
    }
  }

  function selectHazard(index) {
    activeHazardIndex = index;
    updateUI();

    // Pan to hazard polygons if they exist
    const hazard = hazards[index];
    if (hazard) {
      const bounds = L.latLngBounds([]);
      if (hazard.enterLayer) bounds.extend(hazard.enterLayer.getBounds());
      if (hazard.exitLayer) bounds.extend(hazard.exitLayer.getBounds());
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [50, 50] });
      }
    }
  }

  function updateUI() {
    updateYardUI();
    updateHazardTabs();
    updateHazardUI();
    updateValidation();
  }

  function updateYardUI() {
    if (yardLayer) {
      const coords = yardLayer.getLatLngs()[0];
      elements.yardPointCount.textContent = `${coords.length} pts`;
      if (coords.length >= LIMITS.minPolygonPoints) {
        elements.yardValidation.textContent = 'Valid polygon';
        elements.yardValidation.className = 'validation-status valid';
      } else {
        elements.yardValidation.textContent = `Need at least ${LIMITS.minPolygonPoints} points`;
        elements.yardValidation.className = 'validation-status invalid';
      }
    } else {
      elements.yardPointCount.textContent = '0 pts';
      elements.yardValidation.textContent = 'Required - draw a polygon';
      elements.yardValidation.className = 'validation-status invalid';
    }
  }

  function updateHazardTabs() {
    // Clear existing tabs (except add button)
    const tabs = elements.hazardTabs.querySelectorAll('.hazard-tab:not(.add-new)');
    tabs.forEach(tab => tab.remove());

    // Add tabs for each hazard
    hazards.forEach((hazard, index) => {
      const tab = document.createElement('button');
      tab.type = 'button';
      tab.className = 'hazard-tab' + (index === activeHazardIndex ? ' active' : '');
      tab.textContent = hazard.name || `Hazard ${index + 1}`;
      tab.addEventListener('click', () => selectHazard(index));
      elements.hazardTabs.insertBefore(tab, elements.addHazardBtn);
    });

    elements.hazardCount.textContent = hazards.length;
  }

  function updateHazardUI() {
    if (hazards.length === 0) {
      elements.noHazardsMessage.style.display = 'block';
      elements.activeHazardControls.style.display = 'none';
      return;
    }

    elements.noHazardsMessage.style.display = 'none';
    elements.activeHazardControls.style.display = 'block';

    if (activeHazardIndex >= 0 && hazards[activeHazardIndex]) {
      const hazard = hazards[activeHazardIndex];
      elements.hazardNameInput.value = hazard.name || '';
      elements.hazardVoicePromptInput.value = hazard.voicePrompt || '';

      // Enter polygon
      if (hazard.enterLayer) {
        const coords = hazard.enterLayer.getLatLngs()[0];
        elements.enterPointCount.textContent = `${coords.length} pts`;
      } else {
        elements.enterPointCount.textContent = '0 pts';
      }

      // Exit polygon
      if (hazard.exitLayer) {
        const coords = hazard.exitLayer.getLatLngs()[0];
        elements.exitPointCount.textContent = `${coords.length} pts`;
      } else {
        elements.exitPointCount.textContent = '0 pts';
      }
    }
  }

  function updateValidation() {
    const errors = validateConfig();
    const summary = elements.validationSummary;

    if (errors.length === 0) {
      summary.innerHTML = '<p class="success">Config is valid and ready to download.</p>';
    } else {
      summary.innerHTML = '<ul>' + errors.map(e => `<li class="error">${e}</li>`).join('') + '</ul>';
    }
  }

  function validateConfig() {
    const errors = [];

    // Validate yard
    if (!yardLayer) {
      errors.push('Yard polygon is required');
    } else {
      const coords = yardLayer.getLatLngs()[0];
      if (coords.length < LIMITS.minPolygonPoints) {
        errors.push(`Yard polygon needs at least ${LIMITS.minPolygonPoints} points`);
      }
    }

    // Validate at least one hazard
    if (hazards.length === 0) {
      errors.push('At least one hazard is required');
    }

    // Validate each hazard
    hazards.forEach((hazard, index) => {
      const prefix = `Hazard ${index + 1}`;
      if (!hazard.name || hazard.name.trim() === '') {
        errors.push(`${prefix}: Name is required`);
      } else if (hazard.name.length > LIMITS.maxHazardNameLength) {
        errors.push(`${prefix}: Name exceeds ${LIMITS.maxHazardNameLength} characters`);
      }

      if (!hazard.enterLayer) {
        errors.push(`${prefix}: Enter polygon is required`);
      } else {
        const coords = hazard.enterLayer.getLatLngs()[0];
        if (coords.length < LIMITS.minPolygonPoints) {
          errors.push(`${prefix}: Enter polygon needs at least ${LIMITS.minPolygonPoints} points`);
        }
      }

      if (!hazard.exitLayer) {
        errors.push(`${prefix}: Exit polygon is required`);
      } else {
        const coords = hazard.exitLayer.getLatLngs()[0];
        if (coords.length < LIMITS.minPolygonPoints) {
          errors.push(`${prefix}: Exit polygon needs at least ${LIMITS.minPolygonPoints} points`);
        }
      }
    });

    // Validate thresholds
    const speed = parseFloat(elements.movingSpeedInput.value);
    if (isNaN(speed) || speed < LIMITS.movingSpeedMin || speed > LIMITS.movingSpeedMax) {
      errors.push(`Moving speed must be between ${LIMITS.movingSpeedMin} and ${LIMITS.movingSpeedMax} mph`);
    }

    const shutdown = parseInt(elements.shutdownTimeoutInput.value);
    if (isNaN(shutdown) || shutdown < LIMITS.shutdownTimeoutMin || shutdown > LIMITS.shutdownTimeoutMax) {
      errors.push(`Shutdown timeout must be between ${LIMITS.shutdownTimeoutMin} and ${LIMITS.shutdownTimeoutMax} seconds`);
    }

    const volume = parseInt(elements.volumeSlider.value);
    if (isNaN(volume) || volume < LIMITS.volumeMin || volume > LIMITS.volumeMax) {
      errors.push(`Volume must be between ${LIMITS.volumeMin} and ${LIMITS.volumeMax}`);
    }

    return errors;
  }

  function layerToCoords(layer) {
    // Convert Leaflet polygon to [lon, lat] array format
    const latLngs = layer.getLatLngs()[0];
    return latLngs.map(ll => [
      parseFloat(ll.lng.toFixed(7)),
      parseFloat(ll.lat.toFixed(7))
    ]);
  }

  function coordsToLayer(coords, style) {
    // Convert [lon, lat] array to Leaflet polygon
    const latLngs = coords.map(c => L.latLng(c[1], c[0]));
    const polygon = L.polygon(latLngs, style);
    return polygon;
  }

  function buildConfig() {
    const config = {
      volume: parseInt(elements.volumeSlider.value),
      demo_mode: elements.demoModeCheckbox.checked,
      thresholds: {
        moving_speed_mph: parseFloat(elements.movingSpeedInput.value),
        gps_debounce_ms: parseInt(elements.gpsDebounceInput.value),
        power_debounce_ms: parseInt(elements.powerDebounceInput.value),
        shutdown_timeout_sec: parseInt(elements.shutdownTimeoutInput.value),
        config_timeout_sec: parseInt(elements.configTimeoutInput.value)
      },
      yard: {
        polygon: yardLayer ? layerToCoords(yardLayer) : []
      },
      hazards: hazards.map(h => ({
        name: h.name,
        enter_polygon: h.enterLayer ? layerToCoords(h.enterLayer) : [],
        exit_polygon: h.exitLayer ? layerToCoords(h.exitLayer) : [],
        voice_prompt: h.voicePrompt
      })),
      audio: {
        alarm: elements.audioAlarmInput.value,
        boot_music: elements.audioBootMusicInput.value,
        boot_complete: elements.audioBootCompleteInput.value,
        gps_fix_acquired: elements.audioGpsFixInput.value,
        shutdown_safe: elements.audioShutdownInput.value
      },
      wifi: {
        ssid: elements.wifiSsidInput.value,
        password: elements.wifiPasswordInput.value
      }
    };

    return config;
  }

  function loadConfig(config) {
    // Clear existing
    if (yardLayer) {
      drawnItems.removeLayer(yardLayer);
      yardLayer = null;
    }
    hazards.forEach(h => {
      if (h.enterLayer) drawnItems.removeLayer(h.enterLayer);
      if (h.exitLayer) drawnItems.removeLayer(h.exitLayer);
    });
    hazards = [];
    activeHazardIndex = -1;

    // Load volume
    if (config.volume !== undefined) {
      elements.volumeSlider.value = config.volume;
      elements.volumeValue.textContent = config.volume;
    }

    // Load demo mode
    elements.demoModeCheckbox.checked = config.demo_mode === true;

    // Load thresholds
    if (config.thresholds) {
      if (config.thresholds.moving_speed_mph !== undefined) {
        elements.movingSpeedInput.value = config.thresholds.moving_speed_mph;
      }
      if (config.thresholds.gps_debounce_ms !== undefined) {
        elements.gpsDebounceInput.value = config.thresholds.gps_debounce_ms;
      }
      if (config.thresholds.power_debounce_ms !== undefined) {
        elements.powerDebounceInput.value = config.thresholds.power_debounce_ms;
      }
      if (config.thresholds.shutdown_timeout_sec !== undefined) {
        elements.shutdownTimeoutInput.value = config.thresholds.shutdown_timeout_sec;
      }
      if (config.thresholds.config_timeout_sec !== undefined) {
        elements.configTimeoutInput.value = config.thresholds.config_timeout_sec;
      }
    }

    // Load WiFi
    if (config.wifi) {
      if (config.wifi.ssid !== undefined) {
        elements.wifiSsidInput.value = config.wifi.ssid;
      }
      if (config.wifi.password !== undefined) {
        elements.wifiPasswordInput.value = config.wifi.password;
      }
    }

    // Load audio paths
    if (config.audio) {
      if (config.audio.alarm) elements.audioAlarmInput.value = config.audio.alarm;
      if (config.audio.boot_music) elements.audioBootMusicInput.value = config.audio.boot_music;
      if (config.audio.boot_complete) elements.audioBootCompleteInput.value = config.audio.boot_complete;
      if (config.audio.gps_fix_acquired) elements.audioGpsFixInput.value = config.audio.gps_fix_acquired;
      if (config.audio.shutdown_safe) elements.audioShutdownInput.value = config.audio.shutdown_safe;
    }

    // Load yard polygon
    if (config.yard && config.yard.polygon && config.yard.polygon.length >= 3) {
      yardLayer = coordsToLayer(config.yard.polygon, STYLES.yard);
      drawnItems.addLayer(yardLayer);
    }

    // Load hazards
    if (config.hazards && Array.isArray(config.hazards)) {
      config.hazards.forEach(h => {
        const hazard = {
          name: h.name || '',
          voicePrompt: h.voice_prompt || '',
          enterLayer: null,
          exitLayer: null
        };

        if (h.enter_polygon && h.enter_polygon.length >= 3) {
          hazard.enterLayer = coordsToLayer(h.enter_polygon, STYLES.enter);
          drawnItems.addLayer(hazard.enterLayer);
        }

        if (h.exit_polygon && h.exit_polygon.length >= 3) {
          hazard.exitLayer = coordsToLayer(h.exit_polygon, STYLES.exit);
          drawnItems.addLayer(hazard.exitLayer);
        }

        hazards.push(hazard);
      });

      if (hazards.length > 0) {
        activeHazardIndex = 0;
      }
    }

    // Fit map to all polygons
    if (drawnItems.getLayers().length > 0) {
      map.fitBounds(drawnItems.getBounds(), { padding: [50, 50] });
    }

    updateUI();
  }

  function handleFileLoad(e) {
    const file = e.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = function (event) {
      try {
        const config = JSON.parse(event.target.result);
        loadConfig(config);
      } catch (err) {
        alert('Failed to parse config file: ' + err.message);
      }
    };
    reader.readAsText(file);

    // Reset file input so same file can be loaded again
    e.target.value = '';
  }

  function createDefaultConfig() {
    loadConfig({
      volume: DEFAULTS.volume,
      demo_mode: DEFAULTS.demo_mode,
      thresholds: { ...DEFAULTS.thresholds },
      audio: { ...DEFAULTS.audio },
      wifi: { ...DEFAULTS.wifi },
      yard: { polygon: [] },
      hazards: []
    });
  }

  function downloadConfig() {
    const errors = validateConfig();
    if (errors.length > 0) {
      const proceed = confirm(
        'Config has validation errors:\n\n' +
          errors.join('\n') +
          '\n\nDownload anyway?'
      );
      if (!proceed) return;
    }

    const config = buildConfig();
    const json = JSON.stringify(config, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = 'config.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
