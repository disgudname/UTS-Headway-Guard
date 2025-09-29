'use strict';

(function() {
  function initializePlanesFeature(options) {
    const {
      getMap,
      getPlaneLayer,
      updateToggleButton,
      onVisibilityChange,
      initialVisibility = false
    } = options || {};

    function getMapInstance() {
      return typeof getMap === 'function' ? getMap() : null;
    }

    function getLayer() {
      if (typeof getPlaneLayer === 'function') {
        return getPlaneLayer();
      }
      return typeof window !== 'undefined' ? window.PlaneLayer : undefined;
    }

    let currentVisible = !!initialVisibility;

    function notifyVisibility(visible) {
      currentVisible = !!visible;
      if (typeof onVisibilityChange === 'function') {
        try {
          onVisibilityChange(currentVisible);
        } catch (error) {
          console.error('Error notifying aircraft visibility change:', error);
        }
      }
      if (typeof updateToggleButton === 'function') {
        try {
          updateToggleButton();
        } catch (error) {
          console.error('Error updating aircraft toggle button:', error);
        }
      }
    }

    function setVisibility(visible) {
      const desiredVisibility = !!visible;
      const planeLayer = getLayer();
      if (!desiredVisibility) {
        if (planeLayer && planeLayer.isStarted) {
          try {
            if (typeof planeLayer.dispose === 'function') {
              planeLayer.dispose();
            } else if (typeof planeLayer.stop === 'function') {
              planeLayer.stop();
            }
          } catch (error) {
            console.error('Error stopping aircraft layer:', error);
          }
        }
        notifyVisibility(false);
        return;
      }

      const map = getMapInstance();
      if (!map) {
        notifyVisibility(false);
        return;
      }
      if (!planeLayer) {
        console.warn('Plane layer is unavailable; unable to show aircraft.');
        notifyVisibility(false);
        return;
      }
      try {
        if (!planeLayer.isStarted) {
          if (typeof planeLayer.init === 'function') {
            planeLayer.init(map);
          } else if (typeof planeLayer.start === 'function') {
            planeLayer.start();
          }
        }
      } catch (error) {
        console.error('Error initializing aircraft layer:', error);
      }
      notifyVisibility(!!(planeLayer && planeLayer.isStarted));
    }

    function toggleVisibility() {
      setVisibility(!currentVisible);
    }

    if (currentVisible) {
      setVisibility(true);
    } else {
      notifyVisibility(false);
    }

    return {
      setVisibility,
      toggleVisibility
    };
  }

  window.initializePlanesFeature = initializePlanesFeature;
})();
