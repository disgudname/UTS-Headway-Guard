const svg = document.getElementById('mapSvg');
const width = 1000;
const height = 800;
const STROKE_WIDTH = 4;
const OVERLAP_SPACING = STROKE_WIDTH + 2;
// Use a very generous tolerance so nearby but non-identical segments are treated as the same
// Increasing the tolerance helps merge routes that follow almost identical roads
  const SEGMENT_TOLERANCE = STROKE_WIDTH * 32; // pixels used to detect near-overlaps
// Aggressively simplify routes since exact lengths are not important
const SIMPLIFY_TOLERANCE = 12;
// Approximate tolerance in degrees for matching segments to real roads
// Bumped up so routes separated by a median still resolve to the same road
const ROAD_TOLERANCE = 0.0003; // ~33m

function segmentKey(x1, y1, x2, y2, roadId, routeIdx) {
  const q = v => Math.round(v / SEGMENT_TOLERANCE) * SEGMENT_TOLERANCE;
  let base;
  if (x1 < x2 || (x1 === x2 && y1 <= y2)) {
    base = `${q(x1)},${q(y1)},${q(x2)},${q(y2)}`;
  } else {
    base = `${q(x2)},${q(y2)},${q(x1)},${q(y1)}`;
  }
  return `${roadId || 'r' + routeIdx}:${base}`;
}

// Ramer-Douglas-Peucker line simplification
function simplifyLine(points, tolerance) {
  if (points.length <= 2) return points;
  const sqTol = tolerance * tolerance;

  function getSqSegDist(p, a, b) {
    let x = a[0], y = a[1];
    let dx = b[0] - x, dy = b[1] - y;
    if (dx !== 0 || dy !== 0) {
      const t = ((p[0] - x) * dx + (p[1] - y) * dy) / (dx * dx + dy * dy);
      if (t > 1) { x = b[0]; y = b[1]; }
      else if (t > 0) { x += dx * t; y += dy * t; }
    }
    dx = p[0] - x; dy = p[1] - y;
    return dx * dx + dy * dy;
  }

  function simplifyDP(pts, first, last, res) {
    let maxDist = 0, index = first;
    for (let i = first + 1; i < last; i++) {
      const dist = getSqSegDist(pts[i], pts[first], pts[last]);
      if (dist > maxDist) { index = i; maxDist = dist; }
    }
    if (maxDist > sqTol) {
      if (index - first > 1) simplifyDP(pts, first, index, res);
      res.push(pts[index]);
      if (last - index > 1) simplifyDP(pts, index, last, res);
    }
  }

  const res = [points[0]];
  simplifyDP(points, 0, points.length - 1, res);
  res.push(points[points.length - 1]);
  return res;
}

// Build a straight polyline path
function buildStraightPath(points) {
  if (!points.length) return '';
  let d = `M ${points[0][0]} ${points[0][1]}`;
  for (let i = 1; i < points.length; i++) {
    const [x, y] = points[i];
    d += ` L ${x} ${y}`;
  }
  return d;
}

// Snap polyline segments to 45° increments
function snapToAngles(points) {
  if (points.length <= 1) return points;
  const snapped = [points[0].slice()];
  for (let i = 1; i < points.length; i++) {
    const prev = snapped[i - 1];
    const curr = points[i];
    const dx = curr[0] - prev[0];
    const dy = curr[1] - prev[1];
    const len = Math.hypot(dx, dy);
    if (!len) {
      snapped.push(prev.slice());
      continue;
    }
    const angle = Math.atan2(dy, dx);
    const snappedAngle = Math.round(angle / (Math.PI / 4)) * (Math.PI / 4);
    const nx = prev[0] + Math.cos(snappedAngle) * len;
    const ny = prev[1] + Math.sin(snappedAngle) * len;
    snapped.push([nx, ny]);
  }
  return snapped;
}

// Ensure a polyline forms a closed loop by repeating the first point at the end
function ensureClosed(points) {
  if (points.length > 1) {
    const [fx, fy] = points[0];
    const [lx, ly] = points[points.length - 1];
    if (fx !== lx || fy !== ly) {
      points.push([fx, fy]);
    }
  }
}

async function buildRoadLookup(routes) {
  let minLat = Infinity, minLon = Infinity, maxLat = -Infinity, maxLon = -Infinity;
  routes.forEach(r => r.points.forEach(([lat, lon]) => {
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon;
    if (lon > maxLon) maxLon = lon;
  }));
  const query = `[out:json][timeout:25];way[highway](${minLat},${minLon},${maxLat},${maxLon});out geom;`;
  const url = 'https://overpass-api.de/api/interpreter?data=' + encodeURIComponent(query);
  let json;
  try {
    json = await fetch(url).then(r => r.json());
  } catch (e) {
    console.error('Error loading road data', e);
    return { lookup: null, roads: new Map() };
  }
  const segments = [];
  const roadGeoms = new Map();
  (json.elements || []).forEach(el => {
    if (el.type === 'way' && el.geometry) {
      // Group parallel ways with the same name (e.g. divided highways)
      const roadName = (el.tags && el.tags.name) || el.id;
      const geom = el.geometry.map(p => [p.lat, p.lon]);
      if (!roadGeoms.has(roadName)) {
        roadGeoms.set(roadName, geom);
      }
      for (let i = 0; i < geom.length - 1; i++) {
        const a = geom[i];
        const b = geom[i + 1];
        segments.push({
          groupId: roadName,
          a: [a[0], a[1]],
          b: [b[0], b[1]]
        });
      }
    }
  });
  const tol2 = ROAD_TOLERANCE * ROAD_TOLERANCE;
  function sqDistPointToSeg(lat, lon, seg) {
    const x = lon, y = lat;
    const x1 = seg.a[1], y1 = seg.a[0];
    const x2 = seg.b[1], y2 = seg.b[0];
    const dx = x2 - x1;
    const dy = y2 - y1;
    let t = 0;
    if (dx !== 0 || dy !== 0) {
      t = ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy);
      t = Math.max(0, Math.min(1, t));
    }
    const px = x1 + t * dx;
    const py = y1 + t * dy;
    const ddx = x - px;
    const ddy = y - py;
    return ddx * ddx + ddy * ddy;
  }
  function lookup(lat, lon) {
    let best = null;
    let bestDist = Infinity;
    segments.forEach(seg => {
      const d = sqDistPointToSeg(lat, lon, seg);
      if (d < bestDist) {
        bestDist = d;
        best = seg.groupId;
      }
    });
    return bestDist < tol2 ? best : null;
  }
  return { lookup, roads: roadGeoms };
}

function scaleAndRender(routes, roadLookup, roadGeoms) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  routes.forEach(r => r.points.forEach(([y, x]) => {
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }));

  const scale = Math.min(width / (maxX - minX), height / (maxY - minY));
  const offsetX = (width - (maxX - minX) * scale) / 2;
  const offsetY = (height - (maxY - minY) * scale) / 2;

  // Pre-scale road geometries so route paths can snap to them
  const scaledRoads = new Map();
  roadGeoms.forEach((pts, id) => {
    const scaled = pts.map(([y, x]) => {
      const sx = (x - minX) * scale + offsetX;
      const sy = height - ((y - minY) * scale + offsetY);
      return [sx, sy];
    });
    scaledRoads.set(id, scaled);
  });

  // Scale and simplify all points first
  routes.forEach(r => {
    const snapped = [];
    let lastRoadId = null;
    for (let i = 0; i < r.points.length; i++) {
      const [y, x] = r.points[i];
      const sx = (x - minX) * scale + offsetX;
      const sy = height - ((y - minY) * scale + offsetY);
      if (roadLookup) {
        const roadId = roadLookup(y, x);
        if (roadId && scaledRoads.has(roadId)) {
          if (roadId !== lastRoadId) {
            scaledRoads.get(roadId).forEach(pt => snapped.push(pt));
            lastRoadId = roadId;
          }
          continue;
        }
        lastRoadId = null;
      }
      snapped.push([sx, sy]);
    }
    r.scaled = snapped;
    r.scaled = simplifyLine(r.scaled, SIMPLIFY_TOLERANCE);
    r.scaled = snapToAngles(r.scaled);
    r.scaled = simplifyLine(r.scaled, SIMPLIFY_TOLERANCE / 2);
    // Quantize coordinates to reduce tiny differences between near-identical paths
    r.scaled = r.scaled.map(([x, y]) => [Math.round(x), Math.round(y)]);
      ensureClosed(r.scaled);
    });

  // Map of segments to routes that share them
  const segMap = new Map();
  const invScale = 1 / scale;
  routes.forEach((r, ridx) => {
    const pts = r.scaled;
    for (let i = 0; i < pts.length - 1; i++) {
      const [x1, y1] = pts[i];
      const [x2, y2] = pts[i + 1];
      let roadId = null;
      if (roadLookup) {
        const midX = (x1 + x2) / 2;
        const midY = (y1 + y2) / 2;
        const lon = (midX - offsetX) * invScale + minX;
        const lat = ((height - midY) - offsetY) * invScale + minY;
        roadId = roadLookup(lat, lon);
      }
      const key = segmentKey(x1, y1, x2, y2, roadId, ridx);
      if (!segMap.has(key)) segMap.set(key, []);
      segMap.get(key).push({ route: ridx, idx: i });
    }
  });

  // Prepare offset accumulators
  routes.forEach(r => {
    r.offsets = Array(r.scaled.length).fill(0).map(() => [0, 0]);
    r.counts = Array(r.scaled.length).fill(0);
    r.overlapSegments = [];
  });

  // Compute offsets for overlapping segments
  const overlaps = [];
  segMap.forEach((group, key) => {
    // Group by route so that a route overlapping itself isn't offset
    const routeGroups = new Map();
    group.forEach(info => {
      if (!routeGroups.has(info.route)) routeGroups.set(info.route, []);
      routeGroups.get(info.route).push(info);
    });

    // Build entries for each route, capturing orientation so opposite
    // directions can be paired and drawn on top of each other.
    const routeEntries = [];
    routeGroups.forEach((infos, routeId) => {
      const pts = routes[routeId].scaled;
      const info = infos[0];
      let startIdx = info.idx;
      let endIdx = info.idx + 1;
      let pStart = pts[startIdx];
      let pEnd = pts[endIdx];
      let dir = 1;
      if (pStart[0] > pEnd[0] || (pStart[0] === pEnd[0] && pStart[1] > pEnd[1])) {
        [pStart, pEnd] = [pEnd, pStart];
        startIdx = info.idx + 1;
        endIdx = info.idx;
        dir = -1;
      }
      routeEntries.push({ routeId, infos, startIdx, endIdx, pStart, pEnd, dir });
    });

    const nAll = routeEntries.length;
    if (nAll > 1) overlaps.push({ segment: key, routes: routeEntries.map(r => r.routeId) });

    // Average the positions so nearly overlapping segments line up
    const avgStart = [0, 0];
    const avgEnd = [0, 0];
    routeEntries.forEach(re => {
      avgStart[0] += re.pStart[0];
      avgStart[1] += re.pStart[1];
      avgEnd[0] += re.pEnd[0];
      avgEnd[1] += re.pEnd[1];
    });
    avgStart[0] /= nAll; avgStart[1] /= nAll;
    avgEnd[0] /= nAll; avgEnd[1] /= nAll;
    const dx = avgEnd[0] - avgStart[0];
    const dy = avgEnd[1] - avgStart[1];
    const len = Math.hypot(dx, dy) || 1;

    // Pair routes travelling in opposite directions so they share an offset
    const positives = routeEntries.filter(r => r.dir === 1);
    const negatives = routeEntries.filter(r => r.dir === -1);
    const groupsForOffset = [];
    const m = Math.min(positives.length, negatives.length);
    for (let i = 0; i < m; i++) {
      groupsForOffset.push([positives[i], negatives[i]]);
    }
    for (let i = m; i < positives.length; i++) groupsForOffset.push([positives[i]]);
    for (let i = m; i < negatives.length; i++) groupsForOffset.push([negatives[i]]);
    const n = groupsForOffset.length;

    groupsForOffset.forEach((entries, idx) => {
      const offset = n > 1 ? (idx - (n - 1) / 2) * OVERLAP_SPACING : 0;
      const offX = -dy / len * offset;
      const offY = dx / len * offset;
      entries.forEach(re => {
        const pts = routes[re.routeId].scaled;
        re.infos.forEach(info => {
          let startIdx = info.idx;
          let endIdx = info.idx + 1;
          let pStart = pts[startIdx];
          let pEnd = pts[endIdx];
          if (pStart[0] > pEnd[0] || (pStart[0] === pEnd[0] && pStart[1] > pEnd[1])) {
            [pStart, pEnd] = [pEnd, pStart];
            startIdx = info.idx + 1;
            endIdx = info.idx;
          }
          const route = routes[re.routeId];
          route.offsets[startIdx][0] += (avgStart[0] - pStart[0]) + offX;
          route.offsets[startIdx][1] += (avgStart[1] - pStart[1]) + offY;
          route.offsets[endIdx][0] += (avgEnd[0] - pEnd[0]) + offX;
          route.offsets[endIdx][1] += (avgEnd[1] - pEnd[1]) + offY;
          route.counts[startIdx]++;
          route.counts[endIdx]++;
          if (n > 1) route.overlapSegments.push(info.idx);
        });
      });
    });
  });
  if (overlaps.length) console.log('Overlapping segments', overlaps);

  // Render paths with averaged offsets
  routes.forEach(r => {
    const pts = r.scaled.map((p, i) => {
      if (i === 0 || i === r.scaled.length - 1) {
        return p;
      }
      if (r.counts[i]) {
        return [p[0] + r.offsets[i][0] / r.counts[i], p[1] + r.offsets[i][1] / r.counts[i]];
      }
      return p;
    });
    const d = buildStraightPath(pts);
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', d);
    path.setAttribute('fill', 'none');
    path.setAttribute('stroke', r.color || '#000');
    path.setAttribute('stroke-width', STROKE_WIDTH);
    path.setAttribute('stroke-linejoin', 'miter');
    path.setAttribute('stroke-linecap', 'round');
    svg.appendChild(path);
  });
}

(async () => {
  try {
    const [routeData, vehicleData] = await Promise.all([
      fetch('https://uva.transloc.com/Services/JSONPRelay.svc/GetRoutesForMapWithScheduleWithEncodedLine?APIKey=8882812681').then(r => r.json()),
      fetch('https://uva.transloc.com/Services/JSONPRelay.svc/GetMapVehiclePoints?APIKey=8882812681&returnVehiclesNotAssignedToRoute=true').then(r => r.json())
    ]);
    const activeRouteIds = new Set(
      (vehicleData || [])
        .filter(v => v.RouteID && v.RouteID > 0)
        .map(v => v.RouteID)
    );
    const routes = [];
    const seenRouteIds = new Set();
    (routeData || []).forEach(route => {
      // Only include routes that currently have an active vehicle assigned
      if (
        activeRouteIds.has(route.RouteID) &&
        route.EncodedPolyline &&
        !seenRouteIds.has(route.RouteID)
      ) {
        seenRouteIds.add(route.RouteID);
        const decoded = polyline.decode(route.EncodedPolyline);
        ensureClosed(decoded);
        routes.push({
          color: route.MapLineColor || route.Color || '#000',
          points: decoded
        });
      }
    });
    const { lookup: roadLookup, roads: roadGeoms } = await buildRoadLookup(routes);
    scaleAndRender(routes, roadLookup, roadGeoms);
  } catch (err) {
    console.error('Error loading data', err);
  }
})();
