const svg = document.getElementById('mapSvg');
const width = 1000;
const height = 800;
const STROKE_WIDTH = 4;
const GRID_SIZE = 10;

// Ramer-Douglas-Peucker simplification
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

// Snap segments to 45° increments
function snap45(points) {
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

function snapToGrid(points, size) {
  return points.map(([x, y]) => [Math.round(x / size) * size, Math.round(y / size) * size]);
}

function snapVertices(routes, size) {
  const buckets = new Map();
  const key = (x, y) => `${Math.round(x / size) * size},${Math.round(y / size) * size}`;
  routes.forEach(r => {
    r.scaled.forEach(p => {
      const k = key(p[0], p[1]);
      if (!buckets.has(k)) buckets.set(k, []);
      buckets.get(k).push(p);
    });
  });
  buckets.forEach(list => {
    let sx = 0, sy = 0;
    list.forEach(([x, y]) => { sx += x; sy += y; });
    sx /= list.length; sy /= list.length;
    list.forEach(p => { p[0] = sx; p[1] = sy; });
  });
}

// Insert vertices where another route has a point along a segment.
// This helps ensure routes that share the same roadway use identical segmentation.
function insertSharedVertices(routes, tol) {
  function dist(a, b) { return Math.hypot(a[0] - b[0], a[1] - b[1]); }
  function pointOnSegment(p, a, b) {
    const lenSq = (b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2;
    const cross = (b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0]);
    if (cross * cross > tol * tol * lenSq) return false;
    const dot = (p[0] - a[0]) * (b[0] - a[0]) + (p[1] - a[1]) * (b[1] - a[1]);
    if (dot < 0) return false;
    if (dot > lenSq) return false;
    return true;
  }
  routes.forEach(r1 => {
    let i = 0;
    while (i < r1.scaled.length - 1) {
      const a = r1.scaled[i];
      const b = r1.scaled[i + 1];
      const inserts = [];
      routes.forEach(r2 => {
        if (r1 === r2) return;
        r2.scaled.forEach(p => {
          if (dist(p, a) < tol || dist(p, b) < tol) return;
          if (pointOnSegment(p, a, b)) {
            inserts.push({ pt: [p[0], p[1]], d: dist(p, a) });
          }
        });
      });
      if (inserts.length) {
        inserts.sort((u, v) => u.d - v.d);
        inserts.forEach(({ pt }, idx) => {
          r1.scaled.splice(i + 1 + idx, 0, pt);
        });
        i += inserts.length;
      }
      i++;
    }
  });
}

// Simple moving average smoothing preserving endpoints
function smoothPath(points) {
  if (points.length <= 2) return points;
  const smoothed = [points[0].slice()];
  for (let i = 1; i < points.length - 1; i++) {
    const [x0, y0] = points[i - 1];
    const [x1, y1] = points[i];
    const [x2, y2] = points[i + 1];
    smoothed.push([(x0 + x1 + x2) / 3, (y0 + y1 + y2) / 3]);
  }
  smoothed.push(points[points.length - 1].slice());
  return smoothed;
}

// Build a map of segments that occupy the same roadway
function groupSegments(routes, tolerance) {
  const q = v => Math.round(v / tolerance) * tolerance;
  const segMap = new Map();
  function key(p1, p2) {
    let [x1, y1] = p1;
    let [x2, y2] = p2;
    if (x1 > x2 || (x1 === x2 && y1 > y2)) {
      [x1, y1, x2, y2] = [x2, y2, x1, y1];
    }
    return `${q(x1)},${q(y1)},${q(x2)},${q(y2)}`;
  }
  routes.forEach(r => {
    for (let i = 0; i < r.scaled.length - 1; i++) {
      const k = key(r.scaled[i], r.scaled[i + 1]);
      if (!segMap.has(k)) segMap.set(k, []);
      segMap.get(k).push({ route: r, idx: i });
    }
  });
  return segMap;
}

// Align all segments in the same group to shared coordinates
// Only segments whose endpoints are within the given tolerance are aligned.
function alignSharedSegments(segMap, tol) {
  function dist(a, b) { return Math.hypot(a[0] - b[0], a[1] - b[1]); }
  segMap.forEach(entries => {
    if (entries.length < 2) return;
    const ref = entries[0];
    const a0 = ref.route.scaled[ref.idx];
    const b0 = ref.route.scaled[ref.idx + 1];
    const matched = entries.filter(({ route, idx }) => {
      const s = route.scaled[idx];
      const e = route.scaled[idx + 1];
      return (dist(s, a0) <= tol && dist(e, b0) <= tol) ||
             (dist(s, b0) <= tol && dist(e, a0) <= tol);
    });
    if (matched.length < 2) return;
    let sx = 0, sy = 0, ex = 0, ey = 0;
    matched.forEach(({ route, idx }) => {
      const s = route.scaled[idx];
      const e = route.scaled[idx + 1];
      sx += s[0];
      sy += s[1];
      ex += e[0];
      ey += e[1];
    });
    sx /= matched.length; sy /= matched.length;
    ex /= matched.length; ey /= matched.length;
    matched.forEach(({ route, idx }) => {
      route.scaled[idx][0] = sx;
      route.scaled[idx][1] = sy;
      route.scaled[idx + 1][0] = ex;
      route.scaled[idx + 1][1] = ey;
    });
  });
}

function buildPath(points) {
  if (!points.length) return '';
  let d = `M ${points[0][0]} ${points[0][1]}`;
  for (let i = 1; i < points.length; i++) {
    d += ` L ${points[i][0]} ${points[i][1]}`;
  }
  return d;
}

(async () => {
  try {
    const vehRes = await fetch('https://uva.transloc.com/Services/JSONPRelay.svc/GetMapVehiclePoints?APIKey=8882812681&returnVehiclesNotAssignedToRoute=true');
    const vehData = await vehRes.json();
    const activeRouteIds = new Set((vehData || [])
      .filter(v => v.RouteID && v.RouteID > 0 && v.IsOnRoute)
      .map(v => v.RouteID));

    if (!activeRouteIds.size) return;

    const routeRes = await fetch('https://uva.transloc.com/Services/JSONPRelay.svc/GetRoutesForMapWithScheduleWithEncodedLine?APIKey=8882812681');
    const routeData = await routeRes.json();
    const routes = (routeData || [])
      .filter(r => r.EncodedPolyline && activeRouteIds.has(r.RouteID))
      .map(r => ({
        color: r.MapLineColor || r.Color || '#000',
        points: polyline.decode(r.EncodedPolyline)
      }));

    if (!routes.length) return;

    // Compute bounding box
    let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
    routes.forEach(r => {
      r.points.forEach(([lat, lon]) => {
        if (lon < minLon) minLon = lon;
        if (lat < minLat) minLat = lat;
        if (lon > maxLon) maxLon = lon;
        if (lat > maxLat) maxLat = lat;
      });
    });

    const padding = 40;
    const scaleX = (width - padding * 2) / (maxLon - minLon);
    const scaleY = (height - padding * 2) / (maxLat - minLat);
    const scale = Math.min(scaleX, scaleY);

    routes.forEach(r => {
      r.scaled = r.points.map(([lat, lon]) => {
        const x = (lon - minLon) * scale + padding;
        const y = height - ((lat - minLat) * scale + padding);
        return [x, y];
      });
    });

    // Tolerance for grouping segments that share the same roadway.
    // Increased to better align opposite directions separated by a median.
    const KEY_TOL = 12;
    let segMap = groupSegments(routes, KEY_TOL);
    alignSharedSegments(segMap, KEY_TOL);

    routes.forEach(r => {
      let pts = r.scaled;
      pts = simplifyLine(pts, 8);
      pts = smoothPath(pts);
      pts = snap45(pts);
      pts = snapToGrid(pts, GRID_SIZE);
      r.scaled = pts;
    });

    // Re-align and snap shared geometry
    segMap = groupSegments(routes, KEY_TOL);
    alignSharedSegments(segMap, KEY_TOL);
    snapVertices(routes, GRID_SIZE);
    insertSharedVertices(routes, GRID_SIZE / 2);

    // Ensure final geometry uses 45° angles on a shared grid
    routes.forEach(r => {
      r.scaled = snap45(r.scaled);
      r.scaled = snapToGrid(r.scaled, GRID_SIZE);
    });
    segMap = groupSegments(routes, KEY_TOL);
    alignSharedSegments(segMap, KEY_TOL);

    // After aligning, snap again so shared roads follow identical paths
    snapVertices(routes, GRID_SIZE);
    insertSharedVertices(routes, GRID_SIZE / 2);
    routes.forEach(r => {
      r.scaled = snap45(r.scaled);
      r.scaled = snapToGrid(r.scaled, GRID_SIZE);
    });

    // Prepare offset arrays after final alignment and snapping
    routes.forEach(r => {
      r.offsets = Array(r.scaled.length).fill(0).map(() => [0, 0]);
      r.counts = Array(r.scaled.length).fill(0);
    });

    // Detect overlapping segments and offset
    segMap.forEach(entries => {
      if (entries.length < 2) return;
      const r0 = entries[0].route;
      const p1 = r0.scaled[entries[0].idx];
      const p2 = r0.scaled[entries[0].idx + 1];
      const dx = p2[0] - p1[0];
      const dy = p2[1] - p1[1];
      const len = Math.hypot(dx, dy) || 1;
      const nx = -dy / len;
      const ny = dx / len;
      const spacing = STROKE_WIDTH + 2;
      entries.forEach((e, i) => {
        const offset = (i - (entries.length - 1) / 2) * spacing;
        const ox = nx * offset;
        const oy = ny * offset;
        const r = e.route;
        r.offsets[e.idx][0] += ox;
        r.offsets[e.idx][1] += oy;
        r.offsets[e.idx + 1][0] += ox;
        r.offsets[e.idx + 1][1] += oy;
        r.counts[e.idx]++;
        r.counts[e.idx + 1]++;
      });
    });

    // Render paths
    routes.forEach(r => {
      const pts = r.scaled.map((p, i) => {
        if (r.counts[i]) {
          return [p[0] + r.offsets[i][0] / r.counts[i], p[1] + r.offsets[i][1] / r.counts[i]];
        }
        return p;
      });
      const d = buildPath(pts);
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', d);
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', r.color);
      path.setAttribute('stroke-width', STROKE_WIDTH);
      path.setAttribute('stroke-linecap', 'round');
      path.setAttribute('stroke-linejoin', 'round');
      svg.appendChild(path);
    });
  } catch (err) {
    console.error('Error building schematic', err);
  }
})();
