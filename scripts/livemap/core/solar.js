// livemap/core/solar.js
// -----------------------------------------------------------------------------
// "Is it night right now?" for the kiosk shells' solar day/night theme.
//
// The legacy testmap pulled in the SunCalc library for this; livemap has no
// build step and one small question to answer, so this is a self-contained
// low-precision solar-position calculation (NOAA / Astronomical Almanac short
// form, good to a fraction of a degree — far tighter than the ~30 min we care
// about). It returns the sun's elevation angle; "civil twilight" is the sun at
// -6 degrees, the same threshold testmap used.
// -----------------------------------------------------------------------------

const DEG = Math.PI / 180;
const CIVIL_TWILIGHT_DEG = -6;

/** Sun elevation above the horizon, in degrees, at a place and instant. */
export function sunElevationDeg(lat, lon, date = new Date()) {
  // Days since the J2000.0 epoch (2000-01-01 12:00 UT), fractional.
  const jd = 2440587.5 + date.getTime() / 86_400_000;
  const n = jd - 2451545.0;

  const meanLon = norm360(280.46 + 0.9856474 * n);
  const meanAnom = norm360(357.528 + 0.9856003 * n) * DEG;
  const eclLon =
    (meanLon + 1.915 * Math.sin(meanAnom) + 0.02 * Math.sin(2 * meanAnom)) * DEG;
  const obliq = (23.439 - 0.0000004 * n) * DEG;

  const rightAsc = Math.atan2(Math.cos(obliq) * Math.sin(eclLon), Math.cos(eclLon));
  const decl = Math.asin(Math.sin(obliq) * Math.sin(eclLon));

  // Greenwich mean sidereal time -> local hour angle of the sun.
  const gmstHours = norm(24, 18.697374558 + 24.06570982441908 * n);
  const lmstDeg = 15 * gmstHours + lon;
  const hourAngle = lmstDeg * DEG - rightAsc;

  const latRad = lat * DEG;
  const elev = Math.asin(
    Math.sin(latRad) * Math.sin(decl) +
      Math.cos(latRad) * Math.cos(decl) * Math.cos(hourAngle),
  );
  return elev / DEG;
}

/** True when the sun is below civil twilight (i.e. render the night basemap). */
export function isDarkNow(lat, lon, date = new Date()) {
  return sunElevationDeg(lat, lon, date) < CIVIL_TWILIGHT_DEG;
}

function norm360(x) {
  return norm(360, x);
}

function norm(period, x) {
  const r = x % period;
  return r < 0 ? r + period : r;
}
