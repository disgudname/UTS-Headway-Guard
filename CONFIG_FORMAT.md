# DUCK System Configuration File Format

This document describes the JSON configuration file format for the DUCK (Driver Underpass Clearance Keeper) system. The config file must be placed at `/duck/config.json` on the SD card.

## Overview

The DUCK system is an in-cab warning device for electric buses that alerts drivers when approaching low-clearance underpasses. It uses GPS geofencing to detect hazard zones and provides audio/visual warnings.

## File Location

The configuration file must be located at:
```
/duck/config.json
```

## JSON Structure

```json
{
  "volume": 80,
  "thresholds": {
    "moving_speed_mph": 3.0,
    "gps_debounce_ms": 1000,
    "power_debounce_ms": 500,
    "shutdown_timeout_sec": 60,
    "config_timeout_sec": 900
  },
  "yard": {
    "polygon": [
      [-122.4194, 37.7749],
      [-122.4195, 37.7750],
      [-122.4193, 37.7751],
      [-122.4192, 37.7748]
    ]
  },
  "hazards": [
    {
      "name": "Example Underpass",
      "enter_polygon": [
        [-122.4100, 37.7800],
        [-122.4101, 37.7802],
        [-122.4099, 37.7803],
        [-122.4098, 37.7801]
      ],
      "exit_polygon": [
        [-122.4095, 37.7805],
        [-122.4096, 37.7807],
        [-122.4094, 37.7808],
        [-122.4093, 37.7806]
      ],
      "voice_prompt": "/duck/audio/hazards/example_underpass.mp3"
    }
  ],
  "audio": {
    "alarm": "/duck/audio/alarm.mp3",
    "boot_music": "/duck/audio/boot_music.mp3",
    "boot_complete": "/duck/audio/boot_complete.mp3",
    "gps_fix_acquired": "/duck/audio/gps_fix.mp3",
    "shutdown_safe": "/duck/audio/safe_to_power_down.mp3"
  },
  "wifi": {
    "ssid": "DUCK-CONFIG",
    "password": ""
  }
}
```

## Field Reference

### volume (optional)
- **Type:** Integer
- **Range:** 0-100
- **Default:** 80
- **Description:** Audio output volume level

### thresholds (optional)
All threshold fields have defaults and are optional.

| Field | Type | Range | Default | Description |
|-------|------|-------|---------|-------------|
| `moving_speed_mph` | Float | 0.5-15.0 | 3.0 | Speed (mph) above which vehicle is considered "moving" |
| `gps_debounce_ms` | Integer | - | 1000 | GPS sample stability time in milliseconds |
| `power_debounce_ms` | Integer | - | 500 | Power loss confirmation time in milliseconds |
| `shutdown_timeout_sec` | Integer | 10-300 | 60 | Countdown duration when power is lost (seconds) |
| `config_timeout_sec` | Integer | - | 900 | Config mode timeout (seconds, default 15 min) |

### yard (required)
The yard geofence defines the area where config mode can be entered (typically the bus depot).

| Field | Type | Description |
|-------|------|-------------|
| `polygon` | Array | Array of coordinate pairs defining the yard boundary |

### hazards (required)
Array of hazard zone definitions. **At least one hazard is required.**

| Field | Type | Max Length | Description |
|-------|------|------------|-------------|
| `name` | String | 32 chars | Human-readable name for the hazard |
| `enter_polygon` | Array | 16 points | Polygon defining the hazard entry zone |
| `exit_polygon` | Array | 16 points | Polygon defining the safe exit zone |
| `voice_prompt` | String | 64 chars | Path to audio file played when stopped in hazard zone |

### audio (optional)
Custom paths for audio files. All paths are optional and have defaults.

| Field | Default | Description |
|-------|---------|-------------|
| `alarm` | `/duck/audio/alarm.mp3` | Alarm sound when moving through hazard |
| `boot_music` | `/duck/audio/boot_music.mp3` | Music played during boot sequence |
| `boot_complete` | `/duck/audio/boot_complete.mp3` | Sound when boot completes |
| `gps_fix_acquired` | `/duck/audio/gps_fix.mp3` | Sound when GPS fix is acquired |
| `shutdown_safe` | `/duck/audio/safe_to_power_down.mp3` | "Safe to power down" announcement |

### wifi (optional)
WiFi access point settings for config mode.

| Field | Type | Max Length | Default | Description |
|-------|------|------------|---------|-------------|
| `ssid` | String | 32 chars | `DUCK-CONFIG` | WiFi network name |
| `password` | String | 64 chars | `""` (empty/open) | WiFi password |

## Polygon Coordinate Format

**CRITICAL: Coordinates are specified as [longitude, latitude], NOT [latitude, longitude].**

```json
"polygon": [
  [longitude1, latitude1],
  [longitude2, latitude2],
  [longitude3, latitude3]
]
```

Example (San Francisco area):
```json
"polygon": [
  [-122.4194, 37.7749],
  [-122.4195, 37.7750],
  [-122.4193, 37.7751]
]
```

### Polygon Rules
- Minimum 3 points required (to form a valid polygon)
- Maximum 16 points per polygon
- Points are automatically connected in order
- The polygon is automatically closed (last point connects to first)
- Points should be specified in either clockwise or counter-clockwise order

## Validation Rules

The following conditions cause config validation to fail:

1. **Yard polygon missing or invalid** - Must have at least 3 points
2. **No hazards defined** - At least one hazard is required
3. **moving_speed_mph out of range** - Must be 0.5-15.0 mph
4. **shutdown_timeout_sec out of range** - Must be 10-300 seconds
5. **volume out of range** - Must be 0-100
6. **Invalid hazard polygons** - Each hazard needs valid enter and exit polygons (3+ points each)

## System Limits

| Limit | Value |
|-------|-------|
| Maximum hazards | 32 |
| Maximum polygon points | 16 per polygon |
| Maximum hazard name length | 32 characters |
| Maximum audio path length | 64 characters |
| Maximum SSID length | 32 characters |
| Maximum password length | 64 characters |

## Hazard Zone Behavior

The system uses two polygons per hazard:

1. **enter_polygon** - When the bus enters this zone, the alarm activates if moving
2. **exit_polygon** - The bus must reach this zone to confirm safe passage

When stopped inside the enter_polygon, the system plays the `voice_prompt` audio file instead of the alarm, providing specific guidance for that location.

## Example: Complete Configuration

```json
{
  "volume": 85,
  "thresholds": {
    "moving_speed_mph": 2.5,
    "shutdown_timeout_sec": 45
  },
  "yard": {
    "polygon": [
      [-117.1611, 32.7157],
      [-117.1615, 32.7160],
      [-117.1620, 32.7158],
      [-117.1618, 32.7154],
      [-117.1613, 32.7153]
    ]
  },
  "hazards": [
    {
      "name": "Main St Underpass",
      "enter_polygon": [
        [-117.1550, 32.7200],
        [-117.1552, 32.7205],
        [-117.1548, 32.7206],
        [-117.1546, 32.7201]
      ],
      "exit_polygon": [
        [-117.1540, 32.7210],
        [-117.1542, 32.7215],
        [-117.1538, 32.7216],
        [-117.1536, 32.7211]
      ],
      "voice_prompt": "/duck/audio/hazards/main_st.mp3"
    },
    {
      "name": "Railroad Bridge",
      "enter_polygon": [
        [-117.1480, 32.7300],
        [-117.1482, 32.7305],
        [-117.1478, 32.7306],
        [-117.1476, 32.7301]
      ],
      "exit_polygon": [
        [-117.1470, 32.7310],
        [-117.1472, 32.7315],
        [-117.1468, 32.7316],
        [-117.1466, 32.7311]
      ],
      "voice_prompt": "/duck/audio/hazards/railroad.mp3"
    }
  ],
  "wifi": {
    "ssid": "BUS-CONFIG-001",
    "password": "securepass123"
  }
}
```

## Tips for Config Generator

When building a UI to generate this config:

1. **Map Interface** - Use a map UI for drawing polygons. Most mapping libraries use [longitude, latitude] order natively.

2. **Coordinate Precision** - GPS coordinates should have at least 6 decimal places for sufficient precision (approximately 0.1 meter accuracy).

3. **Polygon Drawing Order** - Ensure users draw polygons in a consistent direction (clockwise or counter-clockwise).

4. **Validation Feedback** - Validate on the client side before download:
   - Check all required fields are present
   - Verify polygon point counts (3-16)
   - Validate numeric ranges
   - Ensure at least one hazard exists

5. **Default Values** - Pre-populate optional fields with their defaults so users can see what values are available.

6. **Audio Path Consistency** - If allowing custom audio paths, ensure they follow the `/duck/audio/...` convention.
