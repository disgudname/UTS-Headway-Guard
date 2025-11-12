import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    PULSEPOINT_FIRST_ON_SCENE,
    PULSEPOINT_FIRST_ON_SCENE_LOCK,
    _update_pulsepoint_first_on_scene,
)


async def _clear_state():
    async with PULSEPOINT_FIRST_ON_SCENE_LOCK:
        PULSEPOINT_FIRST_ON_SCENE.clear()


async def _get_state_copy():
    async with PULSEPOINT_FIRST_ON_SCENE_LOCK:
        return {key: value.copy() for key, value in PULSEPOINT_FIRST_ON_SCENE.items()}


def test_first_on_scene_uses_data_timestamp():
    asyncio.run(_clear_state())
    payload = {
        "incidents": {
            "active": [
                {
                    "ID": "INC1",
                    "Latitude": 38.0,
                    "Longitude": -78.0,
                    "Unit": [
                        {
                            "UnitID": "E1",
                            "Status": "On Scene",
                            "OnSceneDateTime": "2024-01-01T12:00:00Z",
                        }
                    ],
                }
            ]
        }
    }
    asyncio.run(_update_pulsepoint_first_on_scene(payload))
    incident = payload["incidents"]["active"][0]
    expected_ts = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert incident.get("_firstOnSceneTimestamp") == expected_ts
    assert incident.get("_firstOnSceneTimestampSource") == "data"
    state = asyncio.run(_get_state_copy())
    assert state.get("INC1") == {"timestamp": expected_ts, "source": "data"}
    asyncio.run(_clear_state())


def test_first_on_scene_records_observed_when_missing():
    asyncio.run(_clear_state())
    payload = {
        "incidents": {
            "active": [
                {
                    "ID": "INC2",
                    "Latitude": 38.0,
                    "Longitude": -78.0,
                    "Unit": [
                        {
                            "UnitID": "E2",
                            "Status": "On Scene",
                        }
                    ],
                }
            ]
        }
    }
    start = int(time.time() * 1000)
    asyncio.run(_update_pulsepoint_first_on_scene(payload))
    incident = payload["incidents"]["active"][0]
    observed = incident.get("_firstOnSceneTimestamp")
    assert isinstance(observed, int)
    assert abs(observed - start) < 5000
    assert incident.get("_firstOnSceneTimestampSource") == "observed"
    state = asyncio.run(_get_state_copy())
    assert state.get("INC2", {}).get("source") == "observed"
    asyncio.run(_clear_state())


def test_first_on_scene_clears_when_unit_leaves_scene():
    asyncio.run(_clear_state())
    first_payload = {
        "incidents": {
            "active": [
                {
                    "ID": "INC3",
                    "Latitude": 38.0,
                    "Longitude": -78.0,
                    "Unit": [
                        {
                            "UnitID": "E3",
                            "Status": "On Scene",
                        }
                    ],
                }
            ]
        }
    }
    asyncio.run(_update_pulsepoint_first_on_scene(first_payload))
    second_payload = {
        "incidents": {
            "active": [
                {
                    "ID": "INC3",
                    "Latitude": 38.0,
                    "Longitude": -78.0,
                    "Unit": [
                        {
                            "UnitID": "E3",
                            "Status": "Cleared",
                        }
                    ],
                }
            ]
        }
    }
    asyncio.run(_update_pulsepoint_first_on_scene(second_payload))
    incident = second_payload["incidents"]["active"][0]
    assert "_firstOnSceneTimestamp" not in incident
    state = asyncio.run(_get_state_copy())
    assert "INC3" not in state
    asyncio.run(_clear_state())
