import sys
from pathlib import Path

import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _trim_transloc_route


def test_trim_transloc_route_preserves_address_id():
    route_raw = {
        "RouteID": 1,
        "Stops": [
            {
                "StopID": 10,
                "StopName": "Test Stop",
                "Latitude": 1.0,
                "Longitude": 2.0,
                "AddressID": 555,
            }
        ],
    }

    trimmed = _trim_transloc_route(route_raw)

    assert trimmed["Stops"][0]["AddressID"] == 555

