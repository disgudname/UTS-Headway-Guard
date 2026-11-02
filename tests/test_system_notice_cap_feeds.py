import pytest
from fastapi import HTTPException

import app


def _notice(**kw):
    base = {"id": "n1", "message": "Welcome to North Grounds Parking Garage!", "short_message": ""}
    base.update(kw)
    return base


@pytest.fixture
def notices(monkeypatch):
    stored = []
    monkeypatch.setattr(app, "_load_system_notices", lambda: stored)
    return stored


def test_cap_feed_notice_only_on_its_cap_feed(notices):
    notices.append(_notice(target_scope="cap_feeds", target_feed_codes=["NGPG"]))
    assert app._active_notice_texts(["66"], cap_feed_code="NGPG") == [("n1", "Welcome to North Grounds Parking Garage!")]
    assert app._active_notice_texts(["66"], cap_feed_code="ngpg")  # codes are case-insensitive
    assert app._active_notice_texts(["66"], cap_feed_code="JPA") == []
    # RSS, countdown, onboard and legacy ?stopID= CAP pass no code
    assert app._active_notice_texts(["66"]) == []
    # home page, /v1/system-notices, GTFS-RT
    assert app._get_active_system_notices(include_auth_only=True) == []


def test_other_notices_still_reach_cap_feed(notices):
    notices.append(_notice(id="all", target_scope="all"))
    notices.append(_notice(id="other", target_scope="stops", target_stop_ids=["40"]))
    assert [i for i, _ in app._active_notice_texts(["66"], cap_feed_code="NGPG")] == ["all"]


def test_parse_target_fields_cap_feeds():
    scope, stops, routes, codes = app._parse_target_fields(
        {"target_scope": "cap_feeds", "target_feed_codes": "ngpg, jpa", "target_stop_ids": ["40"]})
    assert (scope, stops, routes, codes) == ("cap_feeds", [], [], ["NGPG", "JPA"])
    with pytest.raises(HTTPException):
        app._parse_target_fields({"target_scope": "cap_feeds", "target_feed_codes": []})
    assert app._parse_target_fields({"target_scope": "stops", "target_stop_ids": "40"})[3] == []
