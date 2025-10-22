import csv
import io
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app as app_module  # noqa: E402
from tickets_store import TicketStore  # noqa: E402


@pytest.fixture()
def ticket_client(tmp_path, monkeypatch):
    data_path = tmp_path / "tickets.json"
    store = TicketStore(data_path)
    monkeypatch.setattr(app_module, "tickets_store", store, raising=False)
    client = TestClient(app_module.app)
    try:
        yield client, store
    finally:
        client.close()


def test_export_csv_filters_closed_and_soft_hidden(ticket_client):
    client, _ = ticket_client
    open_ticket = client.post(
        "/api/tickets",
        json={
            "fleet_no": "BUS-1",
            "reported_at": "2024-10-10T12:00:00Z",
        },
    ).json()["ticket"]

    closed_ticket = client.post(
        "/api/tickets",
        json={
            "fleet_no": "BUS-2",
            "reported_at": "2024-10-11T08:30:00Z",
        },
    ).json()["ticket"]
    client.put(
        f"/api/tickets/{closed_ticket['id']}",
        json={"completed_at": "2024-10-12T09:00:00Z"},
    )

    hidden_ticket = client.post(
        "/api/tickets",
        json={
            "fleet_no": "BUS-3",
            "reported_at": "2024-10-12T07:45:00Z",
        },
    ).json()["ticket"]
    purge_response = client.post(
        "/api/tickets/purge",
        json={
            "start": "2024-10-12T00:00:00Z",
            "end": "2024-10-12T23:59:59Z",
            "dateField": "reported_at",
            "vehicles": [hidden_ticket["vehicle_label"]],
            "hard": False,
        },
    )
    assert purge_response.status_code == 200

    params = {
        "start": "2024-10-01T00:00:00Z",
        "end": "2024-10-31T23:59:59Z",
        "dateField": "reported_at",
        "includeClosed": "true",
    }
    export_response = client.get("/api/tickets/export.csv", params=params)
    assert export_response.status_code == 200
    assert export_response.headers["content-type"].startswith("text/csv")

    reader = csv.DictReader(io.StringIO(export_response.text))
    rows = list(reader)
    assert [row["vehicle"] for row in rows] == ["BUS-1", "BUS-2"]
    assert rows[0]["ticket_id"] == open_ticket["id"]
    assert rows[1]["ticket_id"] == closed_ticket["id"]

    export_open_only = client.get(
        "/api/tickets/export.csv",
        params={**params, "includeClosed": "false"},
    )
    assert export_open_only.status_code == 200
    reader_open = csv.DictReader(io.StringIO(export_open_only.text))
    open_rows = list(reader_open)
    assert len(open_rows) == 1
    assert open_rows[0]["ticket_id"] == open_ticket["id"]


def test_export_csv_requires_valid_range(ticket_client):
    client, _ = ticket_client
    response = client.get(
        "/api/tickets/export.csv",
        params={"start": "not-a-date", "end": "2024-10-10T00:00:00Z"},
    )
    assert response.status_code == 400
    assert "invalid" in response.json()["detail"].lower()


def test_ticket_history_and_export(ticket_client, monkeypatch):
    client, _ = ticket_client
    monkeypatch.setenv("OPS_PASS", "history-secret")
    app_module._refresh_dispatch_passwords(force=True)
    cookie_value = app_module._dispatcher_cookie_value_for_label("ops")
    assert cookie_value
    client.cookies.set(app_module.DISPATCH_COOKIE_NAME, cookie_value)

    create_payload = {
        "fleet_no": "BUS-99",
        "reported_at": "2024-10-05T08:00:00Z",
        "reported_by": "Unit Test",
        "ops_status": "DOWNED",
        "ops_description": "Air leak discovered",
    }
    create_response = client.post("/api/tickets", json=create_payload)
    assert create_response.status_code == 200
    ticket_id = create_response.json()["ticket"]["id"]

    complete_response = client.put(
        f"/api/tickets/{ticket_id}",
        json={"completed_at": "2024-10-08T12:00:00Z", "shop_status": "UP"},
    )
    assert complete_response.status_code == 200

    reopen_response = client.put(
        f"/api/tickets/{ticket_id}",
        json={"completed_at": None, "ops_status": "LIMITED"},
    )
    assert reopen_response.status_code == 200

    ticket_response = client.get(f"/api/tickets/{ticket_id}")
    assert ticket_response.status_code == 200
    ticket_body = ticket_response.json()["ticket"]
    history_entries = ticket_body.get("history")
    assert isinstance(history_entries, list)
    assert len(history_entries) >= 3

    actions = {entry.get("action"): entry for entry in history_entries}
    assert actions["created"]["actor"] == "ops"
    assert actions["created"]["changes"]["vehicle_label"]["to"] == "BUS-99"
    assert actions["completed"]["changes"]["completed_at"]["to"] == "2024-10-08T12:00:00Z"
    reopened_entry = actions["reopened"]
    assert reopened_entry["changes"]["completed_at"]["to"] is None
    assert reopened_entry["changes"]["ops_status"]["to"] == "LIMITED"

    params = {
        "start": "2024-10-01T00:00:00Z",
        "end": "2024-10-31T23:59:59Z",
        "dateField": "reported_at",
        "includeClosed": "true",
        "includeHistory": "true",
    }
    export_response = client.get("/api/tickets/export.csv", params=params)
    assert export_response.status_code == 200
    reader = csv.DictReader(io.StringIO(export_response.text))
    rows = list(reader)
    assert rows and rows[0].get("history") is not None
    history_raw = next(row["history"] for row in rows if row["ticket_id"] == ticket_id)
    parsed_history = json.loads(history_raw)
    assert any(entry.get("action") == "created" for entry in parsed_history)
