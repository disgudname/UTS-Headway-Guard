from ondemand_client import OnDemandClient


def _client_with_urls(vehicles_url: str) -> OnDemandClient:
    return OnDemandClient(
        login_url="https://example.com/login",
        positions_url="https://api.example.com/vehicles/positions",
        user="user",
        passwd="pass",
        vehicles_url=vehicles_url,
    )


def test_adds_last_active_param_when_missing():
    client = _client_with_urls("https://api.example.com/vehicles")
    assert client._vehicles_url == "https://api.example.com/vehicles?show_last_active_at=true"


def test_overrides_last_active_param_value():
    client = _client_with_urls("https://api.example.com/vehicles?show_last_active_at=false")
    assert client._vehicles_url == "https://api.example.com/vehicles?show_last_active_at=true"


def test_preserves_existing_params():
    client = _client_with_urls("https://api.example.com/vehicles?foo=bar")
    assert client._vehicles_url == "https://api.example.com/vehicles?foo=bar&show_last_active_at=true"
