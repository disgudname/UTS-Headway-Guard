import pytest

from ondemand_client import OnDemandClient


@pytest.fixture()
def client() -> OnDemandClient:
    return OnDemandClient(
        login_url="https://example.com/login",
        positions_url="https://example.com/positions",
        user="user",
        passwd="pass",
    )


def test_extract_csrf_token_with_value_after_name(client: OnDemandClient) -> None:
    html = '<input name="csrf_token" value="token123">'
    assert client._extract_csrf_token(html) == "token123"


def test_extract_csrf_token_with_additional_attributes(client: OnDemandClient) -> None:
    html = (
        '<input id="csrf_token" name="csrf_token" type="hidden" '
        'value="Imbd31c88...">'
    )
    assert client._extract_csrf_token(html) == "Imbd31c88..."


def test_extract_csrf_token_missing(client: OnDemandClient) -> None:
    with pytest.raises(RuntimeError):
        client._extract_csrf_token('<input name="nope" value="1">')
