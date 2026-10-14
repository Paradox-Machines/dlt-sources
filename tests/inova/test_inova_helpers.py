"""Unit tests for inova helpers — throttling, auth, PII stripping, windows."""

from __future__ import annotations

from datetime import date

import pytest
import responses

from paradox_dlt_sources.inova.helpers import (
    ClientCredentialsAuth,
    UkgReadyClient,
    UkgReadyError,
    build_base_url,
    date_windows,
    strip_pii,
)

_BASE = "https://pod.example.com/ta/rest/v2/companies/1"


def _client(sleeps: list[float]) -> UkgReadyClient:
    auth = ClientCredentialsAuth(base_url=_BASE, client_id="id", client_secret="s")
    return UkgReadyClient(_BASE, auth, sleep=sleeps.append)


def _token(rsps: responses.RequestsMock) -> None:
    rsps.add(
        responses.POST,
        f"{_BASE}/oauth2/token",
        json={"access_token": "tok", "token_type": "Bearer", "expires_in": 3600},
    )


def test_build_base_url_normalises_pod():
    assert build_base_url("https://secure.saashr.com/", "|IAC") == (
        "https://secure.saashr.com/ta/rest/v2/companies/|IAC"
    )


@responses.activate
def test_get_sends_bearer_and_caches_token():
    _token(responses.mock)
    responses.add(responses.GET, f"{_BASE}/lookup/eins", json={"items": []})
    client = _client([])
    client.get("/lookup/eins")
    client.get("/lookup/eins")
    token_calls = [c for c in responses.calls if c.request.url.endswith("/oauth2/token")]
    assert len(token_calls) == 1
    assert responses.calls[-1].request.headers["Authorization"] == "Bearer tok"


@responses.activate
def test_throttle_503_waits_time_to_wait_header():
    _token(responses.mock)
    responses.add(
        responses.GET,
        f"{_BASE}/employees",
        status=503,
        headers={"X-CallLimit-TimeToWait": "1500"},
    )
    responses.add(responses.GET, f"{_BASE}/employees", status=503)
    responses.add(responses.GET, f"{_BASE}/employees", json={"employees": [{"id": 1}]})
    sleeps: list[float] = []
    assert _client(sleeps).get("/employees") == {"employees": [{"id": 1}]}
    assert sleeps == [1.5, 60.0]


@responses.activate
def test_throttle_gives_up_after_retries():
    _token(responses.mock)
    responses.add(responses.GET, f"{_BASE}/employees", status=503)
    with pytest.raises(UkgReadyError, match="throttled"):
        _client([]).get("/employees")


@responses.activate
def test_errors_do_not_echo_response_body():
    _token(responses.mock)
    responses.add(responses.GET, f"{_BASE}/employees", status=400, body='{"ssn": "123-45-6789"}')
    with pytest.raises(UkgReadyError) as exc:
        _client([]).get("/employees")
    assert "123-45-6789" not in str(exc.value)


@responses.activate
def test_service_limit_413_raises():
    _token(responses.mock)
    responses.add(responses.GET, f"{_BASE}/payrolls", status=413)
    with pytest.raises(UkgReadyError, match="413"):
        _client([]).get("/payrolls")


def test_strip_pii_is_recursive():
    row = {
        "id": 1,
        "social_security": "x",
        "_links": {"self": "u"},
        "nested": {"birthday": "1980-01-01", "keep": 1},
        "list": [{"account_number": "9", "ok": True}],
    }
    assert strip_pii(row) == {"id": 1, "nested": {"keep": 1}, "list": [{"ok": True}]}


def test_date_windows_respect_max_days():
    windows = list(date_windows(date(2024, 1, 1), date(2025, 6, 1), 364))
    assert windows[0] == (date(2024, 1, 1), date(2024, 12, 29))
    assert windows[-1][1] == date(2025, 6, 1)
    assert all((hi - lo).days < 364 for lo, hi in windows)
