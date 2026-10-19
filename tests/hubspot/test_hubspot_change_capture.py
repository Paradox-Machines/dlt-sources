"""Change capture and delete reconciliation for the HubSpot source.

Runs against a local ``filesystem`` destination: production lands HubSpot as
append-only parquet, so tombstone detection must work on versioned rows.
"""

from __future__ import annotations

import secrets
from datetime import timedelta
from pathlib import Path
from typing import Any

import dlt
import pendulum
import pytest
import responses
from dlt.pipeline.exceptions import PipelineStepFailed

from paradox_dlt_sources.hubspot import hubspot_source
from paradox_dlt_sources.hubspot.helpers import ChangeWindow

_BASE = "https://api.hubapi.com"
_DEALS = f"{_BASE}/crm/v3/objects/deals"


def _deal(deal_id: str, updated: str, name: str | None = None) -> dict[str, Any]:
    return {
        "id": deal_id,
        "properties": {"dealname": name or f"Deal {deal_id}", "amount": "100"},
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": updated,
        "archived": False,
    }


def _pipeline(tmp_path: Path, name: str, workdir: str = "w1") -> Any:
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.filesystem(bucket_url=(tmp_path / "lake").as_uri()),
        dataset_name="hubspot",
        pipelines_dir=str(tmp_path / workdir),
    )


def _run(pipeline: Any, deals: list[dict[str, Any]], **kwargs: Any) -> Any:
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, _DEALS, json={"results": deals, "paging": {}})
        source = hubspot_source(api_key="k", base_url=_BASE, **kwargs).with_resources("deals")
        return pipeline.run(source, loader_file_format="jsonl")


def _rows(pipeline: Any) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return client.execute_sql(
            "SELECT id, properties__dealname, _is_deleted, _deleted_at IS NOT NULL "
            "FROM deals ORDER BY _dlt_load_id, id"
        )


def _latest(pipeline: Any) -> dict[str, tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, properties__dealname, coalesce(_is_deleted, false) FROM ("
            "  SELECT *, row_number() OVER (PARTITION BY id ORDER BY _dlt_load_id DESC) rn"
            "  FROM deals) WHERE rn = 1"
        )
    return {r[0]: r for r in rows}


@pytest.fixture
def name() -> str:
    return f"hs_{secrets.token_hex(4)}"


def test_deleted_deal_is_tombstoned_and_stays_deleted(tmp_path: Path, name: str) -> None:
    p = _pipeline(tmp_path, name)
    _run(p, [_deal("1", "2026-01-01T00:00:00Z"), _deal("2", "2026-01-01T00:00:00Z")])
    _run(p, [_deal("1", "2026-01-01T00:00:00Z"), _deal("2", "2026-01-01T00:00:00Z")])
    _run(
        p,
        [
            _deal("1", "2026-01-01T00:00:00Z"),
            _deal("2", "2026-01-01T00:00:00Z"),
            _deal("3", "2026-01-01T00:00:00Z"),
        ],
    )
    # Deal 2 deleted in HubSpot.
    _run(p, [_deal("1", "2026-01-01T00:00:00Z"), _deal("3", "2026-01-01T00:00:00Z")])

    latest = _latest(p)
    assert latest["2"] == ("2", "Deal 2", True)
    assert latest["1"][2] is False
    assert latest["3"][2] is False
    tombstones = [r for r in _rows(p) if r[2]]
    assert tombstones == [("2", "Deal 2", True, True)]

    # Next run: already tombstoned, nothing re-emitted.
    _run(p, [_deal("1", "2026-01-01T00:00:00Z"), _deal("3", "2026-01-01T00:00:00Z")])
    assert [r for r in _rows(p) if r[2]] == tombstones


def test_reconciliation_works_from_a_fresh_working_dir(tmp_path: Path, name: str) -> None:
    """Production pods start with an empty pipelines dir; schema comes from the lake."""
    _run(_pipeline(tmp_path, name, "pod1"), [_deal("1", "2026-01-01T00:00:00Z")])
    _run(
        _pipeline(tmp_path, name, "pod2"),
        [_deal("1", "2026-01-01T00:00:00Z"), _deal("2", "2026-01-01T00:00:00Z")],
    )
    p = _pipeline(tmp_path, name, "pod3")
    _run(p, [_deal("2", "2026-01-01T00:00:00Z")])
    assert _latest(p)["1"][2] is True


def test_truncated_read_refuses_to_tombstone(tmp_path: Path, name: str) -> None:
    p = _pipeline(tmp_path, name)
    deals = [_deal(str(i), "2026-01-01T00:00:00Z") for i in range(4)]
    _run(p, deals)
    _run(p, deals)
    with pytest.raises(PipelineStepFailed, match="max_delete_share"):
        _run(p, deals[:1])
    assert all(not r[2] for r in _latest(p).values())


def test_unchanged_rows_are_not_relanded(tmp_path: Path, name: str) -> None:
    p = _pipeline(tmp_path, name)
    _run(p, [_deal("1", "2020-01-01T00:00:00Z"), _deal("2", "2020-01-01T00:00:00Z")])
    _run(
        p,
        [_deal("1", "2020-01-01T00:00:00Z"), _deal("2", "2020-06-01T00:00:00Z", "Renamed")],
        lookback_seconds=0,
    )
    assert [r[:2] for r in _rows(p)] == [
        ("1", "Deal 1"),
        ("2", "Deal 2"),
        ("2", "Renamed"),
    ]


def test_cursor_never_passes_run_start() -> None:
    """A deal edited after its page was read must be re-read next run."""
    state: dict[str, Any] = {}
    window = ChangeWindow(state, "updatedAt", timedelta(0))
    future = window.started_at.add(minutes=5)
    window.admit("a", future.isoformat())
    window.commit()
    assert pendulum.parse(state["cursor"]) == window.started_at


def test_mid_run_edit_is_recaptured_next_run() -> None:
    state: dict[str, Any] = {"cursor": "2026-01-01T00:00:00+00:00"}
    first = ChangeWindow(state, "updatedAt", timedelta(0))
    # Deal A's page was read early; A is edited a second into the run, then
    # deal B (edited later still) is read on a later page.
    edit_a = first.started_at.add(seconds=1)
    assert first.admit("b", first.started_at.add(seconds=2).isoformat())
    first.commit()

    second = ChangeWindow(state, "updatedAt", timedelta(0))
    assert second.admit("a", edit_a.isoformat()) is True


def test_window_adopts_replaced_incremental_state() -> None:
    state: dict[str, Any] = {
        "incremental": {"updatedAt": {"last_value": "2026-03-01T00:00:00.000Z"}}
    }
    window = ChangeWindow(state, "updatedAt", timedelta(hours=1))
    assert window.admit("old", "2026-02-28T22:00:00Z") is False
    assert window.admit("new", "2026-02-28T23:30:00Z") is True


def test_window_reads_epoch_millis_cursor() -> None:
    state: dict[str, Any] = {"incremental": {"lastUpdated": {"last_value": 1_772_323_200_000}}}
    window = ChangeWindow(state, "lastUpdated", timedelta(0))
    assert window.admit(1, 1_772_323_200_000) is False
    assert window.admit(2, 1_772_323_200_001) is True
    window.commit()
    assert pendulum.parse(state["cursor"]) == pendulum.from_timestamp(1_772_323_200.001)


def test_first_run_lands_everything() -> None:
    window = ChangeWindow({}, "updatedAt", timedelta(hours=1))
    assert window.admit("a", "1999-01-01T00:00:00Z") is True
    assert window.admit("b", None) is True
