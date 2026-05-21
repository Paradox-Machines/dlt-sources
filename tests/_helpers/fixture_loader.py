"""JSON-fixture → `responses` mock registration."""

from __future__ import annotations

import json
from pathlib import Path

import responses


def load_fixture(source: str, name: str) -> dict:
    """Load a fixture JSON file for a given source.

    Args:
        source: source name (e.g. "attio")
        name: fixture filename without `.json` (e.g. "companies_page_1")
    """
    path = Path(__file__).parent.parent / source / "fixtures" / f"{name}.json"
    with path.open() as f:
        return json.load(f)


def register_post_sequence(
    rsps: responses.RequestsMock,
    url: str,
    fixtures: list[dict],
) -> None:
    """Register a sequence of POST responses for the same URL (one per call)."""
    for body in fixtures:
        rsps.add(responses.POST, url, json=body, status=200)


def register_get(
    rsps: responses.RequestsMock,
    url: str,
    fixture: dict,
) -> None:
    """Register a single GET response."""
    rsps.add(responses.GET, url, json=fixture, status=200)
