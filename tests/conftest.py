"""Shared pytest fixtures for paradox_dlt_sources tests."""

from __future__ import annotations

import secrets

import dlt
import pytest


@pytest.fixture
def tmp_pipeline(tmp_path):
    """A throwaway dlt pipeline using duckdb destination.

    Uses dev_mode=True and a random suffix so tests can run in parallel
    without state collisions.
    """
    suffix = secrets.token_hex(4)
    return dlt.pipeline(
        pipeline_name=f"test_pipeline_{suffix}",
        destination="duckdb",
        dataset_name=f"test_dataset_{suffix}",
        dev_mode=True,
        pipelines_dir=str(tmp_path),
    )
