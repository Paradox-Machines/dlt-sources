"""Unit tests for agree_com helpers — paginator and schema hints."""

from __future__ import annotations

import pytest
from dlt.sources.helpers.rest_client.paginators import (
    PageNumberPaginator,
)

from paradox_dlt_sources.agree_com.helpers import (
    EPOCH_ISO,
    agree_paginator,
    columns,
    nullable_column,
)

# --- nullable_column() ---


def test_nullable_column_returns_typed_nullable_hint():
    assert nullable_column("text") == {"data_type": "text", "nullable": True}
    assert nullable_column("bigint") == {"data_type": "bigint", "nullable": True}
    assert nullable_column("timestamp") == {"data_type": "timestamp", "nullable": True}


# --- columns() ---


def test_columns_builds_text_hints():
    out = columns(text=("a", "b"))
    assert out == {
        "a": {"data_type": "text", "nullable": True},
        "b": {"data_type": "text", "nullable": True},
    }


def test_columns_builds_bigint_hints():
    out = columns(bigint=("count",))
    assert out == {"count": {"data_type": "bigint", "nullable": True}}


def test_columns_builds_boolean_hints():
    out = columns(boolean=("active",))
    assert out == {"active": {"data_type": "bool", "nullable": True}}


def test_columns_builds_timestamp_hints():
    out = columns(timestamp=("created_at",))
    assert out == {"created_at": {"data_type": "timestamp", "nullable": True}}


def test_columns_builds_decimal_hints():
    out = columns(decimal=("tax_pct",))
    assert out == {"tax_pct": {"data_type": "decimal", "nullable": True}}


def test_columns_mixes_all_types():
    out = columns(text=("name",), bigint=("amount",), boolean=("paid",))
    assert out["name"] == {"data_type": "text", "nullable": True}
    assert out["amount"] == {"data_type": "bigint", "nullable": True}
    assert out["paid"] == {"data_type": "bool", "nullable": True}


def test_columns_empty():
    assert columns() == {}


# --- agree_paginator() ---


def test_agree_paginator_returns_page_number_paginator():
    p = agree_paginator()
    assert isinstance(p, PageNumberPaginator)


@pytest.mark.parametrize("_", range(3))
def test_agree_paginator_creates_fresh_instance(_: int):
    # Each call returns a distinct object so resources don't share state.
    p1 = agree_paginator()
    p2 = agree_paginator()
    assert p1 is not p2


# --- EPOCH_ISO constant ---


def test_epoch_iso_is_unix_epoch():
    assert EPOCH_ISO == "1970-01-01T00:00:00Z"
