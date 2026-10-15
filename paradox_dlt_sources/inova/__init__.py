"""iNova Payroll (UKG Ready) dlt source — legal entities, cost centers,
employees, compensation history, payrolls and pay statements.

iNova Payroll private-labels UKG Ready ("Inova HCM"); its REST API is the
UKG Ready v2 API documented at https://secure.saashr.com/ta/docs/rest/public/.
See README.md for the research trail and open questions.

Auth: OAuth 2.0 ``client_credentials`` against
``https://{pod}/ta/rest/v2/companies/{cid}/oauth2/token``.

Tenancy: one UKG Ready company (``cid``) spans all operating companies;
each operating company is an EIN (legal entity). Every row carries
``company_id`` (the tenant ``cid``) and ``ein_id`` (legal entity, ``None``
when the API gives no linkage).

Incremental:
- ``employees`` — ``merge``. First run (or a cursor older than 30 days) pulls
  the full ``/employees`` list; later runs call ``/employees/changed?since=``
  with ``cursor.start_value`` (NOT ``last_value``).
- ``payrolls`` / ``pay_statements`` — ``merge`` on ``pay_date`` via
  ``pay_date_from`` / ``pay_date_to`` windows (< 1 year each), starting at
  ``cursor.start_value`` minus ``payroll_lookback_days`` so open payrolls that
  later finalise are re-read.
- ``companies``, ``cost_centers``, ``compensation_history`` — ``replace``; no
  documented change filter.

PII: rows pass through ``strip_pii`` (SSN / national ids, DOB, PIN, bank
details). Bank-account endpoints are never called. Rows are never logged.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dlt

from .helpers import (
    ClientCredentialsAuth,
    Row,
    UkgReadyClient,
    build_base_url,
    date_windows,
    nested_id,
    strip_pii,
)
from .settings import (
    CHANGED_MAX_LOOKBACK_DAYS,
    COST_CENTER_MAX_TREE_INDEX,
    COST_CENTER_TREE_INDEXES,
    ENDPOINT_COMPENSATION_HISTORY,
    ENDPOINT_COST_CENTERS,
    ENDPOINT_EINS,
    ENDPOINT_EMPLOYEES,
    ENDPOINT_EMPLOYEES_CHANGED,
    ENDPOINT_PAY_STATEMENT,
    ENDPOINT_PAY_STATEMENTS,
    ENDPOINT_PAYROLLS,
    PAYROLL_DEFAULT_HISTORY_DAYS,
    PAYROLL_WINDOW_DAYS,
)

_STATEMENT_LINE_TABLES = {
    "earnings": "pay_statement_earnings",
    "deductions": "pay_statement_deductions",
    "taxes": "pay_statement_taxes",
}


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dlt.source(name="inova")
def inova_source(  # noqa: PLR0915
    company_id: str = dlt.config.value,
    pod: str = dlt.config.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    payroll_start_date: str | None = None,
    payroll_lookback_days: int = 31,
    cost_center_tree_indexes: tuple[int, ...] = COST_CENTER_TREE_INDEXES,
    base_url: str | None = None,
) -> list[Any]:
    """iNova (UKG Ready) source factory.

    Args:
        company_id: UKG Ready company ``cid`` (numeric id or ``|short_name``).
        pod: UKG Ready host serving the tenant (e.g. ``secure.saashr.com``).
        client_id: OAuth Machine-to-Machine client id.
        client_secret: OAuth client secret.
        payroll_start_date: ISO date for the first payroll sync. Defaults to
            364 days before today.
        payroll_lookback_days: Days re-read before the payroll cursor on each
            run so status changes (OPEN → FINALIZED) are picked up.
        cost_center_tree_indexes: Cost-center trees (0..8) to load.
        base_url: Test seam — full company base URL; bypasses ``pod``.
    """
    resolved_base = base_url.rstrip("/") if base_url else build_base_url(pod, company_id)
    client = UkgReadyClient(
        resolved_base,
        ClientCredentialsAuth(
            base_url=resolved_base, client_id=client_id, client_secret=client_secret
        ),
    )
    for index in cost_center_tree_indexes:
        if not 0 <= index <= COST_CENTER_MAX_TREE_INDEX:
            raise ValueError(f"cost_center_tree_indexes must be 0..8, got {index}")

    ein_by_name: dict[str, Any] = {}
    ein_by_account: dict[Any, Any] = {}

    def _ein_names() -> dict[str, Any]:
        if not ein_by_name:
            for item in client.get(ENDPOINT_EINS).get("items", []):
                ein_by_name[item.get("ein_name")] = item.get("id")
        return ein_by_name

    def _employee_eins() -> dict[Any, Any]:
        if not ein_by_account:
            names = _ein_names()
            for emp in client.get(ENDPOINT_EMPLOYEES).get("employees", []):
                ein_by_account[emp.get("id")] = names.get(emp.get("ein_name"))
        return ein_by_account

    def _tag(row: Row, ein_id: Any) -> Row:
        out: Row = dict(strip_pii(row))
        out["company_id"] = company_id
        out["ein_id"] = ein_id
        return out

    def _employee_row(emp: Row) -> Row:
        return _tag(emp, _ein_names().get(str(emp.get("ein_name"))))

    @dlt.resource(name="companies", primary_key="id", write_disposition="replace")
    def companies() -> Iterator[Row]:
        for item in client.get(ENDPOINT_EINS).get("items", []):
            yield _tag(item, item.get("id"))

    @dlt.resource(
        name="cost_centers", primary_key=("tree_index", "id"), write_disposition="replace"
    )
    def cost_centers() -> Iterator[Row]:
        for index in cost_center_tree_indexes:
            body = client.get(ENDPOINT_COST_CENTERS, params={"tree_index": index})
            for item in body.get("cost_centers", []):
                row = _tag(item, None)
                row["tree_index"] = index
                yield row

    @dlt.resource(
        name="employees",
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "nullable": True}},
    )
    def employees(
        synced_at: dlt.sources.incremental[str] = dlt.sources.incremental(  # noqa: B008
            "_synced_at", initial_value=None
        ),
    ) -> Iterator[Row]:
        now = datetime.now(UTC)
        # Microsecond stamps keep the cursor strictly increasing between runs;
        # equal cursor values would make dlt de-duplicate re-changed rows.
        stamp = now.isoformat()
        start = synced_at.start_value
        floor = now - timedelta(days=CHANGED_MAX_LOOKBACK_DAYS)
        if start is None or datetime.fromisoformat(start) < floor:
            for emp in client.get(ENDPOINT_EMPLOYEES).get("employees", []):
                row = _employee_row(emp)
                row["_deleted"] = False
                row["_synced_at"] = stamp
                yield row
            return
        body = client.get(
            ENDPOINT_EMPLOYEES_CHANGED, params={"since": _iso_utc(datetime.fromisoformat(start))}
        )
        for entry in body.get("entries", []):
            status = entry.get("status")
            obj = entry.get("object") or {"id": entry.get("id")}
            row = _employee_row(obj)
            row.setdefault("id", entry.get("id"))
            row["_deleted"] = status == "DELETED"
            row["_change_status"] = status
            row["_synced_at"] = stamp
            yield row

    @dlt.resource(name="compensation_history", primary_key="id", write_disposition="replace")
    def compensation_history() -> Iterator[Row]:
        accounts = _employee_eins()
        for item in client.get(ENDPOINT_COMPENSATION_HISTORY).get("compensations", []):
            account_id = (item.get("employee") or {}).get("account_id")
            row = _tag(item, accounts.get(account_id))
            row["account_id"] = account_id
            yield row

    default_start = (date.today() - timedelta(days=PAYROLL_DEFAULT_HISTORY_DAYS)).isoformat()

    @dlt.resource(name="payrolls", primary_key="id", write_disposition="merge")
    def payrolls(
        pay_date: dlt.sources.incremental[str] = dlt.sources.incremental(  # noqa: B008
            "pay_date", initial_value=payroll_start_date or default_start, range_start="closed"
        ),
    ) -> Iterator[Row]:
        start = date.fromisoformat(str(pay_date.start_value)[:10])
        if pay_date.start_value != pay_date.initial_value:
            start -= timedelta(days=payroll_lookback_days)
        for lo, hi in date_windows(start, date.today(), PAYROLL_WINDOW_DAYS):
            body = client.get(
                ENDPOINT_PAYROLLS,
                params={"pay_date_from": lo.isoformat(), "pay_date_to": hi.isoformat()},
            )
            for item in body.get("payrolls", []):
                yield _tag(item, nested_id(item, "ein"))

    @dlt.transformer(
        name="pay_statements",
        data_from=payrolls,
        primary_key="id",
        write_disposition="merge",
    )
    def pay_statements(payroll: Row) -> Iterator[Any]:
        payroll_id = payroll["id"]
        ein_id = payroll.get("ein_id")
        listing = client.get(ENDPOINT_PAY_STATEMENTS.format(payroll_id=payroll_id))
        for summary in listing.get("pay_statements", []):
            detail = client.get(
                ENDPOINT_PAY_STATEMENT.format(payroll_id=payroll_id, statement_id=summary["id"])
            )
            statement = _tag(detail, ein_id)
            statement["payroll_id"] = payroll_id
            account_id = (statement.get("employee") or {}).get("account_id")
            statement["account_id"] = account_id
            for key, table in _STATEMENT_LINE_TABLES.items():
                for line in statement.pop(key, None) or []:
                    line_row = dict(line)
                    line_row.update(
                        company_id=company_id,
                        ein_id=ein_id,
                        payroll_id=payroll_id,
                        pay_statement_id=statement.get("id"),
                        account_id=account_id,
                        pay_date=statement.get("pay_date"),
                    )
                    yield dlt.mark.with_table_name(line_row, table)
            yield statement

    return [companies, cost_centers, employees, compensation_history, payrolls, pay_statements]


__all__ = ["inova_source"]
