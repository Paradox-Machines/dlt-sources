"""Integration tests for the monday_crm_source factory.

Mocks vendor HTTP endpoints with ``responses`` (NOT pytest-httpx) and asserts
the source materialises the expected resources via the ``tmp_pipeline`` conftest
fixture (full source -> duckdb).

TEMPLATE-OWNED IMPORTS: this header owns every import (``from __future__`` first).
``source_test_body`` below is LOGIC ONLY — test functions that reference the
already-imported ``json`` / ``Path`` / ``pytest`` / ``responses`` / the
``monday_crm_source`` (and any declared paginator classes) and the ``_fixture``
helper defined here, NEVER a second import block (that block was the live E402
failure site).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import responses

from paradox_dlt_sources.monday_crm import monday_crm_source


def _fixture(name: str) -> dict:
    stem = name[: -len(".json")] if name.endswith(".json") else name
    path = Path(__file__).parent / "fixtures" / f"{stem}.json"
    with path.open() as f:
        return json.load(f)


def register_json_response(method: str, url: str, fixture_name: str) -> None:
    # Register a REST JSON mock the CORRECT way: responses needs json=<dict>
    # (it serializes for you). _fixture() already returns a parsed dict, so a
    # body=<dict> would make dlt raise "a bytes-like object is required, not
    # 'dict'". Multi-page = call this once per page (responses preserves FIFO
    # order across repeated add() calls for the same URL).
    responses.add(method, url, json=_fixture(fixture_name))


def register_graphql_response(url: str, fixtures_by_resource: dict[str, str]) -> None:
    # ALL GraphQL resources share ONE POST endpoint, so the resource is selected
    # by the query in the request BODY (body-routing) — exactly what the harness
    # probe does. The callback body MUST be a JSON STRING via json.dumps
    # (responses passes a callback body through verbatim; a dict body makes dlt
    # raise "a bytes-like object is required, not 'dict'"). Most-specific
    # (longest) resource name first so a substring never mis-routes.
    ordered = sorted(fixtures_by_resource, key=len, reverse=True)

    def _route(
        request: responses.PreparedRequest,
    ) -> tuple[int, dict[str, str], str]:
        body = request.body or ""
        if isinstance(body, bytes):
            body = body.decode("utf-8", "replace")
        headers = {"Content-Type": "application/json"}
        for resource in ordered:
            if resource.lower() in str(body).lower():
                return (
                    200,
                    headers,
                    json.dumps(_fixture(fixtures_by_resource[resource])),
                )
        return 200, headers, json.dumps({})

    responses.add_callback(responses.POST, url, callback=_route)


_BASE = "https://api.monday.com/v2"
_HOST = re.compile(rf"^{re.escape(_BASE)}(?:/.*)?$")
_EXPECTED_RESOURCES = {
    "boards",
    "items",
    "users",
    "teams",
    "tags",
    "updates",
    "workspaces",
    "columns",
    "groups",
}
_RESOURCE_FLOOR = {
    "boards",
    "items",
    "users",
    "teams",
    "tags",
    "updates",
    "workspaces",
    "columns",
    "groups",
}
_DATA_SELECTORS = {
    "boards": "data.boards",
    "items": "data.boards[*].items_page.items",
    "users": "data.users",
    "teams": "data.teams",
    "tags": "data.tags",
    "updates": "data.updates",
    "workspaces": "data.workspaces",
    "columns": "data.boards",
    "groups": "data.boards",
}
_GLOBAL_GRAPHQL = True


_PAGE_KEYS = {"page", "page_number", "pageno", "pagenum", "pagenumber"}
_OFFSET_KEYS = {"offset", "skip", "start"}
_CURSOR_KEYS = {
    "after",
    "cursor",
    "next",
    "next_cursor",
    "nextcursor",
    "page_token",
    "pagetoken",
    "start_cursor",
    "startcursor",
    "starting_after",
}


def _is_continuation(body_text: str) -> bool:
    if not body_text:
        return False
    try:
        parsed = json.loads(body_text)
    except (json.JSONDecodeError, TypeError):
        return False
    _scopes: list[dict] = []
    if isinstance(parsed, dict):
        _scopes.append(parsed)
        _vars = parsed.get("variables")
        if isinstance(_vars, dict):
            _scopes.append(_vars)
    for _scope in _scopes:
        for _raw, _val in _scope.items():
            _key = str(_raw).lower()
            if isinstance(_val, bool):
                continue
            if _key in _PAGE_KEYS and isinstance(_val, (int, float)) and _val > 1:
                return True
            if _key in _OFFSET_KEYS and isinstance(_val, (int, float)) and _val > 0:
                return True
            if _key in _CURSOR_KEYS and isinstance(_val, str) and _val:
                return True
    return False


def _global_serve(_body: str, _superset: dict, _req_count: dict) -> tuple:
    _guard_req_count(_req_count, "<global>")
    _headers = {"Content-Type": "application/json"}
    if _is_continuation(_body):
        return (200, _headers, json.dumps({}))
    return (200, _headers, json.dumps(_superset))


def _first_wildcard_prefix(_sel: str) -> str | None:
    if not _sel:
        return None
    _acc: list[str] = []
    for _part in _sel.split("."):
        _acc.append(_part)
        if _part.endswith("[*]"):
            return ".".join(_acc)
    return None


def _family_root(_prefix: str) -> str:
    _parts = _prefix.split(".")
    _parts[-1] = _parts[-1][:-3] if _parts[-1].endswith("[*]") else _parts[-1]
    return ".".join(_parts)


def _elem_key(_index: int, _el: object) -> object:
    if isinstance(_el, dict) and "id" in _el:
        return _el["id"]
    return _index


def _deep_union(_a: object, _b: object) -> object:
    if isinstance(_a, dict) and isinstance(_b, dict):
        _out: dict = dict(_a)
        for _k, _v in _b.items():
            _out[_k] = _deep_union(_a[_k], _v) if _k in _a else _v
        return _out
    if isinstance(_a, list) and isinstance(_b, list):
        _merged: dict = {}
        _order: list = []
        for _i, _el in enumerate(_a):
            _key = _elem_key(_i, _el)
            if _key not in _merged:
                _order.append(_key)
            _merged[_key] = _el
        for _i, _el in enumerate(_b):
            _key = _elem_key(_i, _el)
            if _key in _merged:
                _merged[_key] = _deep_union(_merged[_key], _el)
            else:
                _merged[_key] = _el
                _order.append(_key)
        return [_merged[_k] for _k in _order]
    return _b


def _fold_idless_shells(_node: object) -> object:
    if isinstance(_node, dict):
        return {_k: _fold_idless_shells(_v) for _k, _v in _node.items()}
    if isinstance(_node, list):
        _folded = [_fold_idless_shells(_el) for _el in _node]
        _idd = [_el for _el in _folded if isinstance(_el, dict) and "id" in _el]
        if not _idd:
            return _folded
        _shells = [_el for _el in _folded if isinstance(_el, dict) and "id" not in _el]
        if not _shells:
            return _folded
        _target = _idd[0]
        for _shell in _shells:
            _target = _deep_union(_target, _shell)
        _out: list = []
        for _el in _folded:
            if isinstance(_el, dict) and "id" not in _el:
                continue
            _out.append(_target if _el is _idd[0] else _el)
        return _out
    return _node


def _build_global_superset(_pages_by_resource: dict[str, list[dict]]) -> dict:
    _superset: object = {}
    for _pages in _pages_by_resource.values():
        for _page in _pages:
            _superset = _deep_union(_superset, _page)
    _superset = _fold_idless_shells(_superset)
    return _superset if isinstance(_superset, dict) else {}


def _compute_family_state(
    _decl: list[str], _pages_by_resource: dict[str, list[dict]]
) -> tuple[dict[str, str], dict[str, dict]]:
    _by_prefix: dict[str, list[str]] = {}
    for _r in _decl:
        _pfx = _first_wildcard_prefix(_DATA_SELECTORS.get(_r, ""))
        if _pfx is None:
            continue
        _by_prefix.setdefault(_pfx, []).append(_r)
    _membership: dict[str, str] = {}
    _state: dict[str, dict] = {}
    for _pfx, _members in _by_prefix.items():
        if len(_members) < 2:
            continue
        _root = _family_root(_pfx)
        _full = [_r for _r in _decl if _r in _members or _DATA_SELECTORS.get(_r, "") == _root]
        _superset: object = {}
        for _member in _full:
            for _page in _pages_by_resource.get(_member) or []:
                _superset = _deep_union(_superset, _page)
        if not _superset:
            continue
        _state[_pfx] = {"superset": _superset, "budget": len(_full), "served": 0}
        for _member in _full:
            _membership[_member] = _pfx
    return _membership, _state


def _empty_envelope(_resource: str) -> dict:
    _sel = _DATA_SELECTORS.get(_resource) or _resource
    _payload: object = []
    for _key in reversed(_sel.split(".")):
        _payload = {_key: _payload}
    return _payload  # type: ignore[return-value]


def _guard_req_count(_counts: dict, _key: str, _cap: int = 500) -> None:
    # Bound the mock requests PER resource so a non-terminating paginator
    # fails FAST + named (which resource looped) instead of hanging the
    # gate's pytest to its job timeout (the live monday iter18 1200s wedge).
    _counts[_key] = _counts.get(_key, 0) + 1
    if _counts[_key] > _cap:
        raise RuntimeError(
            f"runaway pagination: resource {_key!r} exceeded {_cap} mock "
            "requests — its paginator's has_next_page never became False "
            "(non-terminating)."
        )


def _install_fixture_routes() -> None:
    def _leaf(_sel: str) -> str:
        return _sel.rsplit(".", 1)[-1] if _sel else ""

    def _tokens(_resource: str) -> list[str]:
        _out: list[str] = []
        for _tok in (_leaf(_DATA_SELECTORS.get(_resource, "")), _resource):
            if _tok and _tok not in _out:
                _out.append(_tok)
        return _out

    def _token_in(_tok: str, _hay: str) -> bool:
        _pat = rf"(?<![A-Za-z0-9_]){re.escape(_tok.lower())}(?![A-Za-z0-9_])"
        return re.search(_pat, _hay.lower()) is not None

    _decl = list(_EXPECTED_RESOURCES)

    def _match(_hay: str) -> str | None:
        _best: tuple[int, int, str] | None = None
        for _i, _resource in enumerate(_decl):
            for _tok in _tokens(_resource):
                if _token_in(_tok, _hay):
                    _cand = (-len(_tok), _i, _resource)
                    if _best is None or _cand < _best:
                        _best = _cand
                    break
        return _best[2] if _best is not None else None

    def _bucket_token(_r: str) -> str:
        return _leaf(_DATA_SELECTORS.get(_r, _r) or _r)

    _fixtures_dir = Path(__file__).parent / "fixtures"
    _ordered = sorted(_decl, key=lambda r: len(_bucket_token(r)), reverse=True)
    _pages_by_resource: dict[str, list[dict]] = {r: [] for r in _decl}
    for _path in sorted(_fixtures_dir.glob("*.json")):
        _stem = _path.stem
        _matched = next(
            (
                r
                for r in _ordered
                for _t in (_bucket_token(r), r)
                if _stem == _t or _stem.startswith(f"{_t}_")
            ),
            None,
        )
        if _matched is not None:
            _pages_by_resource[_matched].append(_fixture(_stem))
    _cursor: dict[str, int] = {r: 0 for r in _decl}
    _family_membership, _family_state = _compute_family_state(_decl, _pages_by_resource)
    _global_superset = _build_global_superset(_pages_by_resource) if _GLOBAL_GRAPHQL else {}
    _req_count: dict[str, int] = {}

    def _route(request: responses.PreparedRequest) -> tuple:
        body = request.body or ""
        if isinstance(body, bytes):
            body = body.decode("utf-8", "replace")
        path = str(getattr(request, "path_url", "") or "")
        haystack = str(body) + " " + path
        headers = {"Content-Type": "application/json"}
        if _GLOBAL_GRAPHQL:
            return _global_serve(str(body), _global_superset, _req_count)
        resource = _match(haystack)
        _guard_req_count(_req_count, resource or "<unmatched>")
        if resource is None:
            return (200, headers, json.dumps({}))
        _fkey = _family_membership.get(resource)
        if _fkey is not None:
            _fam = _family_state[_fkey]
            if _fam["served"] < _fam["budget"]:
                _fam["served"] += 1
                return (200, headers, json.dumps(_fam["superset"]))
            return (200, headers, json.dumps(_empty_envelope(resource)))
        pages = _pages_by_resource.get(resource) or []
        idx = _cursor[resource]
        _cursor[resource] = idx + 1
        if idx < len(pages):
            return (200, headers, json.dumps(pages[idx]))
        return (200, headers, json.dumps(_empty_envelope(resource)))

    responses.add_callback(responses.GET, _HOST, callback=_route)
    responses.add_callback(responses.POST, _HOST, callback=_route)


@responses.activate
def test_monday_crm_source_runs_against_duckdb(tmp_pipeline) -> None:
    _install_fixture_routes()

    info = tmp_pipeline.run(monday_crm_source(api_key="monday_crm_dummy", base_url=_BASE))
    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    realized = {name.lower().replace("-", "_") for name in table_names}
    assert realized >= _RESOURCE_FLOOR


def test_returns_expected_resources() -> None:
    src = monday_crm_source(api_key="monday_crm_dummy")
    assert {r.name for r in src.resources.values()} == _EXPECTED_RESOURCES


def test_resource_dispositions() -> None:
    src = monday_crm_source(api_key="monday_crm_dummy")
    assert src.resources["boards"].write_disposition == "replace"
    assert src.resources["items"].write_disposition == "merge"
    assert src.resources["users"].write_disposition == "replace"
    assert src.resources["teams"].write_disposition == "replace"
    assert src.resources["tags"].write_disposition == "replace"
    assert src.resources["updates"].write_disposition == "replace"
    assert src.resources["workspaces"].write_disposition == "replace"
    assert src.resources["columns"].write_disposition == "replace"
    assert src.resources["groups"].write_disposition == "replace"
