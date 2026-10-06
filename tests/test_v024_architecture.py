"""Regression tests for v0.2.4 architectural changes.

Locks in:
1. 429/503 retry wrapper honors Retry-After and stops after max attempts.
2. Retry wrapper falls through on non-retryable status codes.
3. sempy path detection returns False when sempy is not installed.
4. sempy path extractor returns None gracefully when API not present.
5. Report.skipped_by_reason() groups + counts correctly.
6. Report.coverage() computes score based on inspected item types.
7. HTML render surfaces the coverage KPI + skipped-reason section.
8. Report.to_html() redirects /lakehouse/ paths to /tmp/.
9. discovery.scan_workspace records observed_types + uses sempy when
   available.
10. state.save_state uses atomic replace (temp + rename).
"""

from __future__ import annotations

import os
import types
from urllib.parse import parse_qs, urlparse

import pytest

from pq_adbc_advisor import discovery, fabric_api, report as report_mod, sempy_path, state
from pq_adbc_advisor.constants import RETRY_MAX_ATTEMPTS


# --------------------------------------------------------------------------- #
# 1-2. Retry wrapper behavior
# --------------------------------------------------------------------------- #

class _Resp:
    def __init__(self, status_code, headers=None, body=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body or {}
    def json(self):
        return self._body
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


def test_retry_wrapper_honors_retry_after(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
    calls = [
        _Resp(429, headers={"Retry-After": "3"}),
        _Resp(200, body={"ok": True}),
    ]
    def _request(method, url, headers=None, json=None, timeout=None):
        return calls.pop(0)
    monkeypatch.setattr(fabric_api.requests, "request", _request)

    r = fabric_api._request_with_retry("GET", "https://x")
    assert r is not None and r.status_code == 200
    assert sleeps == [3.0]


def test_retry_wrapper_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    always_429 = _Resp(429, headers={"Retry-After": "0"})
    def _request(*a, **kw):
        return always_429
    monkeypatch.setattr(fabric_api.requests, "request", _request)

    r = fabric_api._request_with_retry("GET", "https://x")
    # Should return the last response so caller can inspect the status,
    # rather than silently swallowing it.
    assert r is always_429


def test_retry_wrapper_passes_through_non_retryable(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    call_count = [0]
    def _request(*a, **kw):
        call_count[0] += 1
        return _Resp(400, body={"error": "bad request"})
    monkeypatch.setattr(fabric_api.requests, "request", _request)

    r = fabric_api._request_with_retry("GET", "https://x")
    assert r is not None and r.status_code == 400
    # 400 must NOT trigger a retry.
    assert call_count[0] == 1


def test_list_items_applies_admin_type_filter_across_pages(monkeypatch):
    urls = []
    pages = [
        _Resp(200, body={"itemEntities": [{"id": "one"}], "continuationToken": "next +/="}),
        _Resp(200, body={"itemEntities": [{"id": "two"}]}),
    ]

    def _request(method, url, **kwargs):
        urls.append(url)
        return pages.pop(0)

    monkeypatch.setattr(fabric_api.requests, "request", _request)

    items = fabric_api.list_items("workspace-id", "token", item_type="Semantic Model")

    assert [item["id"] for item in items] == ["one", "two"]
    first_query = parse_qs(urlparse(urls[0]).query)
    second_query = parse_qs(urlparse(urls[1]).query)
    assert first_query["type"] == ["Semantic Model"]
    assert second_query["type"] == ["Semantic Model"]
    assert second_query["continuationToken"] == ["next +/="]


def test_admin_list_items_identifies_gen1_dataflows_on_seventh_page(monkeypatch):
    urls = []
    pages = []
    page_types = [
        "Dataflow",  # Gen2 dataflow in the Fabric items page
        "Datamart",
        "Report",
        "Dashboard",
        "SemanticModel",
        "App",
        "Dataflow",  # Gen1 dataflows in the seventh page
    ]
    for page, item_type in enumerate(page_types):
        body = {"itemEntities": [{"id": str(page), "type": item_type}]}
        if page < len(page_types) - 1:
            body["continuationToken"] = f"next {page}/+"
        pages.append(_Resp(200, body=body))

    def _request(method, url, **kwargs):
        urls.append(url)
        return pages.pop(0)

    monkeypatch.setattr(fabric_api.requests, "request", _request)

    items = fabric_api.admin_list_items("workspace-id", "token")

    assert [item["id"] for item in items] == [str(page) for page in range(7)]
    assert [item["type"] for item in items] == [
        "Dataflow",
        "Datamart",
        "Report",
        "Dashboard",
        "SemanticModel",
        "App",
        "DataflowGen1",
    ]
    first_query = parse_qs(urlparse(urls[0]).query)
    second_query = parse_qs(urlparse(urls[1]).query)
    assert first_query["workspaceId"] == ["workspace-id"]
    assert second_query["continuationToken"] == ["next 0/+"]


def test_admin_list_items_tracks_groups_across_subpages(monkeypatch):
    urls = []
    responses = [
        ({"id": "fabric-item", "type": "Dataflow"}, "next-group-2"),
        ({"id": "datamart", "type": "Datamart"}, "next-group-3"),
        ({"id": "report", "type": "Report"}, "next-group-4"),
        ({"id": "dashboard-1", "type": "Dashboard"}, "AbCdE%xy"),
        ({"id": "dashboard-2", "type": "Dashboard"}, "next-group-5"),
        ({"id": "semantic-model", "type": "SemanticModel"}, "next-group-6"),
        ({"id": "app", "type": "App"}, "next-group-7"),
        ({"id": "dataflow-1", "type": "Dataflow"}, "Gen1%subpage"),
        ({"id": "dataflow-2", "type": "Dataflow"}, None),
    ]
    pages = [
        _Resp(
            200,
            body={
                "itemEntities": [entity],
                **({"continuationToken": token} if token else {}),
            },
        )
        for entity, token in responses
    ]

    def _request(method, url, **kwargs):
        urls.append(url)
        return pages.pop(0)

    monkeypatch.setattr(fabric_api.requests, "request", _request)

    items = fabric_api.admin_list_items("workspace-id", "token")

    assert [item["type"] for item in items] == [
        "Dataflow",
        "Datamart",
        "Report",
        "Dashboard",
        "Dashboard",
        "SemanticModel",
        "App",
        "DataflowGen1",
        "DataflowGen1",
    ]
    assert parse_qs(urlparse(urls[4]).query)["continuationToken"] == ["AbCdE%xy"]
    assert parse_qs(urlparse(urls[8]).query)["continuationToken"] == ["Gen1%subpage"]


def test_admin_list_items_raises_after_request_retries_fail(monkeypatch):
    monkeypatch.setattr(fabric_api, "_request_with_retry", lambda *args, **kwargs: None)

    with pytest.raises(fabric_api.requests.RequestException, match="failed after retries"):
        fabric_api.admin_list_items("workspace-id", "token")


def test_retry_wrapper_recovers_from_network_error(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    import requests
    calls = [requests.ConnectionError("boom"), _Resp(200, body={})]
    def _request(*a, **kw):
        c = calls.pop(0)
        if isinstance(c, Exception):
            raise c
        return c
    monkeypatch.setattr(fabric_api.requests, "request", _request)

    r = fabric_api._request_with_retry("GET", "https://x")
    assert r is not None and r.status_code == 200


# --------------------------------------------------------------------------- #
# 3-4. Sempy path graceful fallback
# --------------------------------------------------------------------------- #

def test_sempy_available_returns_false_when_missing():
    # In the test venv sempy is not installed.
    assert sempy_path.sempy_available() is False


def test_sempy_extract_returns_none_when_unavailable():
    result = sempy_path.extract_semantic_model_expressions_via_sempy(
        "ws-fake", "ds-fake"
    )
    assert result is None


# --------------------------------------------------------------------------- #
# 5. skipped_by_reason grouping
# --------------------------------------------------------------------------- #

def test_skipped_by_reason_groups_and_counts():
    r = report_mod.ImpactReport(workspace_id="ws")
    r.record_skipped("1", "A", "SemanticModel", reason="no_external_connectors")
    r.record_skipped("2", "B", "SemanticModel", reason="no_external_connectors")
    r.record_skipped("3", "C", "DataPipeline", reason="type_not_inspected")
    r.record_skipped("4", "D", "Report", reason="type_not_inspected")
    by = r.skipped_by_reason()
    assert by["no_external_connectors"] == 2
    assert by["type_not_inspected"] == 2


# --------------------------------------------------------------------------- #
# 6. coverage() score
# --------------------------------------------------------------------------- #

def test_coverage_score_reflects_inspected_types():
    r = report_mod.ImpactReport(workspace_id="ws")
    # DataPipeline is now inspected (v0.3.0). Use Report as the
    # not-inspected type instead.
    r.observed_types = {
        "SemanticModel": 5,
        "Dataflow": 2,
        "Report": 3,
        "Notebook": 10,
    }
    cov = r.coverage()
    # 7 of 20 inspected = 35%
    assert cov["inspected_items"] == 7
    assert cov["total_items"] == 20
    assert cov["score_pct"] == 35
    assert "Report" in cov["not_inspected_types"]
    assert "Notebook" in cov["not_inspected_types"]
    assert "SemanticModel" in cov["inspected_types"]


def test_coverage_score_100_when_only_inspected_types_present():
    r = report_mod.ImpactReport(workspace_id="ws")
    r.observed_types = {"SemanticModel": 3, "Dataflow": 2}
    cov = r.coverage()
    assert cov["score_pct"] == 100
    assert cov["not_inspected_types"] == {}


# --------------------------------------------------------------------------- #
# 7. HTML render surfaces coverage + skipped breakdown
# --------------------------------------------------------------------------- #

def test_html_render_includes_coverage_kpi_and_skipped_breakdown():
    r = report_mod.ImpactReport(workspace_id="ws-abc")
    # 4 inspected + 6 not inspected (Reports).
    r.observed_types = {"SemanticModel": 4, "Report": 6}
    r.record_skipped("1", "A", "Report", reason="type_not_inspected")
    r.record_skipped("2", "B", "Report", reason="type_not_inspected")
    r.record_skipped("3", "C", "SemanticModel", reason="no_external_connectors")
    html = r._repr_html_()
    # Coverage KPI card present
    assert "Coverage" in html
    assert "40%" in html  # 4/10 = 40
    # Skipped-reason section
    assert "Skipped items" in html
    assert "Type not yet inspected" in html
    # Scope disclosure
    assert "Not inspected" in html
    assert "Report" in html


def test_html_render_shows_sempy_badge_when_used():
    r = report_mod.ImpactReport(workspace_id="ws")
    r.used_sempy_path = True
    r.sempy_hits = 7
    html = r._repr_html_()
    assert "sempy fast path" in html
    assert "7 hits" in html


# --------------------------------------------------------------------------- #
# 8. to_html /lakehouse footgun redirect
# --------------------------------------------------------------------------- #

def test_to_html_redirects_lakehouse_path_to_tmp(capsys, tmp_path):
    r = report_mod.ImpactReport(workspace_id="ws")
    result_path = r.to_html("/lakehouse/default/Files/adbc_impact.html")
    assert result_path.startswith("/tmp/")
    assert result_path.endswith("adbc_impact.html")
    assert os.path.exists(result_path)
    # Notice printed to stdout for the customer.
    captured = capsys.readouterr()
    assert "Redirecting write" in captured.out
    os.remove(result_path)


def test_to_html_regular_path_is_not_redirected(tmp_path):
    r = report_mod.ImpactReport(workspace_id="ws")
    target = str(tmp_path / "impact.html")
    result = r.to_html(target)
    assert result == target
    assert os.path.exists(target)


# --------------------------------------------------------------------------- #
# 9. discovery records observed_types
# --------------------------------------------------------------------------- #

def test_discovery_records_observed_types(monkeypatch):
    from pq_adbc_advisor import definitions
    monkeypatch.setattr(fabric_api, "get_token", lambda: "t")
    monkeypatch.setattr(fabric_api, "current_workspace_id", lambda: "ws")
    # Use Report (not DataPipeline) as the type_not_inspected sample —
    # DataPipeline is now inspected in v0.3.0.
    monkeypatch.setattr(fabric_api, "list_items", lambda *a, **kw: [
        {"id": "1", "displayName": "A", "type": "SemanticModel"},
        {"id": "2", "displayName": "B", "type": "Report"},
        {"id": "3", "displayName": "C", "type": "Notebook"},
    ])
    monkeypatch.setattr(fabric_api, "get_item_definition",
                        lambda ws, iid, tok, item_type=None: None)
    monkeypatch.setattr(fabric_api, "get_dataset_gateway",
                        lambda ws, iid, tok: None)
    monkeypatch.setattr(fabric_api, "list_fabric_connections",
                        lambda tok: ([], None))
    monkeypatch.setattr(sempy_path, "sempy_available", lambda: False)

    r = discovery.scan_workspace(
        workspace_id="ws", access_token="t",
        include_fabric_connections=False, telemetry_enabled=False,
        verbose=False,
    )
    assert r.observed_types == {"SemanticModel": 1, "Report": 1, "Notebook": 1}
    # Report and Notebook should show up as skipped with type_not_inspected
    reasons = r.skipped_by_reason()
    assert reasons.get("type_not_inspected", 0) == 2


# --------------------------------------------------------------------------- #
# 10. state.save_state uses atomic replace
# --------------------------------------------------------------------------- #

def test_save_state_atomic(monkeypatch, tmp_path):
    path = tmp_path / "state.json"
    monkeypatch.setattr(state, "_LAKEHOUSE_PATH", str(path))
    ok = state.save_state({"schema_version": 1, "first_run_at": "x"})
    assert ok is True
    assert path.exists()
    # tmp file should not be lingering.
    assert not (tmp_path / "state.json.tmp").exists()


def test_save_state_survives_concurrent_writes(monkeypatch, tmp_path):
    """Two threads writing simultaneously should not corrupt the state file."""
    import json as _json
    import threading
    path = tmp_path / "state.json"
    monkeypatch.setattr(state, "_LAKEHOUSE_PATH", str(path))

    def _write(payload):
        state.save_state(payload)

    threads = [
        threading.Thread(target=_write, args=({"schema_version": 1, "n": i},))
        for i in range(8)
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    # File must be valid JSON at the end (no half-written write).
    with open(path) as f:
        data = _json.load(f)
    assert data["schema_version"] == 1
