"""Thin wrappers around the Fabric / Power BI REST APIs used by the advisor.

Nearly everything here is READ-ONLY.  The one exception is
``trigger_refresh`` which POSTs a refresh request; it is only called from
``validation.py`` and only when the caller opts in (``trigger_refresh=True``
is the default of ``validate_migration``, but the scan phase never calls it).

Auth strategy: reuse the notebook user's token via
``notebookutils.credentials.getToken("pbi")`` when running inside Fabric.
For local runs, callers may pass an ``access_token`` explicitly.
"""

from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import quote, urlencode

import requests

from .constants import (
    DEFAULT_MAX_PARALLEL,
    LRO_FIRST_POLL_SEC,
    RETRY_BACKOFF_BASE_SEC,
    RETRY_BACKOFF_CAP_SEC,
    RETRY_MAX_ATTEMPTS,
    SCANNER_CHUNK_SIZE,
    SCANNER_MAX_POLLS,
    SCANNER_POLL_INTERVAL_SEC,
)


# --------------------------------------------------------------------------- #
# 429 / 503 retry wrapper (v0.2.4)
# --------------------------------------------------------------------------- #
#
# Prior versions silently returned None on 429/503, so a single throttled
# call could silently drop an entire artifact from the report. The wrapper
# below honors Retry-After when present, otherwise applies exponential
# backoff with jitter. Retries are capped to keep worst-case scan time
# bounded even when a tenant is heavily throttled.

_RETRYABLE_STATUSES = {429, 503, 504}


def _sleep_for_retry(response: requests.Response, attempt: int) -> float:
    """Compute the sleep duration for one retry attempt."""
    hint = response.headers.get("Retry-After")
    if hint:
        try:
            return max(0.0, float(hint))
        except ValueError:
            pass
    # Exponential backoff with full jitter, capped.
    delay = min(RETRY_BACKOFF_CAP_SEC, RETRY_BACKOFF_BASE_SEC * (2 ** attempt))
    return random.uniform(0.0, delay)


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: Any = None,
    timeout: float = 60.0,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
) -> requests.Response | None:
    """Issue an HTTP request with 429/503 retry + Retry-After honoring.

    Returns:
        The final Response (which may itself be a 4xx/5xx that the caller
        should interpret), or None if we exhausted retries without ever
        getting a response (e.g. every attempt raised a network error).
    """
    last_response: requests.Response | None = None
    for attempt in range(max_attempts):
        try:
            resp = requests.request(
                method, url, headers=headers, json=json_body, timeout=timeout
            )
        except requests.RequestException:
            # Network error - back off and retry.
            if attempt == max_attempts - 1:
                return None
            time.sleep(min(RETRY_BACKOFF_CAP_SEC, RETRY_BACKOFF_BASE_SEC * (2 ** attempt)))
            continue
        last_response = resp
        if resp.status_code in _RETRYABLE_STATUSES and attempt < max_attempts - 1:
            time.sleep(_sleep_for_retry(resp, attempt))
            continue
        return resp
    return last_response

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_PBI_API = "https://api.powerbi.com/v1.0/myorg"


def get_token() -> str:
    """Return a Power BI access token.

    Precedence (v0.3.0):
      1. Service Principal via ``auth.acquire_token_service_principal``
         when the SP env vars are set.
      2. Delegated notebook user via ``notebookutils.credentials.getToken``.
      3. RuntimeError with clear guidance so the caller supplies a
         token explicitly.
    """
    # 1. Service principal fast path.
    try:
        from . import auth
        if auth.service_principal_env_set():
            return auth.acquire_token_service_principal()
    except Exception:
        # If SP is misconfigured we fall through to the notebook path
        # rather than silently pretend we have no token.
        pass

    # 2. Delegated notebook user.
    try:
        import notebookutils  # type: ignore
        return notebookutils.credentials.getToken("pbi")
    except Exception as e:
        raise RuntimeError(
            "Could not acquire a Power BI token. Options:\n"
            "  * Pass access_token= explicitly to scan_workspace/scan_tenant.\n"
            "  * Set the service-principal env vars: "
            "PQ_ADBC_ADVISOR_SP_TENANT_ID / _SP_CLIENT_ID / _SP_CLIENT_SECRET.\n"
            "  * Run inside a Fabric notebook (notebookutils.credentials.getToken)."
        ) from e


def _auth_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _bearer(access_token: str) -> dict[str, str]:
    """Return a bearer-only Authorization header (no Content-Type)."""
    return {"Authorization": f"Bearer {access_token}"}


def current_workspace_id() -> str:
    """Return the workspace ID the notebook is running in.

    Only works inside a Fabric notebook.
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        return spark.conf.get("trident.workspace.id")
    except Exception as e:
        raise RuntimeError(
            "Could not detect the current workspace ID. "
            "Pass workspace_id= explicitly."
        ) from e


# --------------------------------------------------------------------------- #
# Fabric items API - list items and fetch definitions in a single workspace
# --------------------------------------------------------------------------- #

def list_items(
    workspace_id: str,
    access_token: str,
    item_type: str | None = None
) -> list[dict[str, Any]]:
    """Return every item in a workspace (semantic models, dataflows, pipelines...).

    Handles both continuationUri (older) and continuationToken (newer) paging.
    """
    items: list[dict[str, Any]] = []
    base_url = f"{_FABRIC_API}/workspaces/{workspace_id}/items"
    if item_type:
        base_url += f"?type={item_type}"
    url = base_url
    while url:
        r = _request_with_retry("GET", url, headers=_auth_headers(access_token))
        if r is None:
            break
        r.raise_for_status()
        body = r.json()
        items.extend(body.get("value", []))
        # Fabric APIs use one of these for paging:
        next_url = body.get("continuationUri")
        if not next_url:
            token = body.get("continuationToken")
            if token:
                # Preserve query params (e.g. ?type=SemanticModel) when we go to the next page.
                sep = "&" if "?" in base_url else "?"
                next_url = f"{base_url}{sep}continuationToken={token}"
        url = next_url
    return items

def admin_list_items(
    workspace_id: str,
    access_token: str,
) -> list[dict[str, Any]]:
    """Return admin item entities, identifying Gen1 dataflows in group seven.

    Continuation tokens containing ``%`` indicate another subpage in the same
    item group; tokens without it start the next group.
    """
    items: list[dict[str, Any]] = []
    params = {"workspaceId": workspace_id}
    group_number = 1

    while True:
        url = f"{_FABRIC_API}/admin/items?{urlencode(params)}"
        response = _request_with_retry("GET", url, headers=_auth_headers(access_token))
        if response is None:
            raise requests.RequestException(
                "Fabric Admin List Items request failed after retries."
            )

        response.raise_for_status()
        body = response.json()
        entities = body.get("itemEntities", [])
        if group_number == 7:
            for entity in entities:
                if entity.get("type") == "Dataflow":
                    entity["type"] = "DataflowGen1"
        items.extend(entities)

        continuation_token = body.get("continuationToken")
        if not continuation_token:
            break
        if "%" not in continuation_token:
            group_number += 1
        params["continuationToken"] = continuation_token

    return items


def get_item_definition(
    workspace_id: str,
    item_id: str,
    access_token: str,
    item_type: str | None = None,
) -> dict[str, Any] | None:
    """Fetch the item's definition (base64 parts) via the Fabric REST API.

    Returns None if the item type does not support getDefinition.
    Handles the 202 long-running-operation pattern.

    For SemanticModel we request format=TMSL because our JSON scanner is
    more reliable than the TMDL heuristic.  For other item types we let
    the API pick the default format.
    """
    url = f"{_FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/getDefinition"
    if item_type in ("SemanticModel", "Dataset"):
        url += "?format=TMSL"

    r = _request_with_retry("POST", url, headers=_auth_headers(access_token))
    if r is None:
        return None
    if r.status_code == 400:
        return None
    if r.status_code == 202:
        return _await_lro(r, access_token)
    if r.status_code in (401, 403):
        # v0.3.2 (gap 5): permission-denied on a single item must be
        # distinguishable so the report can tell users "you don't have
        # rights to inspect X" instead of a generic definition_unavailable.
        # Discovery catches this and records skip_reason="permission_denied".
        raise PermissionError(
            f"HTTP {r.status_code} on getDefinition for {workspace_id}/{item_id}"
        )
    if r.status_code == 404:
        # v0.3.3 (bug bash #8): artifact was deleted between enumeration
        # and inspection. Distinguish this from a generic 400/500 so the
        # report shows "deleted during scan" instead of a scary "unavailable".
        raise FileNotFoundError(
            f"HTTP 404 on getDefinition for {workspace_id}/{item_id}"
        )
    if r.status_code >= 400:
        return None
    return r.json()


def _await_lro(initial_response: requests.Response, access_token: str) -> dict[str, Any] | None:
    """Poll a Fabric long-running operation until it completes or times out.

    Optimized backoff (David Coe review, 2026-08-20):

    The Fabric API often returns ``Retry-After: 20`` even when the operation
    completes in under a second. Sleeping 20s per LRO × N artifacts is the
    dominant cost of a workspace scan (81 artifacts × 20s = 27 minutes,
    consistent with David's 23-minute run).

    We start with a short first sleep (``LRO_FIRST_POLL_SEC`` = 1s), then
    exponentially back off up to whatever the server hints via ``Retry-After``.
    In practice this makes fast getDefinition calls return in ~1s while still
    respecting the server's ceiling for genuinely long-running ops.
    """
    location = initial_response.headers.get("Location")
    if not location:
        return None

    server_hint = int(initial_response.headers.get("Retry-After", SCANNER_POLL_INTERVAL_SEC))
    next_sleep = LRO_FIRST_POLL_SEC
    auth = _bearer(access_token)

    for _ in range(SCANNER_MAX_POLLS):
        time.sleep(next_sleep)
        poll = _request_with_retry("GET", location, headers=auth)
        if poll is None or poll.status_code >= 400:
            return None
        server_hint = int(poll.headers.get("Retry-After", server_hint))
        data = poll.json()
        status = data.get("status")
        if status == "Succeeded":
            result_url = poll.headers.get("Location") or location + "/result"
            final = _request_with_retry("GET", result_url, headers=auth)
            if final is not None and final.status_code < 400:
                return final.json()
            return None
        if status in ("Failed", "Cancelled"):
            return None
        # Exponential backoff, capped at the server's hint
        next_sleep = min(next_sleep * 2, server_hint)
    return None


# --------------------------------------------------------------------------- #
# Power BI Scanner API - discover artifacts + connectors across many workspaces
# --------------------------------------------------------------------------- #

def scan_workspaces_modified(access_token: str, exclude_personal: bool = True) -> list[dict]:
    """Return the list of workspaces the scanner API knows about."""
    url = (
        f"{_PBI_API.replace('/v1.0/myorg', '')}/v1.0/myorg/admin/workspaces/modified"
        f"?excludePersonalWorkspaces={str(exclude_personal).lower()}"
    )
    r = _request_with_retry("GET", url, headers=_bearer(access_token))
    if r is None:
        return []
    r.raise_for_status()
    data = r.json()
    return data.get("value", data) if isinstance(data, dict) else data


def _start_scan(access_token: str, workspace_ids: list[str]) -> str:
    """Kick off an admin scan for a chunk of workspace IDs; returns the poll URL."""
    url = (
        f"{_PBI_API}/admin/workspaces/getInfo"
        "?datasetExpressions=True&datasetSchema=True"
        "&datasourceDetails=True&getArtifactUsers=False&lineage=True"
    )
    r = _request_with_retry(
        "POST", url,
        headers=_auth_headers(access_token),
        json_body={"workspaces": workspace_ids},
    )
    if r is None:
        raise RuntimeError("Scanner API refused every retry (network or throttling).")
    r.raise_for_status()
    return r.headers["Location"]


def _poll_scan(access_token: str, poll_url: str) -> str:
    for _ in range(SCANNER_MAX_POLLS):
        time.sleep(SCANNER_POLL_INTERVAL_SEC)
        r = _request_with_retry("GET", poll_url, headers=_bearer(access_token))
        if r is None:
            continue
        r.raise_for_status()
        if r.json().get("status") == "Succeeded":
            return poll_url.replace("/scanStatus/", "/scanResult/")
    raise TimeoutError(f"Scanner API poll timed out for {poll_url}")


def scan_tenant_workspaces(access_token: str, workspace_ids: list[str]) -> dict[str, Any]:
    """Run the admin Scanner API against a subset of workspaces.

    Requires tenant-admin rights (or a service principal in the correct
    security group).  Returns the merged 'workspaces' payload.
    """
    chunks = [
        workspace_ids[i : i + SCANNER_CHUNK_SIZE]
        for i in range(0, len(workspace_ids), SCANNER_CHUNK_SIZE)
    ]
    poll_urls = [_start_scan(access_token, chunk) for chunk in chunks]

    merged: dict[str, Any] = {"workspaces": []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_poll_scan, access_token, url): url for url in poll_urls}
        for fut in as_completed(futures):
            result_url = fut.result()
            r = _request_with_retry("GET", result_url, headers=_bearer(access_token))
            if r is None:
                continue
            r.raise_for_status()
            merged["workspaces"].extend(r.json().get("workspaces", []))
    return merged


# --------------------------------------------------------------------------- #
# Refresh + gateway helpers used by validation.py
# --------------------------------------------------------------------------- #

def get_refresh_history(workspace_id: str, dataset_id: str, access_token: str, top: int = 5) -> list[dict]:
    url = f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
    r = _request_with_retry("GET", url, headers=_bearer(access_token))
    if r is None or r.status_code >= 400:
        return []
    return r.json().get("value", [])


def get_dataflow_refresh_history(
    workspace_id: str, dataflow_id: str, access_token: str, top: int = 5
) -> list[dict]:
    """Return recent refresh entries for a Dataflow Gen2 item.

    v0.3.3 (bug bash #2): the semantic-model refresh endpoint
    (``/datasets/{id}/refreshes``) returns 4xx for Dataflow items, which
    the caller currently swallows silently — a validation pass against a
    DFG2 sees an empty history and reports "no new refresh" forever.

    Fabric Dataflow Gen2 uses the Job Scheduler API instead. We call it
    here and normalize each entry to the same shape validation.py expects
    from datasets: ``{requestId, status, startTime, endTime,
    serviceExceptionJson}``.
    """
    url = (
        f"{_FABRIC_API}/workspaces/{workspace_id}/items/{dataflow_id}"
        f"/jobs/instances?jobType=Refresh"
    )
    r = _request_with_retry("GET", url, headers=_bearer(access_token))
    if r is None or r.status_code >= 400:
        return []
    try:
        raw = r.json().get("value", [])
    except ValueError:
        return []
    normalized: list[dict] = []
    for entry in raw[:top]:
        failure = entry.get("failureReason") or {}
        exc_json = None
        if isinstance(failure, dict):
            exc_json = failure.get("message") or failure.get("errorCode")
        elif isinstance(failure, str):
            exc_json = failure
        normalized.append({
            "requestId": entry.get("id"),
            "status": entry.get("status"),
            "startTime": entry.get("startTimeUtc"),
            "endTime": entry.get("endTimeUtc"),
            "serviceExceptionJson": exc_json,
        })
    return normalized


def get_refresh_history_for_item(
    workspace_id: str,
    item_id: str,
    access_token: str,
    item_type: str,
    top: int = 5,
) -> list[dict]:
    """Route to the correct refresh-history endpoint for the item type.

    Semantic models / datasets use the PBI datasets API; Dataflow Gen2
    items use the Fabric Job Scheduler. Anything else returns [] so
    callers see "no history" instead of a hard error.
    """
    if item_type in ("SemanticModel", "Dataset"):
        return get_refresh_history(workspace_id, item_id, access_token, top=top)
    if item_type == "Dataflow":
        return get_dataflow_refresh_history(workspace_id, item_id, access_token, top=top)
    return []


def get_dataset_gateway(workspace_id: str, dataset_id: str, access_token: str) -> str | tuple[None, str] | None:
    """Return the gatewayId bound to a dataset.

    Returns:
        - A gateway ID string when a gateway is bound
        - None when we successfully saw the datasource list and no gateway is bound
        - The special string "unknown" when we could not read the datasource
          list (401/403/network etc.).  Callers should distinguish this from
          a definite "no gateway" to avoid mis-classifying risk as HIGH.
    """
    url = f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/datasources"
    r = _request_with_retry("GET", url, headers=_bearer(access_token))
    if r is None or r.status_code >= 400:
        return "unknown"
    try:
        values = r.json().get("value", [])
    except ValueError:
        return "unknown"
    for ds in values:
        gw = ds.get("gatewayId")
        if gw:
            return gw
    return None


def trigger_refresh(workspace_id: str, dataset_id: str, access_token: str) -> bool:
    """POST a refresh request. Returns True if accepted."""
    url = f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    r = _request_with_retry(
        "POST", url,
        headers=_auth_headers(access_token),
        json_body={"notifyOption": "NoNotification"},
    )
    return r is not None and r.status_code in (200, 202)


# --------------------------------------------------------------------------- #
# Fabric Connections (first-class shared connections in the Fabric portal)
# --------------------------------------------------------------------------- #

def list_fabric_connections(access_token: str) -> tuple[list[dict[str, Any]], str | None]:
    """Enumerate the caller's Fabric Connections via the Fabric REST API.

    Returns a tuple ``(connections, error)``:
      * ``connections`` — the list of visible connections (possibly empty).
      * ``error`` — None on success, or a short string describing why the
        API was unreachable ("http 403", "http 401", "network", ...).

    Some tenants block this API for non-admins; callers should surface the
    error string so an "empty" list isn't confused with "no connections".
    """
    connections: list[dict[str, Any]] = []
    url = f"{_FABRIC_API}/connections"
    while url:
        r = _request_with_retry("GET", url, headers=_auth_headers(access_token))
        if r is None:
            return connections, "network: exhausted retries"
        if r.status_code >= 400:
            return connections, f"http {r.status_code}"
        try:
            body = r.json()
        except ValueError:
            return connections, "non-json response"
        connections.extend(body.get("value", []))
        url = body.get("continuationUri")
    return connections, None


def list_data_pipelines(workspace_id: str, access_token: str) -> list[dict[str, Any]]:
    """List Fabric Data Pipeline items in a workspace."""
    url = f"{_FABRIC_API}/workspaces/{workspace_id}/dataPipelines"
    r = _request_with_retry("GET", url, headers=_auth_headers(access_token))
    if r is None or r.status_code >= 400:
        return []
    return r.json().get("value", [])


def get_dataset_refresh_error(
    workspace_id: str, dataset_id: str, access_token: str, request_id: str | None = None
) -> str | None:
    """Return the raw error message for the most recent (or specified) refresh.

    The refresh history endpoint returns 'serviceExceptionJson' for failed
    refreshes; we surface that string so troubleshoot.diagnose can classify
    it.
    """
    history = get_refresh_history(workspace_id, dataset_id, access_token, top=5)
    if not history:
        return None
    target = history[0]
    if request_id:
        for entry in history:
            if entry.get("requestId") == request_id:
                target = entry
                break
    exc = target.get("serviceExceptionJson")
    if exc:
        return exc
    if target.get("status") == "Failed":
        return f"Refresh failed at {target.get('endTime')} (no detail returned)"
    return None
