"""Extract M expressions from a Fabric item definition payload.

Fabric's ``getDefinition`` returns a JSON envelope with base64-encoded parts.
Different item types package their M code differently:

  - Semantic Model (TMSL / TMDL) - M expressions live in table partitions
    and in a shared 'expressions' collection.
  - Dataflow Gen 2 - mashup.pq holds an M script.
  - Dataflow Gen 1 - model.json 'pbi:mashup' contains a base64-encoded
    zip whose Formulas/Section1.m file holds the M script. We do NOT
    fully unpack that zip here; we only scan any embedded strings that
    already look like M inside model.json.

Coverage notes we surface to callers:
  * Gen 1 dataflows are BEST EFFORT - if the mashup is not exposed as
    text in model.json we will report the item as skipped rather than
    give a false all-clear.
"""

from __future__ import annotations

import base64
import json
from typing import Iterable


def _b64_decode(payload: str) -> str:
    try:
        return base64.b64decode(payload).decode("utf-8", errors="replace")
    except Exception:
        return ""


def extract_m_expressions(definition: dict, item_type: str) -> list[dict]:
    """Return a list of {name, expression} for every M expression in the item.

    ``definition`` is the getDefinition response body; ``item_type`` is
    the Fabric item's type field (SemanticModel, Dataflow, etc.).
    """
    if item_type in ("DataflowGen1"):
        return [definition.get("pbi:mashup", {}).get("document", "")]
    if not definition or "definition" not in definition:
        return []
    parts = definition["definition"].get("parts", [])
    if item_type in ("SemanticModel", "Dataset"):
        return _from_semantic_model(parts)
    if item_type in ("Dataflow",):
        return _from_dataflow(parts)
    return _generic_scan(parts)


def _from_semantic_model(parts: list[dict]) -> list[dict]:
    """Semantic Model definitions ship as TMDL (.tmdl) or TMSL (model.bim)."""
    out: list[dict] = []
    for part in parts:
        path = part.get("path", "")
        text = _b64_decode(part.get("payload", ""))
        if not text:
            continue
        if path.endswith(".bim") or path.endswith("model.bim"):
            out.extend(_scan_tmsl(text))
        elif path.endswith(".tmdl"):
            out.extend(_scan_tmdl(text, path))
    return out


def _scan_tmsl(text: str) -> list[dict]:
    """Walk TMSL JSON for partition M expressions and shared expressions."""
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        return []
    out: list[dict] = []
    model = doc.get("model", doc)

    for table in model.get("tables", []):
        tname = table.get("name", "?")
        for partition in table.get("partitions", []):
            src = partition.get("source", {})
            # source.type == 'm' is a Power Query M expression.
            # 'calculated' means DAX calculated table - not scanned.
            if src.get("type") == "m" and "expression" in src:
                expr = _join_expression(src["expression"])
                out.append({"name": f"{tname}.{partition.get('name','')}", "expression": expr})

    for expr in model.get("expressions", []):
        out.append({
            "name": f"shared:{expr.get('name','?')}",
            "expression": _join_expression(expr.get("expression", "")),
        })
    return out


def _scan_tmdl(text: str, path: str) -> list[dict]:
    """Very light TMDL scan: pull ``source =`` blocks verbatim.

    TMDL is a whitespace-sensitive DSL; a full parser is overkill for signal
    extraction.  We slice out anything following ``source =`` until the next
    top-level indent decrease and treat it as an M expression.
    """
    out: list[dict] = []
    lines = text.splitlines()
    buf: list[str] = []
    capturing = False
    name = path

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("source ="):
            if buf:
                out.append({"name": name, "expression": "\n".join(buf)})
                buf = []
            capturing = True
            after = stripped[len("source ="):].strip()
            if after:
                buf.append(after)
            continue
        if capturing:
            if not line.startswith((" ", "\t")) and stripped:
                out.append({"name": name, "expression": "\n".join(buf)})
                buf = []
                capturing = False
            else:
                buf.append(line)
    if buf:
        out.append({"name": name, "expression": "\n".join(buf)})
    return out


def _from_dataflow(parts: list[dict]) -> list[dict]:
    """Dataflow Gen 2 packages mashup.pq alongside queryMetadata.json.

    For Gen 1 (model.json + base64 mashup zip) we can only inspect what's
    already exposed as a string; a full ZIP unpack is out of scope.
    """
    out: list[dict] = []
    for part in parts:
        path = part.get("path", "").lower()
        text = _b64_decode(part.get("payload", ""))
        if not text:
            continue
        if path.endswith("mashup.pq") or path.endswith(".pq") or path.endswith(".m"):
            out.append({"name": path, "expression": text})
            continue
        if path.endswith("model.json"):
            # Gen 1 - the actual mashup is a base64-encoded zip inside
            # a field like 'pbi:mashup' > 'document' > <base64>. We do a
            # best-effort walk: any string that decodes into text
            # containing "let" or a known connector prefix, we yield.
            try:
                doc = json.loads(text)
            except json.JSONDecodeError:
                continue
            for candidate in _walk_strings(doc):
                # Try treating the candidate as an M expression directly.
                if _looks_like_m(candidate):
                    out.append({"name": path, "expression": candidate})
                    continue
                # Try base64 decoding.
                inner = _b64_decode(candidate)
                if inner and _looks_like_m(inner):
                    out.append({"name": path, "expression": inner})
    return out


def _looks_like_m(text: str) -> bool:
    if not text or len(text) < 4:
        return False
    return "let " in text or "Source" in text or "shared " in text


def _generic_scan(parts: list[dict]) -> list[dict]:
    """Fallback: return the decoded text of every part for regex scanning."""
    out: list[dict] = []
    for part in parts:
        text = _b64_decode(part.get("payload", ""))
        if text and ("let" in text or "Source" in text):
            out.append({"name": part.get("path", "?"), "expression": text})
    return out


def _join_expression(expr) -> str:
    if isinstance(expr, list):
        return "\n".join(str(x) for x in expr)
    return str(expr)


def _walk_strings(obj) -> list[str]:
    """Return every string value anywhere in obj."""
    hits: list[str] = []
    stack: list = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for v in cur.values():
                if isinstance(v, str):
                    hits.append(v)
                else:
                    stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return hits


def _walk_for_key(obj, key: str) -> list[str]:
    """Return every string value stored under ``key`` anywhere in ``obj``."""
    hits: list[str] = []
    stack: list = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k == key and isinstance(v, str):
                    hits.append(v)
                else:
                    stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return hits


def expressions_from_scanner_dataset(dataset: dict) -> Iterable[dict]:
    """Scanner API embeds M expressions when datasetExpressions=True was set."""
    for table in dataset.get("tables", []):
        for source in table.get("source", []):
            expr = source.get("expression")
            if expr:
                yield {"name": table.get("name", "?"), "expression": expr}
    for expr in dataset.get("expressions", []):
        yield {"name": f"shared:{expr.get('name','?')}", "expression": expr.get("expression", "")}


def expressions_from_scanner_dataflow(dataflow: dict, item_type: str) -> Iterable[dict]:
    """Wraps the generic M expression extraction for scanner dataflow payloads."""
    expressions = extract_m_expressions(dataflow, item_type)
    for e in expressions:
        if isinstance(e, str) and _looks_like_m(e):
            yield {"name": "document", "expression": e}