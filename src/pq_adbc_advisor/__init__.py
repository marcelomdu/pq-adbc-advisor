"""Power Query Connector Upgrade Advisor.

Read-only scanner + validator for the ODBC -> ADBC connector migration.

Typical customer usage (inside a Fabric notebook)::

    %pip install git+https://github.com/microsoft/pq-adbc-advisor.git
    from pq_adbc_advisor import scan_workspace, validate_migration

    # Phase 1 - discover EVERY connector in the workspace, bucketed by migration
    baseline = scan_workspace()
    baseline.summary()
    baseline.to_html("adbc_impact.html")

    # ... flip the ADBC switch ...

    # Phase 2 - validate every connector still refreshes; classify any failures
    result = validate_migration(baseline)
    result.summary()
    result.to_html("adbc_validation.html")
"""
from . import fabric_api
from .constants import IMPACTED_CONNECTORS, TOOL_VERSION
from .discovery import scan_tenant, scan_workspace
from .mcode import ConnectorCall, find_all_connectors, find_hits
from .preflight import preflight_check
from .report import ImpactedArtifact, ImpactReport, ValidationReport, ValidationResult
from .telemetry import (
    disable_telemetry,
    enable_telemetry,
    send_canary,
    telemetry_health,
    telemetry_status,
)
from .troubleshoot import Diagnosis, diagnose
from .validation import validate_migration

__version__ = TOOL_VERSION
__all__ = [
    "fabric_api",
    "scan_workspace",
    "scan_tenant",
    "validate_migration",
    "preflight_check",
    "find_all_connectors",
    "find_hits",
    "diagnose",
    "disable_telemetry",
    "enable_telemetry",
    "telemetry_status",
    "telemetry_health",
    "send_canary",
    "ConnectorCall",
    "Diagnosis",
    "ImpactReport",
    "ImpactedArtifact",
    "ValidationReport",
    "ValidationResult",
    "IMPACTED_CONNECTORS",
]
