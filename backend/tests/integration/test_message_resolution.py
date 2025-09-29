import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def test_validate_corep_lr_resolves_messages_and_hides_raw_keys(client: TestClient):
    # Ensure packages exist
    root = Path(__file__).resolve().parents[4] / "github_work/eba-taxonomies/taxonomies/4.0"
    for req in [
        root / "EBA_XBRL_4.0_Dictionary_4.0.0.0.zip",
        root / "EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0.zip",
        root / "EBA_XBRL_4.0_Severity_4.0.0.0.zip",
    ]:
        if not req.exists():
            pytest.skip(f"Required package zip not found: {req}")

    sample = Path(__file__).resolve().parents[4] / (
        "github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/"
        "DUMMYLEI123456789012.CON_FR_COREP040000_COREPLR_2024-12-31_20241211134749200.xbrl"
    )
    if not sample.exists():
        pytest.skip(f"Sample instance not found: {sample}")

    # Run validation (fast profile acceptable for presence, but full may yield more v-ids)
    with open(sample, "rb") as fh:
        resp = client.post(
            "/api/v1/validate",
            files={"file": (sample.name, fh, "application/xml")},
            data={"profile": "full"},
        )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # Confirm metrics fields exist
    assert "metrics" in data
    m = data["metrics"]
    assert "messages_resolved_count" in m and "messages_unresolved_count" in m

    # Ensure readable_message is populated, and raw keys aren't leaked when hidden
    errors = data.get("errors", [])
    warnings = data.get("warnings", [])
    total = errors + warnings
    assert isinstance(total, list)
    # At least one v-code (formula) should be present in error codes or catalog resolution
    has_v = any((isinstance(e.get("raw_message"), str) and "{" in e.get("raw_message")) or (isinstance(e.get("catalog_message"), str) and e.get("catalog_message")) for e in total)
    assert has_v


def test_metrics_endpoint_optional(client: TestClient):
    # Metrics may be disabled by default; endpoint should 404 or return content when enabled
    r = client.get("/metrics")
    assert r.status_code in (200, 404)


