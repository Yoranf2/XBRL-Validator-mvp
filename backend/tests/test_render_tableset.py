"""
Integration test: render-all (EBA tableset) for the fixed COREP LR sample.

Requires vendored Arelle and local taxonomy packages configured as per backend/README.md.
Runs strictly offline.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _sample_corep_lr_path() -> Path:
    # Fixed sample that is known to produce v-codes; used only to verify rendering pipeline
    return Path(
        "/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_COREP040000_COREPLR_2024-12-31_20241211134749200.xbrl"
    )


@pytest.mark.integration
def test_render_tableset_corep_lr_sample():
    # Import app after environment to ensure vendored Arelle path is registered in sys.path
    from app.main import app

    client = TestClient(app)

    sample_path = _sample_corep_lr_path()
    assert sample_path.exists(), f"Sample instance not found: {sample_path}"

    with open(sample_path, "rb") as fh:
        files = {"file": (sample_path.name, fh, "application/xml")}
        data = {"lang": "en"}
        resp = client.post("/api/v1/render/tableset", files=files, data=data)

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload.get("status") == "success"
    index_url = payload.get("index_url")
    index_path = payload.get("path")

    assert isinstance(index_url, str) and index_url.startswith("/static/tables/") and index_url.endswith("/index.html")
    assert isinstance(index_path, str) and index_path.endswith("index.html")
    assert Path(index_path).exists(), f"index.html not found at {index_path}"


