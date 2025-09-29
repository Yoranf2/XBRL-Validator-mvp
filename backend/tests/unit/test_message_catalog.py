import os
import tempfile
import zipfile
from pathlib import Path

import pytest

from app.services.message_catalog import MessageCatalog


@pytest.mark.parametrize("zip_name", [
    "EBA_XBRL_4.0_Severity_4.0.0.0.zip",
    "EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0.zip",
])
def test_message_catalog_load_from_zip_globs(zip_name):
    root = Path(__file__).resolve().parents[4] / "github_work/eba-taxonomies/taxonomies/4.0"
    zp = root / zip_name
    if not zp.exists():
        pytest.skip(f"Required package zip not found: {zp}")
    mc = MessageCatalog(lang="en")
    loaded = mc.bulk_load_from_zip_globs([str(zp)])
    assert mc.ids_loaded() >= 1 and loaded >= 0
    # pick a key and assert resolve returns non-empty
    any_id = next(iter(mc._messages.keys()))  # noqa: SLF001
    assert mc.resolve(f"message:{any_id}")


def test_message_catalog_load_from_unpacked_extracted(tmp_path: Path):
    # Extract a small subset from the official Severity zip into a temp folder
    root = Path(__file__).resolve().parents[4] / "github_work/eba-taxonomies/taxonomies/4.0"
    zp = root / "EBA_XBRL_4.0_Severity_4.0.0.0.zip"
    if not zp.exists():
        pytest.skip(f"Required package zip not found: {zp}")
    # Extract all XML files under val/ to ensure messages are present
    extract_dir = tmp_path / "severity_unpacked"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zp) as zf:
        for name in zf.namelist():
            low = name.lower()
            if not low.endswith(".xml"):
                continue
            if ("/val/" in low) or ("message" in low) or ("catalog" in low):
                dest = extract_dir / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(name) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
    mc = MessageCatalog(lang="en")
    loaded = mc.load_from_unpacked_roots([str(extract_dir)])
    assert mc.ids_loaded() >= 1 and loaded >= 0
    any_id = next(iter(mc._messages.keys()))  # noqa: SLF001
    assert mc.resolve(f"message:{any_id}")


