#!/usr/bin/env python3
"""
Enumerate formula assertions for a taxonomy entrypoint and write/update a baseline.

Usage examples:
  python backend/scripts/enumerate_assertions.py --entrypoint-id corep_lr --taxonomy-version 4.0.0.0
  python backend/scripts/enumerate_assertions.py --entrypoint-xsd http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/mod/corep_lr.xsd --taxonomy-version 4.0.0.0

Notes:
  - Requires packages configured in backend/config/eba_taxonomies.yaml
  - Runs strictly offline (no HTTP); uses the same ArelleService as validation
"""

import argparse
import json
import sys
from pathlib import Path
import hashlib

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from app.services.arelle_service import ArelleService  # type: ignore


def _find_catalog_dirs_for_40() -> list[str]:
    root = PROJECT_ROOT / "github_work" / "eba-taxonomies" / "taxonomies" / "4.0"
    if not root.exists():
        return []
    candidates = []
    for name in [
        "EBA_XBRL_4.0_Dictionary_4.0.0.0",
        "EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0",
        "EBA_XBRL_4.0_Severity_4.0.0.0",
    ]:
        d = root / name
        if (d / "META-INF" / "catalog.xml").exists():
            candidates.append(str(d))
    return candidates


def load_packages_from_cfg() -> list[str]:
    cfg_path = PROJECT_ROOT / "backend" / "config" / "eba_taxonomies.yaml"
    try:
        import yaml
        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        pkgs = (data.get("eba", {}) or {}).get("rf40", {}).get("packages", []) or []
        paths = [str((PROJECT_ROOT / p).resolve()) for p in pkgs]
        # If YAML still lists zips, try sibling directory with same stem
        fixed: list[str] = []
        for p in paths:
            if p.endswith('.zip'):
                stem = p[:-4]
                if (Path(stem) / 'META-INF' / 'catalog.xml').exists():
                    fixed.append(stem)
                else:
                    fixed.append(p)
            else:
                fixed.append(p)
        # If no valid catalogs among fixed, try scanning default 4.0 root
        if not any((Path(x) / 'META-INF' / 'catalog.xml').exists() for x in fixed):
            scan = _find_catalog_dirs_for_40()
            if scan:
                return scan
        return fixed
    except Exception:
        return []


def enumerate_assertions_for_entrypoint(entrypoint_xsd: str) -> tuple[int, list[str]]:
    """Return (present_count, stable_ids) for all formula assertions in DTS."""
    svc = ArelleService(cache_dir=(BACKEND / "cache"))
    svc.initialize({})
    pkgs = load_packages_from_cfg()
    if pkgs:
        try:
            # Explicitly register package manifests like the service does
            from arelle import PackageManager  # type: ignore
            loaded_any = False
            for p in pkgs:
                path = Path(p)
                # Accept directory or zip
                if path.is_dir():
                    mani = path / "META-INF" / "taxonomyPackage.xml"
                    if mani.exists():
                        try:
                            if PackageManager.addPackage(svc.cntlr, str(mani)):
                                loaded_any = True
                        except Exception:
                            pass
                elif path.is_file() and path.suffix.lower() == ".zip":
                    try:
                        if PackageManager.addPackage(svc.cntlr, str(path)):
                            loaded_any = True
                    except Exception:
                        pass
            try:
                PackageManager.rebuildRemappings(svc.cntlr)
            except Exception:
                pass
            # Also call service helper to build internal maps (best-effort)
            try:
                svc._build_catalog_map()
                svc._register_catalogs_with_arelle()
            except Exception:
                pass
        except Exception:
            pass

    model_xbrl = None
    stable_ids: list[str] = []
    try:
        # Load entrypoint schema only (no instance) so the DTS includes formula networks
        from arelle import FileSource  # type: ignore
        from arelle import ModelDocument  # type: ignore

        local = svc._resolve_dict_url(entrypoint_xsd) or entrypoint_xsd
        fs = FileSource.openFileSource(local, svc.cntlr)
        try:
            taxonomy_pkgs = getattr(svc, '_loaded_package_paths', []) or None
        except Exception:
            taxonomy_pkgs = None
        model_xbrl = svc.model_manager.load(fs, taxonomyPackages=taxonomy_pkgs)
        if model_xbrl is None:
            return 0, []

        # Enumerate assertions across the XBRL Formula assertion namespaces
        # EBA 4.0 uses value/existence/consistency assertion namespaces, not the generic formula ns
        ASSERTION_NAMESPACES = {
            "http://xbrl.org/2008/formula": {"assertion", "valueAssertion", "existenceAssertion", "consistencyAssertion"},
            "http://xbrl.org/2008/assertion/value": {"valueAssertion"},
            "http://xbrl.org/2008/assertion/existence": {"existenceAssertion"},
            "http://xbrl.org/2008/assertion/consistency": {"consistencyAssertion"},
        }

        try:
            objects = getattr(model_xbrl, "modelObjects", [])
        except Exception:
            objects = []

        for obj in objects:
            try:
                ln = getattr(obj, "localName", "")
                ns = getattr(obj, "namespaceURI", "")
                if ns in ASSERTION_NAMESPACES and ln in ASSERTION_NAMESPACES[ns]:
                    doc = getattr(getattr(obj, "modelDocument", None), "uri", "")
                    lbl = getattr(obj, "xlinkLabel", "") or getattr(obj, "id", "")
                    role = getattr(obj, "xlinkRole", "")
                    stable = f"{doc}|{ln}|{lbl}|{role}"
                    stable_ids.append(stable)
            except Exception:
                continue

        # Fallback: if no objects found, traverse DTS documents and count assertions by XML
        if not stable_ids:
            try:
                docs = []
                if hasattr(model_xbrl, 'urlDocs') and isinstance(model_xbrl.urlDocs, dict):
                    docs = list(model_xbrl.urlDocs.values())
                elif hasattr(model_xbrl, 'modelDocument') and model_xbrl.modelDocument is not None:
                    docs = [model_xbrl.modelDocument]
                ASSERTION_NAMESPACES = {
                    "http://xbrl.org/2008/formula": {"assertion", "valueAssertion", "existenceAssertion", "consistencyAssertion"},
                    "http://xbrl.org/2008/assertion/value": {"valueAssertion"},
                    "http://xbrl.org/2008/assertion/existence": {"existenceAssertion"},
                    "http://xbrl.org/2008/assertion/consistency": {"consistencyAssertion"},
                }
                for d in docs:
                    try:
                        xml = getattr(d, 'xmlDocument', None)
                        if xml is None:
                            continue
                        root = getattr(xml, 'getroot', lambda: None)()
                        if root is None:
                            continue
                        # iterate all elements; match by namespace/localname
                        for el in root.iter():
                            try:
                                tag = el.tag
                                if not isinstance(tag, str) or not tag.startswith('{'):
                                    continue
                                ns, ln = tag[1:].split('}', 1)
                                allowed = ASSERTION_NAMESPACES.get(ns)
                                if allowed and ln in allowed:
                                    doc = getattr(d, 'uri', '') or ''
                                    lbl = el.get('{http://www.w3.org/1999/xlink}label', '') or el.get('id', '') or ''
                                    role = el.get('{http://www.w3.org/1999/xlink}role', '') or ''
                                    stable = f"{doc}|{ln}|{lbl}|{role}"
                                    stable_ids.append(stable)
                            except Exception:
                                continue
                    except Exception:
                        continue
            except Exception:
                pass
        stable_ids = sorted(set(stable_ids))
        return len(stable_ids), stable_ids
    finally:
        try:
            if model_xbrl is not None:
                model_xbrl.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrypoint-id", help="Entrypoint id from config (e.g., corep_lr)")
    ap.add_argument("--entrypoint-xsd", help="Entrypoint XSD URL")
    ap.add_argument("--taxonomy-version", required=False, default="unknown", help="Taxonomy version key (e.g., 4.0.0.0)")
    args = ap.parse_args()

    # Resolve entrypoint-xsd from id if provided
    xsd = args.entrypoint_xsd
    if not xsd and args.entrypoint_id:
        # Attempt to map from config
        cfg = PROJECT_ROOT / "backend" / "config" / "eba_taxonomies.yaml"
        try:
            import yaml
            data = yaml.safe_load(cfg.read_text(encoding="utf-8")) or {}
            eps = ((data.get("eba", {}) or {}).get("rf40", {}) or {}).get("entrypoints", []) or []
            for ep in eps:
                if ep.get("id") == args.entrypoint_id:
                    xsd = ep.get("xsd")
                    break
        except Exception:
            xsd = None
    if not xsd:
        print("error: entrypoint-xsd not resolved", file=sys.stderr)
        sys.exit(2)

    present_count, stable_ids = enumerate_assertions_for_entrypoint(xsd)
    digest = hashlib.sha256("\n".join(stable_ids).encode("utf-8")).hexdigest()

    baseline_path = PROJECT_ROOT / "backend" / "config" / "assertion_baseline.json"
    try:
        current = json.loads(baseline_path.read_text(encoding="utf-8"))
    except Exception:
        current = {}

    key = f"{args.taxonomy_version}:{args.entrypoint_id or xsd}"
    current[key] = {
        "present_count": present_count,
        "hash": digest,
        "ids_sample": stable_ids[:5],
    }
    baseline_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"entrypoint": key, "present_count": present_count, "hash": digest}, ensure_ascii=False))


if __name__ == "__main__":
    main()


