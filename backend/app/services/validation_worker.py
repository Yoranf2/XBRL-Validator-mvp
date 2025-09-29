"""
Subprocess validation worker entrypoint.

This module provides a top-level callable suitable for ProcessPoolExecutor.
It constructs its own ArelleService in the child process, loads taxonomy
packages, loads the instance, and runs validation, returning a plain dict.
"""

from typing import Any, Dict, List, Optional, Tuple

def run_validation_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute validation in a fresh process and return results.

    Expected task keys:
      - file_path: str
      - profile: str (fast|full|debug)
      - config: dict (subset for ArelleService.initialize)
      - package_paths: List[str]
      - dts_first_schemas: Optional[List[str]]
    """
    # Local imports inside child process to avoid pickling issues
    import sys
    from pathlib import Path
    # Ensure vendored Arelle path is importable if present relative to project tree
    try:
        project_root = Path(__file__).resolve().parents[3]
        arelle_path = project_root / "third_party" / "arelle"
        if arelle_path.exists():
            sys.path.insert(0, str(arelle_path))
    except Exception:
        pass

    from app.services.arelle_service import ArelleService

    file_path: str = task.get("file_path")
    profile: str = task.get("profile") or "fast"
    config: Dict[str, Any] = task.get("config") or {}
    package_paths: List[str] = task.get("package_paths") or []
    dts_first_schemas: Optional[List[str]] = task.get("dts_first_schemas")

    cache_dir = Path(config.get("cache_dir") or (Path.cwd() / "backend" / "cache"))
    svc = ArelleService(cache_dir=cache_dir)
    svc.initialize(config)
    if package_paths:
        try:
            svc.load_taxonomy_packages(package_paths)
        except Exception:
            # Continue; Validate may still proceed if remappings exist
            pass

    model_xbrl = None
    facts_count = 0
    try:
        model_xbrl, facts_count = svc.load_instance(file_path, dts_first_schemas=dts_first_schemas)
        results = svc.validate_instance(model_xbrl, profile=profile)
    except Exception as e:
        return {
            "status": "error",
            "errors": [{"code": "validation_error", "message": str(e), "severity": "error"}],
            "warnings": [],
            "facts_count": 0,
            "duration_ms": 0,
            "profile": profile,
            "dts_evidence": {},
            "metrics": {}
        }
    finally:
        try:
            if model_xbrl is not None:
                model_xbrl.close()
        except Exception:
            pass

    # Ensure facts_count present from our load step if not set
    if results.get("facts_count", 0) == 0 and facts_count:
        results["facts_count"] = facts_count
    return results


