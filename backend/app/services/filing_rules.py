"""
Filing Rules Preflight (Balanced policy)

Performs lightweight, offline-first checks prior to full validation.
Does not mutate inputs; returns structured results for logging and metrics.
"""

from typing import Any, Dict, List


def run_preflight(model_xbrl: Any, context: Dict[str, Any], light: bool = False) -> Dict[str, Any]:
    """
    Execute balanced preflight checks.

    Args:
        model_xbrl: Loaded Arelle ModelXbrl
        context: Additional context, e.g., offline_status, entrypoint hints

    Returns:
        Dict with keys: passed (int), failed (int), items (list of dicts)
    """
    items: List[Dict[str, Any]] = []

    def add_item(_id: str, ok: bool, severity: str, message: str, source: str = "preflight") -> None:
        items.append({
            "id": _id,
            "ok": bool(ok),
            "severity": severity,
            "message": message,
            "source": source,
        })

    # Offline invariant
    offline = context.get("offline_status", {}) if isinstance(context, dict) else {}
    attempts = offline.get("http_fetch_attempts", []) if isinstance(offline, dict) else []
    if attempts:
        add_item(
            _id="offline:no_http",
            ok=False,
            severity="error",
            message=f"HTTP fetch attempts detected: {attempts}",
        )
    else:
        add_item("offline:no_http", True, "info", "No HTTP attempts detected")

    # Structural hygiene: contexts
    try:
        ctx_count = len(getattr(model_xbrl, "contexts", {}) or {})
        if ctx_count == 0:
            add_item("structure:contexts_present", False, "error", "No contexts present")
        else:
            add_item("structure:contexts_present", True, "info", f"Contexts: {ctx_count}")
    except Exception as e:
        add_item("structure:contexts_present", False, "warning", f"Context inspection failed: {e}")

    # Structural hygiene: units
    try:
        unit_count = len(getattr(model_xbrl, "units", {}) or {})
        if unit_count == 0:
            add_item("structure:units_present", False, "error", "No units present")
        else:
            add_item("structure:units_present", True, "info", f"Units: {unit_count}")
    except Exception as e:
        add_item("structure:units_present", False, "warning", f"Unit inspection failed: {e}")

    # Filing indicators (Balanced policy)
    try:
        fi_ns = "http://www.xbrl.org/taxonomy/int/filing-indicators/REC/2021-02-03"
        facts = list(getattr(model_xbrl, "facts", []) or [])
        fi_facts = []
        if light:
            # Light mode: stop at first indicator to avoid scanning all facts
            for f in facts:
                if getattr(getattr(f, "qname", None), "namespaceURI", None) == fi_ns:
                    fi_facts = [f]
                    break
        else:
            fi_facts = [f for f in facts if getattr(getattr(f, "qname", None), "namespaceURI", None) == fi_ns]
        if not fi_facts:
            add_item("fi:presence", False, "warning", "No filing indicators found (balanced policy: warning)")
        else:
            # Ill-formed and conflicting checks
            filed_true = 0
            invalid = 0
            for f in fi_facts:
                try:
                    val = str(getattr(f, "value", "")).strip().lower()
                    if val in ("true", "1"):  # treat as boolean true
                        filed_true += 1
                    elif val in ("false", "0"):
                        pass
                    else:
                        invalid += 1
                except Exception:
                    invalid += 1
            if invalid > 0:
                add_item("fi:ill_formed", False, "error", f"Invalid filing indicator values: {invalid}")
            add_item("fi:filed_true", True, "info", f"filed=true count: {filed_true}")
            # Conflicts are framework-specific; placeholder OK
    except Exception as e:
        add_item("fi:presence", False, "warning", f"Filing indicators check failed: {e}")

    # Emit a dedicated filing indicators log if there are any warnings/errors related to indicators
    try:
        fi_issues = [i for i in items if i.get("id", "").startswith("fi:") and (not i.get("ok"))]
        if fi_issues and hasattr(model_xbrl, 'modelManager'):
            from pathlib import Path
            import json, uuid
            logs_dir = Path(__file__).resolve().parents[2] / "logs"
            logs_dir.mkdir(exist_ok=True)
            run_id = uuid.uuid4().hex[:8]
            payload = {
                "filing_indicators_issues": fi_issues,
            }
            (logs_dir / f"filing_indicators_{run_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass

    # Entrypoint/module consistency (lightweight)
    try:
        uri = str(getattr(getattr(model_xbrl, "modelDocument", None), "uri", ""))
        add_item("entrypoint:uri", True, "info", f"Entrypoint: {uri}")
    except Exception as e:
        add_item("entrypoint:uri", False, "warning", f"Entrypoint detection failed: {e}")

    passed = sum(1 for i in items if i.get("ok") is True)
    failed = sum(1 for i in items if i.get("ok") is False and i.get("severity") in ("error", "warning"))
    return {"passed": passed, "failed": failed, "items": items}

"""
Filing Rules Service

Handles Filing Rules pre and post validation checks.
Will be implemented in subsequent iterations.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class FilingRulesService:
    """Service for Filing Rules validation."""
    
    def __init__(self):
        """Initialize Filing Rules service."""
        logger.info("Initializing FilingRulesService")
    
    def pre_validation_checks(self, instance_data: Any) -> Dict[str, Any]:
        """
        Perform pre-validation Filing Rules checks.
        
        Args:
            instance_data: XBRL instance data
            
        Returns:
            Dictionary with pre-validation results
        """
        logger.info("Filing Rules pre-validation checks")
        
        # TODO: Implement Filing Rules pre-validation
        return {
            "status": "not_implemented",
            "message": "Filing Rules pre-validation will be implemented in subsequent iterations"
        }
    
    def post_validation_checks(self, instance_data: Any, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform post-validation Filing Rules checks.
        
        Args:
            instance_data: XBRL instance data
            validation_results: Results from primary validation
            
        Returns:
            Dictionary with post-validation results
        """
        logger.info("Filing Rules post-validation checks")
        
        # TODO: Implement Filing Rules post-validation
        return {
            "status": "not_implemented", 
            "message": "Filing Rules post-validation will be implemented in subsequent iterations"
        }
