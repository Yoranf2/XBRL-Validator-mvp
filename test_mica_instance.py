#!/usr/bin/env python3
"""
Diagnostic: Load a specific MICA instance and report facts, undefined facts,
namespace metrics, and DTS evidence using the current ArelleService behavior.

This script does not modify code. It only observes and reports.
"""

import sys
import logging
from pathlib import Path

# Add backend to path
PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_PATH = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_PATH))

from app.services.arelle_service import ArelleService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("diagnostic")

MICA_FILE = \
    "/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207.xbrl"

DTS_FIRST_SCHEMAS = [
    "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/mica/4.0/mod/mica_its.xsd",
    "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
]

PACKAGE_PATHS = [
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "rf-unpacked"),
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "dict-unpacked"),
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "severity-unpacked"),
]

def run():
    svc = ArelleService()
    svc.initialize({
        'allow_instance_rewrite': False,
    })
    svc.load_taxonomy_packages(PACKAGE_PATHS)

    logger.info("Loading instance with DTS-first schema preload (mica_its + met)")
    model_xbrl, facts_count = svc.load_instance(MICA_FILE, dts_first_schemas=DTS_FIRST_SCHEMAS)

    if not model_xbrl:
        print("ERROR: model_xbrl is None")
        return 2

    undefined_facts = len(getattr(model_xbrl, "undefinedFacts", []))
    contexts_count = len(getattr(model_xbrl, "contexts", {}))
    units_count = len(getattr(model_xbrl, "units", {}))

    # Build facts_by_namespace
    facts_by_ns = {}
    if hasattr(model_xbrl, 'facts'):
        for fact in model_xbrl.facts:
            if getattr(fact, 'qname', None) and getattr(fact.qname, 'namespaceURI', None):
                ns = fact.qname.namespaceURI
                facts_by_ns[ns] = facts_by_ns.get(ns, 0) + 1

    # Count eba_met concepts available in model
    eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
    eba_met_concepts = 0
    if hasattr(model_xbrl, 'qnameConcepts'):
        for qname in model_xbrl.qnameConcepts.keys():
            if getattr(qname, 'namespaceURI', None) == eba_met_ns:
                eba_met_concepts += 1

    # How many undefined facts are eba_met namespace by qname?
    eba_met_undefined = 0
    if hasattr(model_xbrl, 'undefinedFacts') and model_xbrl.undefinedFacts:
        for fact in model_xbrl.undefinedFacts:
            if getattr(fact, 'qname', None) and getattr(fact.qname, 'namespaceURI', None) == eba_met_ns:
                eba_met_undefined += 1

    print("=== Diagnostic Results ===")
    print(f"File: {MICA_FILE}")
    print(f"facts_count: {facts_count}")
    print(f"undefined_facts: {undefined_facts}")
    print(f"contexts: {contexts_count}, units: {units_count}")
    print(f"eba_met concepts available: {eba_met_concepts}")
    print(f"facts_by_namespace (top 10): {dict(list(sorted(facts_by_ns.items(), key=lambda x: -x[1])[:10]))}")
    print(f"eba_met undefined facts: {eba_met_undefined}")

    # Sample a few undefined facts
    if hasattr(model_xbrl, 'undefinedFacts') and model_xbrl.undefinedFacts:
        print("Sample undefined facts:")
        for i, fact in enumerate(list(model_xbrl.undefinedFacts)[:10]):
            try:
                qn = getattr(fact, 'qname', None)
                ns = getattr(qn, 'namespaceURI', None) if qn else None
                print(f"  {i+1}. {qn} (ns={ns}) value={getattr(fact, 'text', '')}")
            except Exception:
                print(f"  {i+1}. {fact}")

    return 0

if __name__ == "__main__":
    sys.exit(run())
