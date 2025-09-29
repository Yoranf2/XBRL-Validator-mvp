#!/usr/bin/env python3
"""
Iteration 0 spike: Build DTS first (mica_its + met) and then load the instance
with the same ModelXbrl so Arelle classifies facts with dictionary present.
This script is read-only (no code changes), prints evidence and exits nonzero on failure.
"""

import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_PATH = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_PATH))

from app.services.arelle_service import ArelleService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("iter0")

INSTANCE = \
    "/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207.xbrl"

SCHEMAS = [
    "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/mica/4.0/mod/mica_its.xsd",
    "http://www.eba.europa.eu/xbrl/crr/dict/met/met.xsd",
]

PACKAGE_PATHS = [
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "rf-unpacked"),
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "dict-unpacked"),
    str(BACKEND_PATH / "github_work" / "taxonomies" / "eba" / "rf40" / "severity-unpacked"),
]

def main() -> int:
    svc = ArelleService()
    svc.initialize({})
    svc.load_taxonomy_packages(PACKAGE_PATHS)

    from arelle import FileSource
    from arelle import ModelDocument

    # Load base schema (mica_its)
    mica_local = svc._resolve_dict_url(SCHEMAS[0])
    if not mica_local or not Path(mica_local).exists():
        logger.error(f"Could not resolve mica_its.xsd: {SCHEMAS[0]}")
        return 2
    base_fs = FileSource.openFileSource(mica_local, svc.cntlr)
    base_model = svc.model_manager.load(base_fs)
    if not base_model:
        logger.error("Failed to load base DTS from mica_its.xsd")
        return 2

    # Augment DTS with met.xsd
    met_local = svc._resolve_dict_url(SCHEMAS[1])
    if not met_local or not Path(met_local).exists():
        logger.error(f"Could not resolve met.xsd: {SCHEMAS[1]}")
        return 2
    ModelDocument.load(base_model, met_local, base=base_model.modelDocument)

    # Evidence: eba_met concepts loaded in base
    eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
    eba_met_concepts_in_base = 0
    if hasattr(base_model, 'qnameConcepts'):
        for qn in base_model.qnameConcepts.keys():
            if getattr(qn, 'namespaceURI', None) == eba_met_ns:
                eba_met_concepts_in_base += 1
    logger.info(f"Base DTS eba_met concepts: {eba_met_concepts_in_base}")

    # Load instance into the same model
    ModelDocument.load(base_model, INSTANCE, base=base_model.modelDocument, isEntry=True)

    # Metrics from base_model
    facts = len(getattr(base_model, 'factsInInstance', [])) or len(getattr(base_model, 'facts', []))
    undefined = len(getattr(base_model, 'undefinedFacts', []))

    facts_by_ns = {}
    if hasattr(base_model, 'facts'):
        for fact in base_model.facts:
            if getattr(fact, 'qname', None) and getattr(fact.qname, 'namespaceURI', None):
                ns = fact.qname.namespaceURI
                facts_by_ns[ns] = facts_by_ns.get(ns, 0) + 1

    eba_met_undefined = 0
    if hasattr(base_model, 'undefinedFacts') and base_model.undefinedFacts:
        for f in base_model.undefinedFacts:
            if getattr(f, 'qname', None) and getattr(f.qname, 'namespaceURI', None) == eba_met_ns:
                eba_met_undefined += 1

    print("=== Iteration 0 Spike Results ===")
    print(f"eba_met concepts in base DTS: {eba_met_concepts_in_base}")
    print(f"facts_count: {facts}")
    print(f"undefined_facts: {undefined} (eba_met: {eba_met_undefined})")
    print(f"facts_by_namespace (top 10): {dict(list(sorted(facts_by_ns.items(), key=lambda x: -x[1])[:10]))}")

    if eba_met_concepts_in_base <= 0:
        logger.error("Base DTS missing eba_met concepts")
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
