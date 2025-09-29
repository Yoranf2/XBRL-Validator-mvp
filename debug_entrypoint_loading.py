#!/usr/bin/env python3
"""
Debug script to test loading with proper entrypoints that include dictionary imports
"""
import sys
from pathlib import Path

# Add Arelle to path
PROJECT_ROOT = Path(__file__).resolve().parent
ARELLE_PATH = PROJECT_ROOT / "third_party" / "arelle"
sys.path.insert(0, str(ARELLE_PATH))

from arelle import Cntlr, ModelManager, FileSource

def test_entrypoint_loading():
    print("Testing entrypoint loading with dictionary imports...")
    
    # Initialize Arelle
    cntlr = Cntlr.Cntlr(logFileName=None)
    mm = ModelManager.initialize(cntlr)
    
    # Configure offline mode with catalog support
    cntlr.webCache.workOffline = True
    cntlr.internetConnectivity = 'offline'
    cntlr.config['internetConnectivity'] = 'offline'
    cntlr.config['workOffline'] = True
    cntlr.config['allow_catalogs'] = True
    
    # Load packages
    package_paths = [
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "rf-unpacked"),
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "dict-unpacked"),
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "severity-unpacked")
    ]
    
    from arelle import PackageManager
    for pkg_path in package_paths:
        try:
            PackageManager.addPackage(cntlr, pkg_path)
            print(f"✓ Loaded package: {pkg_path}")
        except Exception as e:
            print(f"✗ Failed to load package {pkg_path}: {e}")
    
    # Test with COREP entrypoint
    corep_entrypoint = "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/mod/corep_of.xsd"
    print(f"\nTesting with COREP entrypoint: {corep_entrypoint}")
    
    try:
        # Load entrypoint directly
        model_xbrl = mm.load(corep_entrypoint)
        
        if model_xbrl:
            print("✓ Successfully loaded COREP entrypoint")
            
            # Check DTS for met.xsd
            print("\nChecking DTS for met.xsd...")
            met_schema_found = False
            for doc in model_xbrl.urlDocs.values():
                if 'dict/met/met.xsd' in str(doc.uri):
                    print(f"✓ Found met.xsd in DTS: {doc.uri}")
                    met_schema_found = True
                    break
            
            if not met_schema_found:
                print("✗ met.xsd NOT found in DTS")
                print("Available DTS documents:")
                for uri in sorted(model_xbrl.urlDocs.keys()):
                    print(f"  - {uri}")
            
            # Check qnameConcepts for eba_met namespace
            print("\nChecking qnameConcepts for eba_met namespace...")
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            eba_met_concepts = []
            
            for qname, concept in model_xbrl.qnameConcepts.items():
                if qname.namespaceURI == eba_met_ns:
                    eba_met_concepts.append(concept)
            
            print(f"Found {len(eba_met_concepts)} concepts in eba_met namespace:")
            if eba_met_concepts:
                for concept in eba_met_concepts[:10]:  # Show first 10
                    print(f"  ✓ {concept.qname}")
                if len(eba_met_concepts) > 10:
                    print(f"  ... and {len(eba_met_concepts) - 10} more")
            else:
                print("✗ No concepts found in eba_met namespace")
            
            # Test specific concept resolution
            print("\nTesting specific concept resolution...")
            test_qname = "{http://www.eba.europa.eu/xbrl/crr/dict/met}qAOJ"
            concept = model_xbrl.qnameConcepts.get(test_qname)
            if concept:
                print(f"✓ Successfully resolved eba_met:qAOJ -> {concept.qname}")
            else:
                print(f"✗ Failed to resolve eba_met:qAOJ")
                
        else:
            print("✗ Failed to load COREP entrypoint")
    except Exception as e:
        print(f"Error loading entrypoint: {e}")
        import traceback
        traceback.print_exc()
    
    # Test loading met.xsd as entrypoint
    print(f"\n" + "="*60)
    print("Testing with met.xsd as entrypoint...")
    
    met_entrypoint = "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
    try:
        met_model = mm.load(met_entrypoint)
        
        if met_model:
            print("✓ Successfully loaded met.xsd as entrypoint")
            
            # Check concepts
            eba_met_concepts = []
            for qname, concept in met_model.qnameConcepts.items():
                if qname.namespaceURI == "http://www.eba.europa.eu/xbrl/crr/dict/met":
                    eba_met_concepts.append(concept)
            
            print(f"Found {len(eba_met_concepts)} eba_met concepts in met.xsd entrypoint")
            if eba_met_concepts:
                print("Sample concepts:")
                for concept in eba_met_concepts[:5]:
                    print(f"  - {concept.qname}")
        else:
            print("✗ Failed to load met.xsd as entrypoint")
    except Exception as e:
        print(f"Error loading met.xsd entrypoint: {e}")

if __name__ == "__main__":
    test_entrypoint_loading()
