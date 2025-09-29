#!/usr/bin/env python3
"""
Debug script to test Arelle package loading and fact counting
"""
import sys
from pathlib import Path

# Add Arelle to path
PROJECT_ROOT = Path(__file__).resolve().parent
ARELLE_PATH = PROJECT_ROOT / "third_party" / "arelle"
sys.path.insert(0, str(ARELLE_PATH))

from arelle import Cntlr, ModelManager

def test_package_loading():
    print("Testing Arelle package loading...")
    
    # Initialize Arelle
    cntlr = Cntlr.Cntlr(logFileName=None)
    mm = ModelManager.initialize(cntlr)
    
    # Configure offline mode
    cntlr.webCache.workOffline = True
    cntlr.internetConnectivity = 'offline'
    
    # Test package paths
    package_paths = [
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "rf-unpacked"),
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "dict-unpacked"),
        str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "severity-unpacked")
    ]
    
    print(f"Package paths: {package_paths}")
    
    # Check if META-INF/taxonomyPackage.xml exists
    for pkg_path in package_paths:
        manifest_path = Path(pkg_path) / "META-INF" / "taxonomyPackage.xml"
        print(f"Package {pkg_path}: manifest exists = {manifest_path.exists()}")
    
    # Load packages
    from arelle import PackageManager
    for pkg_path in package_paths:
        try:
            PackageManager.addPackage(cntlr, pkg_path)
            print(f"✓ Loaded package: {pkg_path}")
        except Exception as e:
            print(f"✗ Failed to load package {pkg_path}: {e}")
    
    # Test loading a simple XBRL file
    test_file = PROJECT_ROOT / "test-samples" / "v4.0" / "base" / "base_corep_comprehensive.xbrl"
    if test_file.exists():
        print(f"\nTesting with file: {test_file}")
        try:
            model_xbrl = mm.load(str(test_file))
            if model_xbrl:
                facts_count = len(getattr(model_xbrl, "factsInInstance", [])) or len(getattr(model_xbrl, "facts", []))
                undefined_facts = len(getattr(model_xbrl, "undefinedFacts", []))
                contexts_count = len(getattr(model_xbrl, "contexts", {}))
                units_count = len(getattr(model_xbrl, "units", {}))
                
                print(f"Facts count: {facts_count}")
                print(f"Undefined facts: {undefined_facts}")
                print(f"Contexts: {contexts_count}")
                print(f"Units: {units_count}")
                
                # Show some undefined facts if any
                if undefined_facts > 0:
                    print("Sample undefined facts:")
                    for i, fact in enumerate(model_xbrl.undefinedFacts[:5]):
                        print(f"  {i+1}: {fact.qname} = {fact.text}")
            else:
                print("Failed to load model")
        except Exception as e:
            print(f"Error loading file: {e}")
    else:
        print(f"Test file not found: {test_file}")

if __name__ == "__main__":
    test_package_loading()
