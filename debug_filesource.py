#!/usr/bin/env python3
"""
Debug script to test FileSource loading with catalog remapping
"""
import sys
from pathlib import Path

# Add Arelle to path
PROJECT_ROOT = Path(__file__).resolve().parent
ARELLE_PATH = PROJECT_ROOT / "third_party" / "arelle"
sys.path.insert(0, str(ARELLE_PATH))

from arelle import Cntlr, ModelManager, FileSource

def test_filesource_loading():
    print("Testing FileSource loading with catalog remapping...")
    
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
    
    # Test with MICA file using FileSource
    mica_file = PROJECT_ROOT / "github_work" / "eba-taxonomies" / "EBA Taxonomy 4.0" / "sample_files" / "DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207.xbrl"
    
    if mica_file.exists():
        print(f"\nTesting with MICA file using FileSource: {mica_file}")
        try:
            # Use FileSource instead of direct path
            file_source = FileSource.openFileSource(str(mica_file), cntlr)
            model_xbrl = mm.load(file_source)
            
            if model_xbrl:
                facts_count = len(getattr(model_xbrl, "factsInInstance", [])) or len(getattr(model_xbrl, "facts", []))
                undefined_facts = len(getattr(model_xbrl, "undefinedFacts", []))
                contexts_count = len(getattr(model_xbrl, "contexts", {}))
                units_count = len(getattr(model_xbrl, "units", {}))
                
                print(f"Facts count: {facts_count}")
                print(f"Undefined facts: {undefined_facts}")
                print(f"Contexts: {contexts_count}")
                print(f"Units: {units_count}")
                
                # Show schema location elements
                if hasattr(model_xbrl, 'modelDocument') and hasattr(model_xbrl.modelDocument, 'schemaLocationElements'):
                    schema_locations = model_xbrl.modelDocument.schemaLocationElements
                    if schema_locations:
                        schema_list = list(schema_locations) if hasattr(schema_locations, '__iter__') else []
                        print(f"Schema location evidence 1: {schema_list[0] if len(schema_list) > 0 else 'none'}")
                        print(f"Schema location evidence 2: {schema_list[1] if len(schema_list) > 1 else 'none'}")
                
                # Show some undefined facts if any
                if undefined_facts > 0:
                    print("Sample undefined facts:")
                    for i, fact in enumerate(model_xbrl.undefinedFacts[:5]):
                        print(f"  {i+1}: {fact.qname} = {fact.text}")
                
                # Show some facts if any
                if facts_count > 0:
                    print("Sample facts:")
                    for i, fact in enumerate(list(model_xbrl.factsInInstance)[:5]):
                        print(f"  {i+1}: {fact.qname} = {fact.text}")
            else:
                print("Failed to load model")
        except Exception as e:
            print(f"Error loading file: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"MICA file not found: {mica_file}")

if __name__ == "__main__":
    test_filesource_loading()
