#!/usr/bin/env python3
"""
Debug script to test the modified Arelle service with dictionary schema loading
"""
import sys
from pathlib import Path

# Add Arelle to path
PROJECT_ROOT = Path(__file__).resolve().parent
ARELLE_PATH = PROJECT_ROOT / "third_party" / "arelle"
sys.path.insert(0, str(ARELLE_PATH))

from backend.app.services.arelle_service import ArelleService

def test_modified_service():
    print("Testing modified Arelle service with dictionary schema loading...")
    
    # Initialize Arelle service
    cache_dir = PROJECT_ROOT / "backend" / "cache"
    arelle_service = ArelleService(cache_dir=cache_dir)
    
    # Initialize with offline configuration
    config = {
        "offline": True,
        "use_packages": True,
        "allow_catalogs": True
    }
    
    try:
        arelle_service.initialize(config)
        print("✓ Arelle service initialized successfully")
        
        # Load taxonomy packages
        package_paths = [
            str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "rf-unpacked"),
            str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "dict-unpacked"),
            str(PROJECT_ROOT / "backend" / "github_work" / "taxonomies" / "eba" / "rf40" / "severity-unpacked")
        ]
        
        arelle_service.load_taxonomy_packages(package_paths)
        print("✓ Taxonomy packages loaded successfully")
        
        # Test with sample instance
        sample_file = PROJECT_ROOT / "test-samples" / "v4.0" / "base" / "base_corep_comprehensive.xbrl"
        
        if sample_file.exists():
            print(f"\nTesting with sample file: {sample_file}")
            
            model_xbrl, facts_count = arelle_service.load_instance(str(sample_file))
            
            if model_xbrl:
                print(f"✓ Loaded instance successfully with {facts_count} facts")
                
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
                print("✗ Failed to load instance")
        else:
            print(f"Sample file not found: {sample_file}")
            
    except Exception as e:
        print(f"Error testing modified service: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_modified_service()
