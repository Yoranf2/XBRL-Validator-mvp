"""
Integration tests for concept resolution parity.

Tests that dictionary schemas (met.xsd) are properly loaded and eba_met:* concepts
are resolvable in RF 4.0 instances, ensuring offline catalog resolution works correctly.
"""

import pytest
import logging
from pathlib import Path
from typing import Dict, Any, List
import json

# Add backend to path for imports
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.services.arelle_service import ArelleService
from app.utils.config_loader import load_config

logger = logging.getLogger(__name__)

class TestConceptResolutionParity:
    """Test concept resolution parity for RF 4.0 instances."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        self.project_root = Path(__file__).resolve().parents[3]
        self.test_samples_dir = self.project_root / "test-samples"
        self.backend_dir = self.project_root / "backend"
        
        # Load configuration
        self.config = load_config(self.backend_dir / "config" / "app.yaml")
        self.taxonomy_config = load_config(self.backend_dir / "config" / "eba_taxonomies.yaml")
        
        # Initialize Arelle service
        self.arelle_service = ArelleService()
        self.arelle_service.initialize(self.config)
        
        # Load taxonomy packages - convert relative paths to absolute
        rf40_packages = self.taxonomy_config["eba"]["rf40"]["packages"]
        # Convert relative paths to absolute paths from project root
        absolute_packages = []
        for package_path in rf40_packages:
            if package_path.startswith('./backend/'):
                # Remove ./backend/ prefix and make absolute from project root
                relative_path = package_path[10:]  # Remove './backend/'
                absolute_path = str(self.project_root / "backend" / relative_path)
            else:
                absolute_path = str(self.project_root / package_path)
            absolute_packages.append(absolute_path)
        
        self.arelle_service.load_taxonomy_packages(absolute_packages)
        
        logger.info("Test setup completed")
    
    def test_rf40_corep_concept_resolution(self):
        """Test that COREP RF 4.0 instances can resolve eba_met:* concepts."""
        # Use the base COREP comprehensive test file
        test_file = self.test_samples_dir / "v4.0" / "base" / "base_corep_comprehensive.xbrl"
        
        if not test_file.exists():
            pytest.skip(f"Test file not found: {test_file}")
        
        logger.info(f"Testing concept resolution with: {test_file}")
        
        # Load instance
        model_xbrl, facts_count = self.arelle_service.load_instance(str(test_file))
        
        assert model_xbrl is not None, "Failed to load XBRL instance"
        
        # Check that eba_met concepts are available
        eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
        eba_met_concepts = [
            concept for qname, concept in model_xbrl.qnameConcepts.items()
            if qname.namespaceURI == eba_met_ns
        ]
        
        assert len(eba_met_concepts) > 0, f"Expected eba_met concepts to be available, found {len(eba_met_concepts)}"
        logger.info(f"Found {len(eba_met_concepts)} eba_met concepts in DTS")
        
        # Verify specific eba_met concepts that should be available
        expected_concepts = [
            "eba_met:qCCB",  # Common concepts that appear in COREP
            "eba_met:qAOJ",  # Another common concept
        ]
        
        available_qnames = {str(qname) for qname in model_xbrl.qnameConcepts.keys()}
        found_concepts = [concept for concept in expected_concepts if concept in available_qnames]
        
        logger.info(f"Found expected concepts: {found_concepts}")
        # At least one expected concept should be available
        assert len(found_concepts) > 0, f"Expected at least one of {expected_concepts} to be available"
        
        # Verify offline mode was maintained
        offline_status = self.arelle_service.get_offline_status()
        assert offline_status["offline_mode"], "Offline mode should be maintained"
        assert len(offline_status["http_fetch_attempts"]) == 0, f"HTTP fetch attempts detected: {offline_status['http_fetch_attempts']}"
        
        logger.info("COREP concept resolution test passed")
    
    def test_rf40_mica_concept_resolution(self):
        """Test that MICA RF 4.0 instances can resolve eba_met:* concepts."""
        # Look for MICA test files
        mica_files = list(self.test_samples_dir.glob("**/*MICA*.xbrl"))
        
        if not mica_files:
            pytest.skip("No MICA test files found")
        
        # Use the first MICA file found
        test_file = mica_files[0]
        logger.info(f"Testing MICA concept resolution with: {test_file}")
        
        # Load instance
        model_xbrl, facts_count = self.arelle_service.load_instance(str(test_file))
        
        assert model_xbrl is not None, "Failed to load MICA XBRL instance"
        logger.info(f"Loaded MICA instance with {facts_count} facts")
        
        # Check that eba_met concepts are available
        eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
        eba_met_concepts = [
            concept for qname, concept in model_xbrl.qnameConcepts.items()
            if qname.namespaceURI == eba_met_ns
        ]
        
        assert len(eba_met_concepts) > 0, f"Expected eba_met concepts to be available in MICA, found {len(eba_met_concepts)}"
        logger.info(f"Found {len(eba_met_concepts)} eba_met concepts in MICA DTS")
        
        # Verify offline mode was maintained
        offline_status = self.arelle_service.get_offline_status()
        assert offline_status["offline_mode"], "Offline mode should be maintained"
        assert len(offline_status["http_fetch_attempts"]) == 0, f"HTTP fetch attempts detected: {offline_status['http_fetch_attempts']}"
        
        logger.info("MICA concept resolution test passed")
    
    def test_dictionary_schema_presence_in_dts(self):
        """Test that met.xsd schema is present in the DTS after loading."""
        # Use a simple test file
        test_file = self.test_samples_dir / "v4.0" / "base" / "base_corep_comprehensive.xbrl"
        
        if not test_file.exists():
            pytest.skip(f"Test file not found: {test_file}")
        
        logger.info(f"Testing DTS schema presence with: {test_file}")
        
        # Load instance
        model_xbrl, facts_count = self.arelle_service.load_instance(str(test_file))
        
        assert model_xbrl is not None, "Failed to load XBRL instance"
        
        # Check that met.xsd is in the DTS
        dts_models = getattr(model_xbrl, 'modelDocument', None)
        if dts_models:
            # Get all schema documents in DTS
            schema_docs = []
            if hasattr(dts_models, 'referencedDocuments'):
                for doc in dts_models.referencedDocuments:
                    if hasattr(doc, 'uri') and 'met.xsd' in str(doc.uri):
                        schema_docs.append(str(doc.uri))
            
            # Also check modelManager for loaded schemas
            if hasattr(model_xbrl, 'modelManager') and hasattr(model_xbrl.modelManager, 'urlDocs'):
                for url, doc in model_xbrl.modelManager.urlDocs.items():
                    if 'met.xsd' in str(url):
                        schema_docs.append(str(url))
            
            logger.info(f"Found met.xsd references in DTS: {schema_docs}")
            assert len(schema_docs) > 0, "Expected met.xsd to be present in DTS"
        
        # Verify eba_met concepts are available (this confirms met.xsd was loaded)
        eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
        eba_met_concepts = [
            concept for qname, concept in model_xbrl.qnameConcepts.items()
            if qname.namespaceURI == eba_met_ns
        ]
        
        assert len(eba_met_concepts) > 0, f"Expected eba_met concepts to be available, found {len(eba_met_concepts)}"
        logger.info(f"Found {len(eba_met_concepts)} eba_met concepts, confirming met.xsd is loaded")
        
        logger.info("DTS schema presence test passed")
    
    def test_catalog_resolution_coverage(self):
        """Test that catalog resolution covers all required dictionary schemas."""
        # Check catalog map coverage
        offline_status = self.arelle_service.get_offline_status()
        catalog_mappings_count = offline_status["catalog_mappings_count"]
        
        assert catalog_mappings_count > 0, "Expected catalog mappings to be available"
        logger.info(f"Catalog has {catalog_mappings_count} URL mappings")
        
        # Test resolution of key dictionary URLs
        test_urls = [
            "http://www.eba.europa.eu/xbrl/crr/dict/met/met.xsd",
            "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
        ]
        
        resolved_count = 0
        for url in test_urls:
            # Access the private method for testing
            local_path = self.arelle_service._resolve_dict_url(url)
            if local_path and Path(local_path).exists():
                resolved_count += 1
                logger.info(f"Successfully resolved: {url} -> {local_path}")
            else:
                logger.warning(f"Could not resolve: {url}")
        
        assert resolved_count > 0, f"Expected at least one dictionary URL to resolve, resolved {resolved_count}"
        logger.info(f"Catalog resolution test passed: {resolved_count}/{len(test_urls)} URLs resolved")
    
    def test_validation_with_dictionary_concepts(self):
        """Test that validation works correctly with dictionary concepts loaded."""
        # Use a test file that should have validation rules referencing eba_met concepts
        test_file = self.test_samples_dir / "v4.0" / "base" / "base_corep_comprehensive.xbrl"
        
        if not test_file.exists():
            pytest.skip(f"Test file not found: {test_file}")
        
        logger.info(f"Testing validation with dictionary concepts: {test_file}")
        
        # Load instance
        model_xbrl, facts_count = self.arelle_service.load_instance(str(test_file))
        
        assert model_xbrl is not None, "Failed to load XBRL instance"
        
        # Run validation
        validation_results = self.arelle_service.validate_instance(model_xbrl, profile="fast")
        
        assert validation_results["status"] in ["success", "failed"], f"Unexpected validation status: {validation_results['status']}"
        assert "errors" in validation_results, "Validation results should include errors"
        assert "warnings" in validation_results, "Validation results should include warnings"
        assert "facts_count" in validation_results, "Validation results should include facts_count"
        
        logger.info(f"Validation completed: {validation_results['status']}, "
                   f"{len(validation_results['errors'])} errors, "
                   f"{len(validation_results['warnings'])} warnings, "
                   f"{validation_results['facts_count']} facts")
        
        # Verify that validation didn't fail due to missing dictionary concepts
        concept_errors = [
            error for error in validation_results["errors"]
            if "eba_met" in str(error.get("message", "")) and "undefined" in str(error.get("message", ""))
        ]
        
        assert len(concept_errors) == 0, f"Found concept resolution errors: {concept_errors}"
        
        logger.info("Validation with dictionary concepts test passed")
    
    def test_parity_between_instances(self):
        """Fast vs Full parity on fixed sample: offline, met.xsd present, ≥1 v-code in full, and differing behavior."""
        fixed_file = Path("/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_COREP040000_COREPLR_2024-12-31_20241211134749200.xbrl")
        if not fixed_file.exists():
            pytest.skip(f"Fixed sample not found: {fixed_file}")

        # Load instance
        model_xbrl, facts_count = self.arelle_service.load_instance(str(fixed_file))
        assert model_xbrl is not None, "Failed to load XBRL instance"

        # Run fast and full
        res_fast = self.arelle_service.validate_instance(model_xbrl, profile="fast")
        # Re-load for clean state
        model_xbrl_full, _ = self.arelle_service.load_instance(str(fixed_file))
        res_full = self.arelle_service.validate_instance(model_xbrl_full, profile="full")

        # Assertions
        offline_status = self.arelle_service.get_offline_status()
        assert offline_status["http_fetch_attempts"] == [], f"HTTP attempts recorded: {offline_status['http_fetch_attempts']}"

        assert res_full.get("dts_evidence", {}).get("met_xsd_present", False) is True, "met.xsd should be present"

        # v-code presence in full: from codes or from message text
        top_codes = (res_full.get("metrics", {}) or {}).get("top_error_codes", [])
        has_v = any(str(c.get("code",""))[:9] == "message:v" for c in top_codes)
        if not has_v:
            # Fallback: check raw errors/warnings for message:v pattern
            import re
            vpat = re.compile(r"\bmessage:(v\d+_[a-z]_?\d*)\b", re.IGNORECASE)
            msgs = res_full.get("errors", []) + res_full.get("warnings", [])
            has_v = any(vpat.search(str(m.get("message",""))) for m in msgs)
        assert has_v, f"Expected at least one v-code in full; top_codes={top_codes}"

        # Fast vs full should differ (e.g., error counts)
        assert len(res_fast.get("errors", [])) != len(res_full.get("errors", [])), "Fast vs Full should differ in errors"
    
    def teardown_method(self):
        """Cleanup after each test."""
        if hasattr(self, 'arelle_service'):
            # Check final offline status
            offline_status = self.arelle_service.get_offline_status()
            logger.info(f"Final offline status: {offline_status}")
            
            # Ensure no HTTP fetch attempts occurred
            if offline_status["http_fetch_attempts"]:
                logger.warning(f"HTTP fetch attempts detected: {offline_status['http_fetch_attempts']}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
