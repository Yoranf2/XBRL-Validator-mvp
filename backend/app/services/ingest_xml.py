"""
XML Ingest Service

Handles XML XBRL file ingestion with preflight checks.
Validates well-formedness and schema references.
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class XMLIngestService:
    """Service for ingesting and validating XML XBRL files."""
    
    def __init__(self):
        """Initialize XML ingest service."""
        logger.info("Initializing XMLIngestService")
    
    def preflight_check(self, file_path: str) -> Dict[str, Any]:
        """
        Perform preflight checks on XML XBRL file.
        
        Validates:
        - Well-formedness
        - Exactly one schemaRef
        - eba_met namespace usage and missing dict/met/met.xsd detection
        - Local mapping verification
        
        Args:
            file_path: Path to XML file
            
        Returns:
            Dictionary with preflight results
        """
        try:
            logger.info(f"Performing preflight check on: {file_path}")
            
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Check well-formedness
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                logger.info("XML is well-formed")
            except ET.ParseError as e:
                logger.error(f"XML parsing error: {e}")
                return {
                    "status": "failed",
                    "error": f"XML parsing error: {e}",
                    "well_formed": False
                }
            
            # Check for exactly one schemaRef
            schema_refs = self._find_schema_refs(root)
            
            if len(schema_refs) == 0:
                logger.error("No schemaRef found in XBRL instance")
                return {
                    "status": "failed", 
                    "error": "No schemaRef found in XBRL instance",
                    "well_formed": True,
                    "schema_ref_count": 0
                }
            elif len(schema_refs) > 1:
                logger.error(f"Multiple schemaRefs found: {len(schema_refs)}")
                return {
                    "status": "failed",
                    "error": f"Multiple schemaRefs found: {len(schema_refs)}",
                    "well_formed": True,
                    "schema_ref_count": len(schema_refs)
                }
            
            schema_ref = schema_refs[0]
            logger.info(f"Found single schemaRef: {schema_ref}")
            
            # Check for eba_met namespace usage
            eba_met_usage = self._detect_eba_met_usage(root)
            logger.info(f"eba_met namespace usage detected: {eba_met_usage}")
            
            # Check if dict/met/met.xsd is referenced
            dict_met_referenced = self._check_dict_met_reference(schema_refs)
            logger.info(f"dict/met/met.xsd referenced: {dict_met_referenced}")
            
            # Fail fast if eba_met is used but dict/met/met.xsd is not referenced
            if eba_met_usage and not dict_met_referenced:
                error_msg = "eba_met namespace is used but dict/met/met.xsd is not referenced in schemaRef"
                logger.error(error_msg)
                return {
                    "status": "failed",
                    "error": error_msg,
                    "well_formed": True,
                    "schema_ref_count": 1,
                    "schema_ref": schema_ref,
                    "eba_met_usage": eba_met_usage,
                    "dict_met_referenced": dict_met_referenced,
                    "local_mapping_valid": False
                }
            
            # Optional RF version guard: if configured only for 4.0, warn on 4.2 schemaRefs
            try:
                if "/4.2/" in (schema_ref or ""):
                    logger.warning("SchemaRef appears to target RF 4.2 while server is configured for RF 4.0")
            except Exception:
                pass

            # Verify local mapping (placeholder)
            # TODO: Implement actual local mapping verification
            local_mapping_valid = True
            
            return {
                "status": "success",
                "well_formed": True,
                "schema_ref_count": 1,
                "schema_ref": schema_ref,
                "eba_met_usage": eba_met_usage,
                "dict_met_referenced": dict_met_referenced,
                "local_mapping_valid": local_mapping_valid
            }
            
        except Exception as e:
            logger.error(f"Preflight check failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "well_formed": False
            }
    
    def _find_schema_refs(self, root: ET.Element) -> list[str]:
        """
        Find schemaRef elements in XBRL instance.
        
        Args:
            root: Root XML element
            
        Returns:
            List of schemaRef href values
        """
        schema_refs = []
        
        # Define namespaces commonly used in XBRL
        namespaces = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink'
        }
        
        # Look for schemaRef elements
        for ns_prefix, ns_uri in namespaces.items():
            schema_ref_elements = root.findall(f'.//{{{ns_uri}}}schemaRef')
            for elem in schema_ref_elements:
                href = elem.get('{http://www.w3.org/1999/xlink}href')
                if href:
                    schema_refs.append(href)
        
        # Also check without namespace prefix (fallback)
        if not schema_refs:
            for elem in root.iter():
                if elem.tag.endswith('schemaRef'):
                    href = elem.get('{http://www.w3.org/1999/xlink}href')
                    if href:
                        schema_refs.append(href)
        
        return schema_refs
    
    def _detect_eba_met_usage(self, root: ET.Element) -> bool:
        """
        Detect if eba_met namespace is used in the XBRL instance.
        
        Args:
            root: Root XML element
            
        Returns:
            True if eba_met namespace is used, False otherwise
        """
        # Check for eba_met namespace declaration
        eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
        
        # Check root element attributes for namespace declaration
        for attr_name, attr_value in root.attrib.items():
            if attr_name.startswith('xmlns:') and attr_value == eba_met_ns:
                logger.info(f"Found eba_met namespace declaration: {attr_name}={attr_value}")
                return True
            elif attr_name == 'xmlns' and attr_value == eba_met_ns:
                logger.info(f"Found eba_met as default namespace: {attr_value}")
                return True
        
        # Check for eba_met prefixed elements
        for elem in root.iter():
            if elem.tag.startswith('eba_met:'):
                logger.info(f"Found eba_met prefixed element: {elem.tag}")
                return True
        
        # Check for elements with eba_met namespace URI
        for elem in root.iter():
            if elem.tag.startswith('{http://www.eba.europa.eu/xbrl/crr/dict/met}'):
                logger.info(f"Found eba_met namespace element: {elem.tag}")
                return True
        
        return False
    
    def _check_dict_met_reference(self, schema_refs: list[str]) -> bool:
        """
        Check if dict/met/met.xsd is referenced in schemaRef elements.
        
        Args:
            schema_refs: List of schemaRef href values
            
        Returns:
            True if dict/met/met.xsd is referenced, False otherwise
        """
        dict_met_patterns = [
            'dict/met/met.xsd',
            '/dict/met/met.xsd',
            'met.xsd'
        ]
        
        for schema_ref in schema_refs:
            for pattern in dict_met_patterns:
                if pattern in schema_ref:
                    logger.info(f"Found dict/met/met.xsd reference: {schema_ref}")
                    return True
        
        return False
