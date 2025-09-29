"""
DPM Detection Service

Detects DPM version (1.0 vs 2.0) based on DTS namespaces and architecture.
Uses heuristics on DTS URLs for arch v2.0 markers.
"""

import logging
from typing import Dict, Any, Optional, List
import xml.etree.ElementTree as ET
from pathlib import Path

logger = logging.getLogger(__name__)

class DMPDetectionService:
    """Service for detecting DPM version from XBRL instances."""
    
    def __init__(self):
        """Initialize DMP detection service."""
        logger.info("Initializing DMPDetectionService")
        
        # DPM 2.0 architecture markers as specified in development plan
        self.dpm_20_markers = ['/dict/', '/tab/', '/mod/', '/val/']
        self.eba_owner_pattern = 'eba.europa.eu'
    
    def detect_dmp_version(self, file_path: str) -> Dict[str, Any]:
        """
        Detect DPM version from XBRL instance.
        
        Uses heuristic on DTS URLs for arch v2.0 markers 
        (/dict/, /tab/, /mod/, /val/) under EBA owner; else 1.0.
        
        Args:
            file_path: Path to XBRL instance file
            
        Returns:
            Dictionary with DPM detection results
        """
        try:
            logger.info(f"Detecting DPM version for: {file_path}")
            
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Parse XML to extract DTS URLs
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extract schema references and linkbase references
            dts_urls = self._extract_dts_urls(root)
            
            if not dts_urls:
                logger.warning("No DTS URLs found in instance")
                return {
                    "dmp_version": "unknown",
                    "confidence": "low",
                    "reason": "No DTS URLs found",
                    "dts_urls": []
                }
            
            # Check for DPM 2.0 markers
            dmp_20_evidence = []
            eba_urls = []
            
            for url in dts_urls:
                if self.eba_owner_pattern in url:
                    eba_urls.append(url)
                    
                    # Check for DPM 2.0 architecture markers
                    for marker in self.dmp_20_markers:
                        if marker in url:
                            dmp_20_evidence.append({
                                "url": url,
                                "marker": marker
                            })
            
            # Determine DPM version based on evidence
            if dmp_20_evidence:
                dmp_version = "2.0"
                confidence = "high"
                reason = f"Found DPM 2.0 markers: {[e['marker'] for e in dmp_20_evidence]}"
            elif eba_urls:
                dmp_version = "1.0"
                confidence = "medium"
                reason = "EBA URLs found but no DPM 2.0 architecture markers"
            else:
                dmp_version = "1.0"
                confidence = "low"
                reason = "No EBA URLs found, defaulting to DPM 1.0"
            
            result = {
                "dmp_version": dmp_version,
                "confidence": confidence,
                "reason": reason,
                "dts_urls": dts_urls,
                "eba_urls": eba_urls,
                "dmp_20_evidence": dmp_20_evidence
            }
            
            logger.info(f"DPM detection result: {dmp_version} (confidence: {confidence})")
            return result
            
        except Exception as e:
            logger.error(f"DPM detection failed: {e}")
            return {
                "dmp_version": "unknown",
                "confidence": "error",
                "reason": str(e),
                "dts_urls": []
            }
    
    def _extract_dts_urls(self, root: ET.Element) -> List[str]:
        """
        Extract DTS URLs from XBRL instance.
        
        Looks for schemaRef and linkbaseRef elements.
        
        Args:
            root: Root XML element
            
        Returns:
            List of DTS URLs
        """
        urls = []
        
        # Define namespaces
        namespaces = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink'
        }
        
        # Find schemaRef elements
        for ns_prefix, ns_uri in namespaces.items():
            schema_refs = root.findall(f'.//{{{ns_uri}}}schemaRef')
            for elem in schema_refs:
                href = elem.get('{http://www.w3.org/1999/xlink}href')
                if href:
                    urls.append(href)
            
            # Find linkbaseRef elements
            linkbase_refs = root.findall(f'.//{{{ns_uri}}}linkbaseRef')
            for elem in linkbase_refs:
                href = elem.get('{http://www.w3.org/1999/xlink}href')
                if href:
                    urls.append(href)
        
        # Fallback: check without namespace prefix
        if not urls:
            for elem in root.iter():
                if elem.tag.endswith('schemaRef') or elem.tag.endswith('linkbaseRef'):
                    href = elem.get('{http://www.w3.org/1999/xlink}href')
                    if href:
                        urls.append(href)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls
