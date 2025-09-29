"""
Validation Models

Pydantic models for validation requests, responses, and related data structures.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class ValidationRequest(BaseModel):
    """Request model for XBRL validation."""
    
    file_path: Optional[str]
    profile: Optional[str] = "fast"
    entrypoint: Optional[str] = Field(None, description="Specific entrypoint to use")
    
class ValidationError(BaseModel):
    """Model for validation errors."""
    
    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    severity: str = Field(..., description="Error severity (error/warning)")
    location: Optional[str] = Field(None, description="Location in file where error occurred")
    # Extended fields for enriched UX
    rule_id: Optional[str] = Field(None, description="Canonical rule identifier (e.g., v0704_m_1)")
    category: Optional[str] = Field(None, description="Derived category (formulas, dimensions, calculation, xbrl21)")
    table_id: Optional[str] = Field(None, description="Resolved table id (e.g., C_43.00.c)")
    rowLabel: Optional[str] = Field(None, description="Resolved row header label")
    colLabel: Optional[str] = Field(None, description="Resolved column header label")
    rowCode: Optional[str] = Field(None, description="Resolved row code (unpadded)")
    colCode: Optional[str] = Field(None, description="Resolved column code (unpadded)")
    qualifiers: Optional[List[Dict[str, Any]]] = Field(None, description="Effective dimension qualifiers for the cell")
    readable_message: Optional[str] = Field(None, description="User-friendly transformed message")
    # Stable identifier fields for deep linking, baselines, and de-duplication
    id: Optional[str] = Field(None, description="Stable, short identifier for this finding")
    id_full: Optional[str] = Field(None, description="Full hash used to derive id (debug)")
    canonical_key: Optional[Dict[str, Any]] = Field(None, description="Canonicalized key used to compute the id")
    
class ValidationResponse(BaseModel):
    """Response model for XBRL validation."""
    
    status: str = Field(..., description="Validation status (success/failed)")
    trace_id: str = Field(..., description="Unique trace identifier")
    run_id: str = Field(..., description="Unique run identifier")
    duration_ms: int = Field(..., description="Validation duration in milliseconds")
    facts_count: int = Field(..., description="Number of facts in instance")
    dpm_version: str = Field(..., description="Detected DPM version")
    is_csv: bool = Field(..., description="Whether instance is CSV format")
    errors: List[ValidationError] = Field(default_factory=list, description="Validation errors")
    warnings: List[ValidationError] = Field(default_factory=list, description="Validation warnings")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    dts_evidence: Optional[Dict[str, Any]] = Field(None, description="DTS evidence including met.xsd presence and concept counts")
    metrics: Optional[Dict[str, Any]] = Field(None, description="Enhanced metrics including undefinedFacts, contexts, units")
    tables_index_url: Optional[str] = Field(None, description="URL to rendered tableset index for this run")

class EntrypointInfo(BaseModel):
    """Model for taxonomy entrypoint information."""
    
    id: str = Field(..., description="Entrypoint identifier")
    label: str = Field(..., description="Human-readable label")
    xsd: str = Field(..., description="XSD schema URL")

class TaxonomyInfo(BaseModel):
    """Model for taxonomy information."""
    
    id: str = Field(..., description="Taxonomy identifier")
    label: str = Field(..., description="Human-readable label")
    version: str = Field(..., description="Taxonomy version")
    entrypoints: List[EntrypointInfo] = Field(..., description="Available entrypoints")

class HealthResponse(BaseModel):
    """Response model for health check."""
    
    status: str = Field(..., description="Service status")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    arelle_version: str = Field(..., description="Arelle library version")
    offline_mode: bool = Field(..., description="Whether running in offline mode")
    timestamp: datetime = Field(default_factory=datetime.now, description="Health check timestamp")

class ProfileInfo(BaseModel):
    """Model for validation profile information."""
    
    name: str = Field(..., description="Profile name")
    formulas: bool = Field(..., description="Whether formulas are enabled")
    csv_constraints: bool = Field(..., description="Whether CSV constraints are enabled")
    trace: bool = Field(..., description="Whether tracing is enabled")
    description: Optional[str] = Field(None, description="Profile description")

class PreflightResponse(BaseModel):
    passed: int
    failed: int
    items: List[Dict[str, Any]]
    offline_status: Dict[str, Any]
    duration_ms: int
    dts_evidence: Optional[Dict[str, Any]] = None
