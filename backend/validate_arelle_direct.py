#!/usr/bin/env python3
"""
Direct Arelle validation harness for EBA XBRL instances.

This script runs validations directly in Arelle (outside our service) and captures
all validation messages (errors, warnings, info) for analysis.
"""

import sys
import pathlib
from typing import List, Dict, Any

# Add Arelle to path
sys.path.insert(0, '/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/third_party/arelle')

from arelle import Cntlr, FileSource, PackageManager, Validate
from arelle.logging.handlers.LogToBufferHandler import LogToBufferHandler
from arelle.ModelManager import ModelManager

# RF 4.0 Taxonomy package paths
ZIP_PATHS = [
    '/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0.zip',
    '/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Dictionary_4.0.0.0.zip',
    '/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Severity_4.0.0.0.zip',
]

# Schema URLs to inject
MET_SCHEMA_URLS = [
    'http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd',
]

# Validation schema URLs to inject (for COREP LR)
VALIDATION_SCHEMA_URLS = [
    'http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/val/corep_lr-val.xsd',
]

def inject_met_schema_refs(xml_text: str, urls: List[str]) -> str:
    """
    Inject met.xsd schemaRef elements into XML content.
    
    Args:
        xml_text: Original XML content
        urls: List of schema URLs to inject
        
    Returns:
        Modified XML content with injected schemaRefs
    """
    marker = '<link:schemaRef xlink:type="simple" xlink:href="'
    pos = xml_text.find(marker)
    if pos < 0:
        return xml_text
    
    # Find indentation from existing schemaRef
    line_start = xml_text.rfind('\n', 0, pos) + 1
    indent = ''.join(ch for ch in xml_text[line_start:pos] if ch in (' ', '\t'))
    
    # Build injection content
    injection = ''.join(f'{indent}<link:schemaRef xlink:type="simple" xlink:href="{url}"/>\n' for url in urls)
    
    return xml_text[:pos] + injection + xml_text[pos:]

def setup_arelle_controller() -> Cntlr:
    """Set up Arelle controller with offline configuration."""
    # Use in-memory buffer log handler
    cntlr = Cntlr.Cntlr(logFileName="logToBuffer")
    
    # Configure offline mode
    cntlr.webCache.workOffline = True
    cntlr.internetConnectivity = 'offline'
    
    # Configure additional offline settings
    if hasattr(cntlr, 'config'):
        cntlr.config['internetConnectivity'] = 'offline'
        cntlr.config['workOffline'] = True
        cntlr.config['allow_catalogs'] = True
    
    # Initialize formula options; default to run (can be tuned later if needed)
    try:
        from arelle.ModelFormulaObject import FormulaOptions
        cntlr.modelManager.formulaOptions = FormulaOptions()
        cntlr.modelManager.formulaOptions.formulaAction = 'run'
        cntlr.modelManager.formulaOptions.traceUnsatisfiedAssertions = True
        cntlr.modelManager.formulaOptions.traceUnmessagedUnsatisfiedAssertions = True
        cntlr.modelManager.formulaOptions.errorUnsatisfiedAssertions = False
    except Exception:
        pass

    # Always load custom transforms before validation
    try:
        cntlr.modelManager.loadCustomTransforms()
    except Exception:
        pass
    
    # Ensure log handler is LogToBufferHandler
    if not isinstance(cntlr.logHandler, LogToBufferHandler):
        cntlr.logHandler = LogToBufferHandler()

    return cntlr

def load_taxonomy_packages(cntlr: Cntlr) -> bool:
    """
    Load RF 4.0 taxonomy packages and rebuild remappings.
    
    Args:
        cntlr: Arelle controller
        
    Returns:
        True if packages loaded successfully
    """
    print("Loading RF 4.0 taxonomy packages...")
    
    for zip_path in ZIP_PATHS:
        if not pathlib.Path(zip_path).exists():
            print(f"ERROR: Package not found: {zip_path}")
            return False
        
        try:
            result = PackageManager.addPackage(cntlr, zip_path)
            if result:
                print(f"✓ Loaded: {pathlib.Path(zip_path).name}")
            else:
                print(f"✗ Failed to load: {pathlib.Path(zip_path).name}")
                return False
        except Exception as e:
            print(f"✗ Error loading {zip_path}: {e}")
            return False
    
    # Rebuild remappings
    try:
        PackageManager.rebuildRemappings(cntlr)
        print(f"✓ Rebuilt PackageManager remappings")
        return True
    except Exception as e:
        print(f"✗ Failed to rebuild remappings: {e}")
        return False

def _parse_buffer_json(json_text: str) -> List[Dict[str, Any]]:
    """Parse JSON produced by LogToXmlHandler.getJson() into list of entries."""
    import json
    try:
        parsed = json.loads(json_text)
        entries = parsed.get("log", []) if isinstance(parsed, dict) else []
        # Normalize fields
        normalized = []
        for e in entries:
            normalized.append({
                'code': e.get('code') or '',
                'level': (e.get('level') or '').lower(),
                'message': (e.get('message') or {}).get('text') if isinstance(e.get('message'), dict) else e.get('message'),
                'refs': e.get('refs') or []
            })
        return normalized
    except Exception:
        return []

def collect_validation_messages(model_xbrl, cntlr=None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Collect all validation messages from the model.
    
    Args:
        model_xbrl: Loaded ModelXbrl instance
        
    Returns:
        Dictionary with errors, warnings, and info messages
    """
    messages = {
        'errors': [],
        'warnings': [],
        'info': []
    }
    
    # Prefer buffered log messages if controller and buffer are available
    if cntlr is not None and isinstance(cntlr.logHandler, LogToBufferHandler):
        try:
            # Retrieve JSON without clearing first to parse; then clear
            buffer_json = cntlr.logHandler.getJson(clearLogBuffer=False)
            buffered = _parse_buffer_json(buffer_json)
            # Clear buffer now
            cntlr.logHandler.clearLogBuffer()
            for entry in buffered:
                level = entry.get('level')
                target = 'info'
                if level in ('error', 'critical', 'fatal'):
                    target = 'errors'
                elif level in ('warning', 'warn'):
                    target = 'warnings'
                messages[target].append({
                    'code': entry.get('code') or 'arelle',
                    'message': entry.get('message') or '',
                    'severity': target[:-1] if target != 'warnings' else 'warning',
                    'type': 'ArelleLog'
                })
        except Exception:
            # Fall back to model_xbrl collections below
            pass

    # Collect errors
    if hasattr(model_xbrl, 'errors') and model_xbrl.errors:
        try:
            for error in model_xbrl.errors:
                messages['errors'].append({
                    'code': getattr(error, 'messageCode', 'unknown'),
                    'message': str(error),
                    'severity': 'error',
                    'type': type(error).__name__
                })
        except Exception as e:
            messages['errors'].append({
                'code': 'error_collection_failed',
                'message': f"Could not collect errors: {e}",
                'severity': 'error',
                'type': 'CollectionError'
            })
    
    # Collect warnings
    if hasattr(model_xbrl, 'warnings') and model_xbrl.warnings:
        try:
            for warning in model_xbrl.warnings:
                messages['warnings'].append({
                    'code': getattr(warning, 'messageCode', 'unknown'),
                    'message': str(warning),
                    'severity': 'warning',
                    'type': type(warning).__name__
                })
        except Exception as e:
            messages['warnings'].append({
                'code': 'warning_collection_failed',
                'message': f"Could not collect warnings: {e}",
                'severity': 'warning',
                'type': 'CollectionError'
            })
    
    # Collect info messages (if available)
    if hasattr(model_xbrl, 'info') and model_xbrl.info:
        try:
            if callable(model_xbrl.info):
                # info is a method, try to get the log entries
                from arelle import Cntlr
                cntlr = Cntlr.Cntlr(logFileName=None)
                log_entries = cntlr.logEntries if hasattr(cntlr, 'logEntries') else []
                for entry in log_entries:
                    messages['info'].append({
                        'code': getattr(entry, 'messageCode', 'unknown'),
                        'message': str(entry),
                        'severity': 'info',
                        'type': type(entry).__name__
                    })
            else:
                for info in model_xbrl.info:
                    messages['info'].append({
                        'code': getattr(info, 'messageCode', 'unknown'),
                        'message': str(info),
                        'severity': 'info',
                        'type': type(info).__name__
                    })
        except Exception as e:
            messages['info'].append({
                'code': 'info_collection_failed',
                'message': f"Could not collect info messages: {e}",
                'severity': 'info',
                'type': 'CollectionError'
            })
    
    # Also check for formula-specific validation messages
    try:
        if hasattr(model_xbrl, 'formulaOptions') and hasattr(model_xbrl.formulaOptions, 'formulaAction'):
            print(f"Formula action: {model_xbrl.formulaOptions.formulaAction}")
        
        # Check if there are any unsatisfied assertions
        if hasattr(model_xbrl, 'unsatisfiedAssertions'):
            unsatisfied = model_xbrl.unsatisfiedAssertions
            print(f"Unsatisfied assertions: {len(unsatisfied) if unsatisfied else 0}")
            if unsatisfied:
                for assertion in unsatisfied:
                    if 'v23175_s_20' in str(assertion):
                        print(f"✓ Found unsatisfied v23175_s_20: {assertion}")
                        messages['errors'].append({
                            'code': 'v23175_s_20',
                            'message': str(assertion),
                            'severity': 'error',
                            'type': 'UnsatisfiedAssertion'
                        })
    except Exception as e:
        print(f"Error checking formula results: {e}")
    
    return messages

def validate_instance(instance_path: str) -> Dict[str, Any]:
    """
    Validate an XBRL instance using direct Arelle API.
    
    Args:
        instance_path: Path to XBRL instance file
        
    Returns:
        Dictionary with validation results and messages
    """
    print(f"\n=== Validating: {pathlib.Path(instance_path).name} ===")
    
    # Check if file exists
    if not pathlib.Path(instance_path).exists():
        return {
            'success': False,
            'error': f"File not found: {instance_path}",
            'messages': {'errors': [], 'warnings': [], 'info': []}
        }
    
    cntlr = None
    model_xbrl = None
    
    try:
        # Set up Arelle controller
        cntlr = setup_arelle_controller()

        # Clear any pre-existing buffer messages
        if isinstance(cntlr.logHandler, LogToBufferHandler):
            cntlr.logHandler.clearLogBuffer()
        
        # Load taxonomy packages
        if not load_taxonomy_packages(cntlr):
            return {
                'success': False,
                'error': "Failed to load taxonomy packages",
                'messages': {'errors': [], 'warnings': [], 'info': []}
            }
        
        # Check if schemaRefs need to be injected
        original_path = instance_path
        xml_content = pathlib.Path(instance_path).read_text(encoding='utf-8')
        injected_content = xml_content
        
        # Inject met.xsd schemaRef if needed
        if 'dict/met/met.xsd' not in xml_content:
            print("Injecting met.xsd schemaRef...")
            injected_content = inject_met_schema_refs(injected_content, MET_SCHEMA_URLS)
        
        # Inject validation schemaRef if needed (for COREP files)
        if 'corep_lr-val.xsd' not in injected_content and 'corep' in instance_path.lower():
            print("Injecting validation schemaRef...")
            injected_content = inject_met_schema_refs(injected_content, VALIDATION_SCHEMA_URLS)
        
        if injected_content != xml_content:
            # Write injected content to temp file
            temp_path = pathlib.Path(instance_path).with_suffix('.injected.xbrl')
            temp_path.write_text(injected_content, encoding='utf-8')
            instance_path = str(temp_path)
            print(f"✓ Created injected file: {temp_path.name}")
        
        # Load instance
        print("Loading XBRL instance...")
        fs = FileSource.openFileSource(instance_path, cntlr)
        model_xbrl = cntlr.modelManager.load(fs)
        
        if model_xbrl is None:
            return {
                'success': False,
                'error': "Failed to load XBRL instance",
                'messages': {'errors': [], 'warnings': [], 'info': []}
            }
        
        print("✓ Instance loaded successfully")
        
        # Get basic statistics
        facts_count = len(getattr(model_xbrl, 'factsInInstance', [])) or len(getattr(model_xbrl, 'facts', []))
        undefined_facts = len(getattr(model_xbrl, 'undefinedFacts', []))
        contexts_count = len(getattr(model_xbrl, 'contexts', {}))
        units_count = len(getattr(model_xbrl, 'units', {}))
        
        print(f"Facts: {facts_count}, Undefined: {undefined_facts}, Contexts: {contexts_count}, Units: {units_count}")
        
        # Single-pass validation per plan
        print("Running Arelle validation (single pass)...")
        try:
            Validate.validate(model_xbrl)
            print("✓ Validation completed")
        except Exception as _e:
            print(f"Validation failed: {_e}")
        
        # Debug: Check what documents are loaded in DTS
        print("Checking DTS documents...")
        if hasattr(model_xbrl, 'urlDocs'):
            print(f"Total documents in DTS: {len(model_xbrl.urlDocs)}")
            for uri, doc in model_xbrl.urlDocs.items():
                if 'val' in uri or 'vr-' in uri:
                    print(f"  Validation-related doc: {uri} (type: {doc.type})")
                elif 'corep_lr-val.xsd' in uri:
                    print(f"  ✓ Found validation schema: {uri}")
        
        # Debug: Check if validation rules are loaded
        print("Checking for validation rules...")
        if hasattr(model_xbrl, 'formulaLinkbaseDocumentObjects'):
            formula_docs = model_xbrl.formulaLinkbaseDocumentObjects
            print(f"Found {len(formula_docs)} formula linkbase documents")
            for doc in formula_docs:
                if 'vr-v23175_s.xml' in doc.uri:
                    print(f"✓ Found v23175_s validation rules: {doc.uri}")
                else:
                    print(f"  Formula doc: {doc.uri}")
        else:
            print("No formula linkbase documents found")
        
        # Check for assertions
        if hasattr(model_xbrl, 'assertions'):
            assertions = model_xbrl.assertions
            print(f"Found {len(assertions)} assertions")
            for assertion in assertions:
                if 'v23175_s_20' in str(assertion):
                    print(f"✓ Found v23175_s_20 assertion: {assertion}")
        else:
            print("No assertions found")
        
        # Collect validation messages from buffer and model
        messages = collect_validation_messages(model_xbrl, cntlr)

        # Attempt to harvest unsatisfied assertions explicitly if available
        try:
            if hasattr(model_xbrl, 'unsatisfiedAssertions') and model_xbrl.unsatisfiedAssertions:
                for ua in model_xbrl.unsatisfiedAssertions:
                    ua_text = str(ua)
                    messages['errors'].append({
                        'code': 'formula:assertionUnsatisfied',
                        'message': ua_text,
                        'severity': 'error',
                        'type': 'UnsatisfiedAssertion'
                    })
        except Exception:
            pass
        
        # Check for eba_met concepts
        eba_met_concepts = 0
        if hasattr(model_xbrl, 'qnameConcepts'):
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            eba_met_concepts = len([
                concept for qname, concept in model_xbrl.qnameConcepts.items()
                if qname.namespaceURI == eba_met_ns
            ])
        
        # Clean up temp file if created
        if instance_path != original_path:
            try:
                pathlib.Path(instance_path).unlink()
                print(f"✓ Cleaned up temp file")
            except Exception:
                pass
        
        return {
            'success': True,
            'facts_count': facts_count,
            'undefined_facts': undefined_facts,
            'contexts_count': contexts_count,
            'units_count': units_count,
            'eba_met_concepts': eba_met_concepts,
            'messages': messages
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'messages': {'errors': [], 'warnings': [], 'info': []}
        }
    
    finally:
        # Clean up
        if model_xbrl:
            try:
                model_xbrl.close()
            except Exception:
                pass

def print_validation_results(results: Dict[str, Any]):
    """Print validation results in a readable format."""
    if not results['success']:
        print(f"\n❌ VALIDATION FAILED: {results['error']}")
        return
    
    print(f"\n✅ VALIDATION SUCCESSFUL")
    print(f"📊 Statistics:")
    print(f"   Facts: {results['facts_count']}")
    print(f"   Undefined Facts: {results['undefined_facts']}")
    print(f"   Contexts: {results['contexts_count']}")
    print(f"   Units: {results['units_count']}")
    print(f"   eba_met Concepts: {results['eba_met_concepts']}")
    
    messages = results['messages']
    
    # Print errors
    if messages['errors']:
        print(f"\n❌ ERRORS ({len(messages['errors'])}):")
        for i, error in enumerate(messages['errors'], 1):
            print(f"   {i}. [{error['code']}] {error['message']}")
    
    # Print warnings
    if messages['warnings']:
        print(f"\n⚠️  WARNINGS ({len(messages['warnings'])}):")
        for i, warning in enumerate(messages['warnings'], 1):
            print(f"   {i}. [{warning['code']}] {warning['message']}")
    
    # Print info messages
    if messages['info']:
        print(f"\nℹ️  INFO ({len(messages['info'])}):")
        for i, info in enumerate(messages['info'], 1):
            print(f"   {i}. [{info['code']}] {info['message']}")
    
    # Summary
    total_messages = len(messages['errors']) + len(messages['warnings']) + len(messages['info'])
    if total_messages == 0:
        print(f"\n🎉 No validation issues found!")
    else:
        print(f"\n📝 Total validation messages: {total_messages}")

def main():
    """Main function to run validation on MICA file."""
    if len(sys.argv) > 1:
        instance_path = sys.argv[1]
    else:
        # Default to MICA file
        instance_path = '/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207.xbrl'
    
    print("🔍 EBA XBRL Direct Arelle Validation")
    print("=" * 50)
    
    results = validate_instance(instance_path)
    print_validation_results(results)
    
    return 0 if results['success'] else 1

if __name__ == '__main__':
    sys.exit(main())
