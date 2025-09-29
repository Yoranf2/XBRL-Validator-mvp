# Arelle Integration Documentation

## Overview

This document describes the integration of Arelle XBRL processor as a git submodule in the EBA XBRL Validator backend.

## Arelle Version

- **Repository**: https://github.com/Arelle/Arelle.git
- **Tag**: edgr19.2.1
- **Commit**: 5406dd6bf202f705b2aead68b7215ebf0db3b317
- **Location**: `third_party/arelle/`

## Integration Details

### Path Configuration

Arelle is added to the Python path early in `app/main.py`:

```python
ARELLE_PATH = Path(__file__).resolve().parents[2] / "third_party" / "arelle"
sys.path.insert(0, str(ARELLE_PATH))
```

### Offline Configuration

The Arelle service is configured for strict offline operation:

- `workOffline = True`
- `maxAgeSeconds = 0`
- `internetConnectivity = 'offline'`
- Cache directory: `backend/cache/`

### Service Initialization

The `ArelleService` class provides:

1. **Controller Setup**: Initializes `Cntlr.Cntlr` with offline settings
2. **Package Loading**: Loads taxonomy packages from unpacked directories
3. **Instance Loading**: Loads XBRL instances with fact counting
4. **Validation**: Validates instances with profile-based configuration

### Validation Profiles

- **fast**: No formulas, no CSV constraints, no trace
- **full**: Formulas + CSV constraints, no trace
- **debug**: Formulas + CSV constraints + trace

## API Integration

### Health Endpoint

The `/health` endpoint reports Arelle availability and version:

```json
{
  "status": "healthy",
  "service": "eba-xbrl-validator",
  "version": "0.1.0",
  "arelle_version": "edgr19.2.1",
  "arelle_available": true,
  "arelle_path": "/path/to/third_party/arelle",
  "offline_mode": true
}
```

### Service Access

The Arelle service is available globally via `app.state.arelle_service` after startup.

## Error Handling

- Import errors are caught and logged
- Service continues startup even if Arelle fails to initialize
- Health endpoint reports Arelle availability status
- Validation requests fail gracefully with error messages

## CI/CD Considerations

### Submodule Checkout

Ensure CI systems checkout submodules:

```bash
git submodule update --init --recursive
```

### Dependencies

Arelle may require additional system dependencies:
- `lxml` (usually available as wheel)
- `python-dateutil`
- `isodate`

### Offline Enforcement

CI should enforce offline mode by:
- Blocking network access during tests
- Verifying no HTTP requests are made
- Testing with local taxonomy packages only

## Troubleshooting

### Import Errors

If Arelle imports fail:
1. Check that `third_party/arelle/` exists
2. Verify submodule is properly initialized
3. Check Python path configuration
4. Review Arelle dependencies

### Package Loading Issues

If taxonomy packages fail to load:
1. Verify package paths exist
2. Check for `taxonomyPackage.xml` files
3. Review Arelle logs for package errors
4. Ensure offline mode is properly configured

### Validation Failures

If validation fails unexpectedly:
1. Check instance file format
2. Verify taxonomy packages are loaded
3. Review validation profile settings
4. Check Arelle error messages

## DTS-First Injection (Option 2b)

The service supports DTS-first injection for instances that use `eba_met:*` concepts but lack `met.xsd` schema references. This feature is controlled by configuration flags:

### Configuration Flags

```yaml
flags:
  enable_dts_first_api: false          # Enable DTS-first API features
  dts_first_inject_schema_refs: false  # Enable schemaRef injection
  allow_instance_rewrite: true         # Allow temp file creation (dev only)
```

### How It Works

1. **Detection**: Scans instances for `eba_met` namespace usage without `met.xsd` references
2. **URL Resolution**: Tests both `/eu/fr` and non-`/eu/fr` URL variants via catalog resolution
3. **Injection**: Creates in-memory XML with injected `<link:schemaRef>` elements
4. **Fallback**: Uses temp file approach if in-memory injection fails
5. **Verification**: Confirms `met.xsd` is present and `eba_met` concepts are loaded

### Acceptance Criteria

- `met.xsd` present in DTS
- `eba_met_concepts_count > 0`
- `facts_by_namespace["http://www.eba.europa.eu/xbrl/crr/dict/met"] ≥ 79` (for MICA samples)
- `undefined_facts == 0` (no undefined `eba_met` facts)
- No HTTP fetch attempts (offline mode maintained)

### Debugging

Use the `/api/v1/debug/probe` endpoint to test URL resolution:

```bash
curl -X POST "http://localhost:8000/api/v1/debug/probe" \
  -F "url=http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
```

## Future Enhancements

- Plugin support for additional validation rules
- Custom taxonomy package formats
- Enhanced error reporting and diagnostics
- Performance optimization for large instances
- True in-memory injection without temp file fallback
