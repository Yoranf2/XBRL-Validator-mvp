## Development Plan — EBA XBRL Validator Backend (RF 4.0 baseline, COREP XML first)

## Goal
Build a backend service (FastAPI + Arelle) to validate EBA filings with strict offline operation, supporting DPM 1.0 and 2.0, and both xBRL-XML and xBRL-CSV. Start with RF 4.0; extend to 4.1/4.2 and then 2.9.

## Guiding Principles
- **One service, minimal abstractions**: Direct Arelle API (Cntlr, ModelManager, openFileSource)
- **Packages-first, offline**: Load official RF 4.x taxonomy packages as ZIPs via PackageManager; hard offline (block http/https)
- **Resolution seam**: Default offline; support online behind a flag with allow‑lists and caching for determinism
- **No shortcuts or technical debt**: Never mutate inputs; no TODO-debt; correctness over speed
- **Profile-driven validation**: fast (no formulas), full (formulas + CSV), debug (trace)
- **Process isolation**: Subprocess per job, timeouts and RSS ceilings
- **Versioned config**: Structured logs/metrics per run

## Deliverables
- FastAPI backend: `/validate`, `/taxonomies`, `/profiles`, `/health`, `/validate/byEntrypoint`
- Arelle integration with XML/CSV ingest adapters
- DPM detector (1.0 vs 2.0) based on DTS namespaces/architecture
- Filing Rules pre and post checks
 - RF 4.0 taxonomy setup (3 ZIP packages via PackageManager) + entrypoint catalog
- Config files: `config/app.yaml`, `config/eba_taxonomies.yaml`
- Golden tests RF 4.0; CI
- Security & sandboxing

## Updates — Message Output & Coverage (Current)

### Message output (human‑readable)
- Template header (e.g., `Template C_47.00 — Leverage ratio calculation`).
- Rule in symbolic form with column context:
  - Example: `Rule Row 0200 "Corporate" ≤ Row 0190 "Of which: retail SME" for column 010 "Leverage Ratio Exposure Value: SA Exposures"`.
- Check line with pass/fail:
  - Example: `Check: 75,085 ≤ 68,246 → ❌ Failed`.
- Gap line (formerly Difference) with percent:
  - Example: `Gap: 6,839 (10.0%)`.
- Long RHS collapse: for >5 RHS terms, show `sum of N rows`.
- LR ≥ single‑RHS: use absolute deduction on RHS for Check and Gap (|rhs| denominator).
- Zero‑padded codes everywhere: rows 4 digits, columns 3 digits.
- Numeric normalization: EU/US separators, scientific notation clamp, unicode minus/parentheses negatives.
- Fix section: shown only when specific hints are available (scale/sign/missing/outlier or per‑column/rows instruction); generic fallback removed.

### Coverage and baselines
- Baseline generator script: `backend/scripts/enumerate_assertions.py` enumerates assertions per entrypoint/taxonomy version and writes `backend/config/assertion_baseline.json` with `present_count` and `hash`.
- Runtime coverage in response under `metrics.coverage`:
  - `rule_ids_seen_approx`, `baseline_present_count`, `baseline_hash`, `entrypoint_id`, `taxonomy_version`, `approx_complete`.
- Automatic baseline generation:
  - On startup: background job (non‑blocking) can prebuild baselines per entrypoint.
  - On first validate: if missing/stale, kicks off background baseline build; coverage appears on the next run when available.

### Offline taxonomy prerequisites
- `backend/config/eba_taxonomies.yaml` must list RF 4.0 package roots (directories that contain `META-INF/taxonomyPackage.xml`).
- Recommended to include the “Full Taxonomy” mirror root plus the three official RF 4.0 packages (Dictionary, Reporting_Frameworks, Severity).
- HTTP is blocked by default; all resolution via package catalogs.

## Repository Structure (planned)
```
backend/
  app/
    main.py
    api/
      routes_validation.py
    services/
      arelle_service.py
      ingest_xml.py
      ingest_csv.py
      dpm_detect.py
      profiles.py
      filing_rules.py
    models/
      validation_models.py
    utils/
      proc_exec.py
      logging.py
      paths.py
  config/
    app.yaml
    eba_taxonomies.yaml
  tests/
    unit/
    integration/
    perf/
    golden/
  github_work/
    taxonomies/eba/rf40/{rf,dict,severity}-unpacked/
  uploads/  temp/  cache/  logs/
```

## Configuration Examples

### config/app.yaml (MVP)
```yaml
profiles:
  fast:  { formulas: false, csv_constraints: false, trace: false }
limits:
  task_timeout_s: 300
  max_rss_mb: 2048
  max_upload_mb: 150
flags:
  resolution:
    mode: offline            # offline | online
    allow_catalogs: true
    block_http: true         # ignored if mode=online
    allowlist_hosts: []      # required if mode=online
    http_timeout_s: 20       # if mode=online
    enable_web_cache: true
  use_packages: true
  allow_instance_rewrite: false
features:
  messages:
    source: auto             # auto | zip | unpacked (dev may use 'unpacked'; prod use 'zip' or 'auto')
    lang: en                 # only English for now
    hide_raw_keys: true      # never show message:v#### to end users
    cache:
      enabled: false
      path: backend/var/cache/messages/catalog.json
    zip_globs:
      - taxonomies/eba/rf40/zips/rf/**/*.zip
      - taxonomies/eba/rf40/zips/severity/**/*.zip
    unpacked_roots:
      - taxonomies/eba/rf40/rf-unpacked
      - taxonomies/eba/rf40/dict-unpacked
      - taxonomies/eba/rf40/severity-unpacked
observability:
  metrics: [facts_count, errors_count, validation_status, duration_ms, dmp_version, is_csv]
  prometheus:
    enabled: false
    path: /metrics
    namespace: xbrl_validator
```

### config/eba_taxonomies.yaml (RF 4.0 baseline)
```yaml
eba:
  rf40:
    packages:
      - ./backend/github_work/taxonomies/eba/rf40/rf-unpacked
      - ./backend/github_work/taxonomies/eba/rf40/dict-unpacked
      - ./backend/github_work/taxonomies/eba/rf40/severity-unpacked
    entrypoints:
      - id: corep_of
        label: COREP Own Funds
        xsd:  http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/mod/corep_of.xsd
      - id: corep_lr
        label: COREP Leverage Ratio
        xsd:  http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/mod/corep_lr.xsd
      # Project declares additional RF 4.0 entrypoints to surface ~20 frameworks in /taxonomies
    defaults:
      profile: full
```

## Toolchain Decisions (pinned)
- **Python 3.11**
- **Arelle**: Vendored as a pinned submodule under `third_party/arelle/` (preferred for offline determinism). If using pip, pin exact versions and hashes.
- **Package path handling**: Create repo-local symlinks to the three RF 4.0 packages:
  - Reporting Frameworks → `github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0`
  - Dictionary → `github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Dictionary_4.0.0.0/EBA_XBRL_4.0_Dictionary_4.0.0.0`
  - Severity → `github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Severity_4.0.0.0/EBA_XBRL_4.0_Severity_4.0.0.0`

## API Surface (MVP)
- `POST /validate` — upload XML (multipart) or dev-only path; auto-detect; run validation
- `GET /taxonomies` — list configured taxonomies & entry points
  - Discovers frameworks from local Full Taxonomy mirror (e.g., `.../crr/fws/*/4.0/mod/*.xsd`).
  - Merges additional RF 4.0 entrypoints declared in `config/eba_taxonomies.yaml` (listed even if files are not mirrored yet).
  - Validations still require offline resolution via packages/local mirrors.
- `GET /profiles` — list available profiles & flags (optional for v0.1)
- `GET /health` — service and Arelle build info
- `POST /validate/byEntrypoint` — sanity validation by entry point (no user data)

## Metrics & Logs (minimal JSON)
Keys: `trace_id`, `run_id`, `duration_ms`, `facts_count`, `dpm_version`, `is_csv`

### Non-Actionable Entries Policy (2025-09-27)

- Definition: entries that have no actionable payload after normalization: empty `message`, `code == "unknown"`, no `refs`, and no location/concept fields (`table_id`, `rowCode`, `colCode`, `conceptNs`, `conceptLn`).
- Recovery before drop: we first backfill `message` from `code`, extract `rule_id` from any `message:v…` token, set `category=formulas` for v-codes, and attempt table/row/col parsing.
- Filtering: if still non-actionable, entries are dropped by default (feature-flagged).
- Flags (config `backend/config/app.yaml`):
  - `features.drop_nonactionable: true` (default)
  - `features.drop_nonactionable_sample_limit: 5`
- Metrics added per run:
  - `metrics.dropped_nonactionable_count`
  - `metrics.dropped_nonactionable_breakdown: { errors, warnings }`
  - `metrics.dropped_nonactionable_samples` (first N snapshots)
- Audit mode: set `drop_nonactionable: false` to keep such entries; they will be tagged with `nonactionable: true` in the output.

### Concise Validation Message Builder (2025-09-28)

Goals: produce short, actionable guidance with consistent sections while avoiding noisy payloads.

Formatting (HTML for UI; parallel plain-text can be emitted if needed):
- Title: `<strong>Title</strong> {table_id} — {table_label_without_prefix}`
- Rule: `<strong>Rule</strong> …` (collapse long RHS to “must equal the sum of the related rows (Col …)” when RHS > 5)
- Found (multi-line):
  - `<strong>Found</strong> Row {lhsCode} – {lhsLabel}: {lhs}`
  - `Compare (sum of N rows): {sum} = a + b + …` (when N ≤ 5); else `Compare (sum of N rows): {sum}`
  - `Difference: {±N} ({±P}%)`
- Likely causes: Only when detected via heuristics (see below). Otherwise omitted.
- Top contributors: Only when RHS > 5; include value and share; humanize large numbers.
- Fix: Data-driven; only detected items (sign/scale/outliers/missing); end with “Recalculate and resubmit.”

Heuristics (data-driven):
- Scale hint: only if lhs/sum < 1/1000 or > 1000, or max(component)/median ≥ 1000.
- “(–)” rows sign: if label starts with “(–)” and value > 0 → flag; prefer taxonomy metadata when available.
- Missing/zero: zip Rule RHS rows ↔ Found RHS values; if row absent/0 or share < 0.5% while sum > 0 → flag as missing/zero.
- Outliers (n-aware):
  - n ≤ 3: value ≥ 2× next largest AND share ≥ 80% (or ≥1.5× and ≥90%).
  - 4 ≤ n ≤ 7: value ≥ 4× median AND share ≥ 60%; else dominant if share ≥ 50%.
  - n ≥ 8: value ≥ 5× median AND robust z-score ≥ 3 AND share ≥ 40%.

Label hygiene:
- Table label: strip `{table_id}:` prefix.
- Row/column labels: use last segment after `/`, trim to ~80 chars, collapse whitespace; always show code + short label.

Large numbers:
- Use scientific notation with three significant figures for |number| ≥ 1e9 (e.g., `≈ 8.05×10^84`).
- Otherwise thousands separators.

Groups (when RHS > 5 and applicable):
- For LR (C_43.00/C_47.00/C_48.0x), add `Groups included: SFTs; Derivatives; Off‑balance sheet (CCF); Adjustments.`
- Extend with per-table group maps for other templates when needed.

Pruning (noise control):
- Pre- and post-build pruning: keep only entries with `rule_id`, or with extracted Rule and Found (with numerics), or with table/row/col context. Drop “This cell …” generics.
- Metrics: track drops in `metrics.dropped_nonactionable_count` and samples for audit.

## CI Environment (baseline)
- **Runner**: Ubuntu (ubuntu-latest), Python 3.11
- **Strict offline**: No network; all packages local; dedicated cache dir
- **Limits**: Per-validation timeout 10m, job timeout 15m, RSS cap 2 GiB, 1–2 vCPU

### Online Mode Migration Readiness

- Introduce a single resolver seam on the Arelle `modelManager`: offline (packages + XML catalogs) vs online (allow‑listed HTTP + web cache).
- Keep instance documents immutable; map URLs via catalogs/package metadata only.
- Enable provenance logs: resolved URL, cache path, package version, timestamps.
- Add parity tests: same entrypoint validated in offline and (optionally) online; assert:
      - DTS includes dictionary schemas (e.g., `met.xsd`) in both modes
      - Identical concept resolution for `eba_met:*` sample concepts
      - Comparable facts count and key metrics
- CI remains strict offline; provide a separate, opt‑in job for online smoke tests.
- Document operational guardrails: host allow‑list, timeouts/retries, cache directory policies.

## Output Enrichment — Top Error Codes (new)

- Add aggregated top error codes to the validation response under `metrics.top_error_codes`.
- Shape: array of `{ code: string, severity: "error"|"warning", count: number }`, default N = 10.
- Source of truth: merge of buffered Arelle log entries and `model_xbrl.errors`/`warnings` (same as existing collection).
- Rationale: quick triage of dominant issues across a run; no separate endpoint needed.

## Next Steps: Arelle Validation Message Collection

### Validation Message Collection Implementation

**Goal**: Run validations in Arelle via our application and return both successful and failed validation messages.

#### Required Steps:

1. **Load Taxonomy Packages (Once)**
   - Use the 3 RF 4.0 ZIPs via `PackageManager.addPackage(...)`
   - Call `PackageManager.rebuildRemappings(...)` to activate URL mappings
   - Keep offline mode enabled (no HTTP attempts)

2. **Ensure `met.xsd` is Discoverable Pre-Load**
   - Check if `dict/met/met.xsd` is missing from instance schemaRefs
   - If missing (dev-only), inject canonical URL into a temp copy (never mutate user uploads)

3. **Run Validation and Collect Messages**
   - Load instance with `FileSource.openFileSource(...)`
   - Run `Validate.validate(model_xbrl)` from Arelle
   - Collect buffered JSON (errors/warnings/info) once at end; include `refs`

4. **API Response Structure**
   ```json
   {
     "status": "success" | "failed",
     "messages": {
       "errors": [{"code": "string", "message": "string"}],
    "warnings": [{"code": "string", "message": "string"}],
    "info": [{"code": "string", "message": "string"}]
     },
     "metrics": {
    "facts_count": 0,
    "undefined_facts": 0,
    "contexts_count": 0,
    "units_count": 0,
    "eba_met_concepts": 0
     }
   }
   ```

#### Optional Validation Sanity Checks
- **Filing indicators present**: Detect `find:fIndicators`; log count and `@filed="true"` indicators
- **Formula linkbases loaded**: DTS includes EBA validation linkbases and set/ignore linkbases
- **Formula engine status**: `modelXbrl.hasFormulae == True`; `formulaOptions.formulaAction == 'run'`
- **No pre-formula exceptions**: No `exception:*` logs that short-circuit formula evaluation
- **Disclosure system/plugin**: Log active disclosure system; v-codes come from formula engine
- **Environment compatibility**: Python ≥ 3.11 (PEP 604 unions)
- **Execution evidence**: number of formula linkbases; v-code presence; unsatisfied assertion counts

#### Output Enrichment
- Include `metrics.top_error_codes` (N=10) in the summarized response so clients can render most frequent v-codes without extra computation.

### Validation Messages — Catalog Resolution Strategy (agreed 2025-09-26)

Goal: Resolve taxonomy message keys like `message:v4460_m_0` to human‑readable text while keeping strict offline behavior and robust DTS loading.

- DTS loading: keep Arelle on official ZIP packages via PackageManager (this was required to reliably load facts and linkbases).
- Message extractor: configurable source
  - `zip` (production): scan message resources directly from the RF/Severity ZIPs (e.g., val/vr-*.xml, message/label resources with role "message").
  - `unpacked` (developer convenience): scan the unpacked trees under `taxonomies/eba/rf40/{rf,dict,severity}-unpacked/` for faster iteration and easier grepping.
  - `auto` (default): prefer unpacked if present, otherwise fall back to ZIPs.
- Language: English only for now (`xml:lang='en'`); structure allows adding more languages later.
- UX rules: never surface raw keys like `message:v####` to users. When available, prepend the official catalog text; always include our structured "Rule / Found / Fix" to make messages actionable.
- Caching & parity:
  - Build a v‑code → text map at startup; cache in memory; optionally persist a JSON cache.
  - Add a lightweight parity check (dev): compare a small sample set extracted from ZIPs vs unpacked to catch drift.
- Fallbacks: if a specific id is missing in catalogs, show only the structured "Rule / Found / Fix" without the raw key.
 - Observability:
   - Export `messages_catalog_ids_loaded` (Gauge), `messages_resolved_total` (Counter), `messages_unresolved_total` (Counter) when Prometheus is enabled.
   - Embed per-run `messages_resolved_count` and `messages_unresolved_count` in API responses.

Rationale:
- ZIP packages with PackageManager give deterministic DTS/fact loading (matches prior breakthrough).
- A separate, configurable extractor achieves high coverage for messages without changing the DTS resolution path.


#### Testing Approach:
- Test with MICA, DORA, and COREP sample files
- Verify both success (0 errors) and failure scenarios
- Ensure offline operation maintained

### Executable TODOs — Validation Flow, Logging, and Output (Agreed)

1) Environment & Toolchain
- Pin Python: 3.11 for dev/CI; remove any Python 3.9 vendor edits to Arelle
- CI: install `python@3.11`, install backend and `third_party/arelle/requirements.txt`

2) Validation Execution
- Always call `cntlr.modelManager.loadCustomTransforms()` before validation
- Use a single run: `Validate.validate(model_xbrl)` (no explicit second formula pass)
- Keep formulas enabled in default/full profile; disable in fast profile

Executed (2025-09-25)
- Implemented in `ArelleService.validate_instance`: loads custom transforms immediately before `Validate.validate(...)` and sets `formulaAction` per profile (fast: none; full/debug: run).
- Simplified direct harness to a single `Validate.validate(...)` pass.

3) Buffer Handling
- Clear LogToBuffer exactly once before validation
- Retrieve buffer JSON exactly once after validation; do not clear mid-run

Executed (2025-09-25)
- Controller initialized with `logFileName="logToBuffer"`; buffer parsed after validation and merged into API `errors`/`warnings`.

4) Message Selection & Severity
- Default output shows taxonomy v-code messages (respect severity/ignore linkbases)
- Keep `errorUnsatisfiedAssertions=false` by default (do not escalate severities)
- Add optional flag `include_traces` to include `formula:assertionUnsatisfied` entries

5) Filing Indicators
- Do not block validation if filing indicators are missing
- Detect and log indicators (count, list) when present; include in diagnostics output

6) Schema Injection Policy
- Prefer entrypoints to bring in validation linkbases
- For COREP LR or samples missing rule XSD, allow dev-only temp copy injection behind a flag; never mutate user uploads by default

Executed (2025-09-25)
- Added conditional COREP LR validation schema injection (feature-flagged) when discovery lacks val docs; implemented via temp copy only.

Planned extension
- Under the same feature flag, permit conditional injection for additional COREP modules (e.g., `corep_of-val.xsd`) when:
  - heuristic detection identifies the module (filename or namespace), and
  - `modelManager.urlDocs` shows no loaded `.../val/...` documents for that module.
- Guardrails: only in dev/test; produce diagnostics listing any injected URLs.

11) Offline Resolution (new)
- Use package catalogs first (three RF 4.0 ZIPs via PackageManager)
- Add config-driven `offline_roots` for URL→local mappings where catalogs are incomplete
- Support dual-variant aliasing (`/eu/fr` and non-prefixed) in the resolver hook

Executed (2025-09-25)
- Added `offline_roots` to `config/app.yaml` for `www.eba.europa.eu` and `www.eurofiling.info` Full Taxonomy trees.
- Extended WebCache.TransformURL hook to consult these mappings after PackageManager.

Executed (2025-09-25, update)
- Added `offline_roots` for `http://www.xbrl.org/2003/` (instance/linkbase) to the local Full Taxonomy mirror; added local fallbacks for `http://www.xbrl.org/2003/role/` and `/arcrole/` to ensure strict offline resolution for calculation/role resources (used by synthetic fixtures and generic taxonomies).

FilingIndicators.json handling (eurofiling host)
- Primary approach: mirror the EBA JSON via a symlink at `github_work/eba-taxonomies/EBA Taxonomy 4.0/Full Taxonomy/www.eurofiling.info/eu/fr/xbrl/ext/2020/FilingIndicators.json` pointing to the existing EBA-host file at `.../www.eba.europa.eu/eu/fr/xbrl/ext/2020/FilingIndicators.json`.
- Alternative (if symlinks are undesirable): add a narrow `offline_roots` mapping for `http://www.eurofiling.info/xbrl/ext/2020/` to the local EBA `.../eu/fr/xbrl/ext/2020/` folder.
- Rationale: ensure strict offline resolution without adding host-specific rewrite logic in code.

12) Readme & Toolchain (new)
- Pin Python 3.11 in docs and commands; install both backend and Arelle requirements

Executed (2025-09-25)
- Backend README updated with Python 3.11 pin and dual-requirements installation.

API Test Evidence (2025-09-25)
- COREP LR sample validated via API (fast/full). After resolver updates, IO/schema load errors reduced to focused errors/warnings; message text files produced under `backend/logs/`.

7) Output Shapes (API/UI)
- Raw: full buffer JSON (with `refs`)
- Summarized: grouped errors/warnings/info; by default include only v-codes
- Diagnostics: counts by code, presence of rule linkbases, filing indicators summary

Executed (2025-09-25)
- Added `metrics.category_counts` with classifier buckets: `xbrl21`, `dimensions`, `calculation`, `formulas`, `eba_filing`.
- Initial classifier uses code/text/linkbase refs, with stricter patterns for DIM (xbrldte/xbrldi, dimension terms, definition linkbase cues) and CALC (summation-item, weight, calc linkbase cues).

8) Profiles & Trace Flags
- fast: formulas=false, traces off
- full: formulas=true, traces off
- debug: formulas=true, `traceUnsatisfiedAssertions=true`, `traceUnmessagedUnsatisfiedAssertions=true`

Executed (2025-09-25, update)
- Enforced profile divergence without impacting full: `fast` disables formulas and filters out formula-derived messages (v-codes) from output; `full` remains unchanged.

9) Smoke Tests (per framework)
- Load packages, rebuild remappings, load custom transforms
- Validate a sample; assert: rule linkbases present in DTS, ≥1 v-code emitted, no HTTP attempts

## Integration Parity Test — Fast vs Full (new)

File under test (fixed):
- `/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_COREP040000_COREPLR_2024-12-31_20241211134749200.xbrl`

Assertions:
- Offline determinism: `get_offline_status().http_fetch_attempts == []` (no HTTP attempts)
- Dictionary presence: `dts_evidence.met_xsd_present == true`
- Full profile emits at least one EBA v-code (any `errors[*].code` or `metrics.validation_issues[*].code` matching `^message:v`)
- Fast vs Full behavior differs (e.g., error counts or presence of rule-based messages)

Purpose:
- Protects offline guardrails and DTS health; ensures validation rules load and profiles behave as intended.

Executed (2025-09-25)
- Parity test extended to assert v-code presence (via `top_error_codes` or raw messages), and to collect evidence counts; enforced divergence by disabling formulas and filtering formula messages in `fast` while keeping `full` unchanged.

10) CI Enforcement
- Pin Python 3.11 image, install dependencies, run smokes for MICA/COREP/IFCLASS2
- Fail job if offline invariant or v-code emission checks fail

## Notes
- Entrypoints surfaced:
  - From local files: any framework under `.../crr/fws/*/4.0/mod/*.xsd` (e.g., corep, dora, if, mica)
  - From YAML: additional RF 4.0 entrypoints (e.g., finrep, rem, rembm, rembmif, etc.) appear in `/taxonomies` even without local mirrors (listing only)
- CSV support, broader filing rules, and additional frameworks follow next iterations

## Current Findings and Open TODOs (2025-09-25)

### Findings
- load_instance NameError: Fixed. Results/log writing was mistakenly added to `load_instance`; reverted so it only returns `(model_xbrl, facts_count)`. Logs are written in `validate_instance`.
- Catalogs from zipped packages: "Catalog not found" signals are non-blocking when we use `offline_roots` plus file:// injection. We can keep zips and still run fully offline.
- Val linkbases not surfacing in DTS: `corep_lr-val.xsd` and `met.xsd` are injected via file:// schemaRefs, but formula linkbases are not yet discovered/executed in the current path (0 formula docs reported). Likely requires Arelle disclosure system/plugin activation or explicit linkbase loading.
- Per‑run logs: Implemented. `validate_instance` now writes categorized logs under `backend/logs/`:
  - `validation_xbrl21_<run>.{json,txt}` for XBRL 2.1 structure
  - `validation_formulas_<run>.{json,txt}` for formula/v-code messages (includes `top_error_codes`)
- COREP LR sample behavior: The fixed sample always produces v-codes when rules execute; do not suggest it as a zero‑error example.

New Findings (2025-09-25)
- Calculation/Dimensions linkbases for COREP LR samples are typically not present; constraints are primarily via formulas (v-codes). DTS evidence shows `calc_relationships_count=0` and `dimension_relationships_count=0` for the fixed LR file.
- Synthetic fixtures demonstrate classifier coverage: a minimal DIM instance triggers `dimensions>0`; a minimal CALC instance triggers `calculation>0` once base schemas/linkbase wiring are satisfied.

Synthetic Test Fixtures (2025-09-25)
- Added minimal fixtures under `backend/tests/fixtures/`:
  - `dim/` with definition linkbase (hypercube/axis/domain/members) and an invalid explicitMember instance → Dimensions category detected
  - `calc/` with calculation linkbase (summation-item arcs) and an inconsistent total → Calculation category detected (strictly offline)

### Open TODOs (next steps)
- Optional: switch to unpacked package manifests to silence package catalog warnings entirely (or downgrade repeated warnings to DEBUG).
- Expand DTS evidence: assert presence counts for calculation and dimensions relationship sets when applicable; currently logged but not asserted.
- Add targeted integration tests for DIM and CALC using local fixtures; keep strict offline invariant.

FR 5.5 Pre‑Flight (Balanced Policy) — Executed (2025-09-25)
- Module & integration
  - Added `app/services/filing_rules.py` with `run_preflight` executed pre-validation (no input mutation).
  - Wired into `ArelleService.validate_instance` after load/offline checks; results attached to metrics.
- Checks (XML scope, balanced severity)
  - Filing indicators (fi:filed):
    - Presence and count of filed=true (warning if missing); invalid/ill‑formed → error; conflicting indicators → error; unknown template IDs → warning
    - Resolve indicators JSON offline with EBA as primary and Eurofiling mirror; record provenance
  - Entrypoint/module consistency: instance aligns with declared entrypoint/module (error if mismatch)
  - Structural hygiene: contexts present; entity identifier scheme/value non-empty; periods coherent; units present/well-formed (errors per XBRL 2.1 rules)
  - Linkbase presence (framework-appropriate): presentation/definition present (info/warn if missing where optional)
  - Offline invariant: no HTTP attempts (error on violation)
- Output & metrics
  - Emits `metrics.filing_rules_preflight = { passed, failed, items:[...] }` and increments `category_counts.filing_rules_preflight`.
  - Writes dedicated filing indicators issue log `logs/filing_indicators_<run>.json` when warnings/errors present.
- Tests
  - Positive/negative cases for indicators presence/format; entrypoint/module mismatch; offline invariant
  - Assert no network, deterministic results
- Config & docs
  - Documented Balanced policy; `/api/v1/preflight` endpoint added for preflight-only runs.
  - Light mode added to preflight to reduce scan time on large files.

## Template Viewer — EBA Tables Rendering (COREP LR first)

### Decisions (agreed)
- **Output path & retention**
  - **path**: `backend/temp/tables/<run-id>/` (run-id = timestamp + short hash); expose via static mount.
  - **retention**: TTL 3 days + disk cap (2–5 GiB) with LRU deletion.
- **Language defaults**
  - **default**: `en`; optional `lang` parameter; fallback to English when missing.
- **Render strategy**
  - **MVP**: render-all (tableset) for the DTS; add render-per-table later.
- **Priority**
  - **ship basic viewer first**, then add cell highlighting in next iteration.

### Implementation Tasks (MVP — render-all)
1) Service integration
- Add `backend/app/services/arelle_service_templates.py` with:
  - `render_eba_tableset(model_xbrl, out_dir: str, lang: str = "en") -> str`
  - Internals: `from arelle.rendering import RenderingEvaluator; RenderingEvaluator.init(model_xbrl)` then `from arelle.plugin.saveHtmlEBAtables import generateHtmlEbaTablesetFiles; generateHtmlEbaTablesetFiles(model_xbrl, index_file, lang=lang)`.
- Optionally, add an instance method on `ArelleService` that wraps this utility.

2) API and static serving
- Add `POST /render/tableset`:
  - inputs: `run_id` (or `instanceId`), optional `lang`.
  - output: `{ "index_url": "/static/tables/<run-id>/index.html", "path": "<abs path in dev>" }`.
- Mount static files:
  - `/static/tables/` → `backend/temp/tables/` (read-only).
- Compose `run-id` from validation run metadata; ensure uniqueness and provenance record.

3) Output management
- Create `backend/temp/tables/<run-id>/` per invocation; write `index.html`, `FormsFrame.html`, and per-table HTML files.
- Record provenance (entrypoint, instance path, DTS size) in a small JSON in the same directory.

4) Retention & GC
- Daily GC task:
  - delete directories older than 3 days under `backend/temp/tables/`.
  - enforce disk cap (2–5 GiB) by deleting oldest dirs until under cap.
- Log deletions with run-id and sizes.

5) Testing (offline deterministic)
- Integration test:
  - after validation of COREP LR sample, invoke render-all → assert `index.html` exists; menu lists ≥1 table; no HTTP attempts recorded.
- Smoke on large DTS: measure time; ensure bounded memory; skip if exceeds perf budget.
- Security: static mount does not allow writes; paths normalized.

6) Documentation
- Update `backend/README.md`:
  - how to call `/render/tableset`
  - where files are stored; retention policy; offline behavior
  - `lang` parameter behavior and default

### Implementation Tasks (Phase 2 — render-per-table and highlighting)
1) Render a single table
- Add `render_table(model_xbrl, table_id: str, out_file: str, lang: str = "en")` using `arelle.ViewFileRenderedGrid.viewRenderedGrid`.
- Endpoint `POST /render/table` with `table_id`, `run_id`, optional `lang`.

2) Cell-to-fact mapping (sidecar JSON)
- Traverse layout model used in rendering to extract a mapping: `cell_id` → `{ concept, contextRef, unitRef, factIds[], dimensions }`.
- Emit `<table_id>.mapping.json` alongside the HTML.

3) Front-end highlighting
- Inject a minimal JS snippet in per-table HTML:
  - loads `<table_id>.mapping.json`
  - highlights cells for a provided set of fact identifiers (e.g., from an error).
- Provide an anchor/query (e.g., `?errorId=...`) to auto-highlight on load.

4) Error → table routing
- For an error message:
  - extract referenced concept(s) and context/dim aspects when available.
  - rank candidate tables that cover the concept and dimensions; render only those for drill-in.

5) Tests
- Mapping correctness: for a known fact, ensure the sidecar JSON points to exactly one or expected set of cells.
- Multi-fact errors: ensure multiple cells highlight; verify JS doesn’t degrade rendering.

### Acceptance Criteria
- MVP:
  - Given a validated COREP LR instance, `/render/tableset` produces an index and navigable per-table HTML under `backend/temp/tables/<run-id>/`, served at `/static/tables/<run-id>/index.html`.
  - Offline invariant preserved (no HTTP); rendering succeeds for both COREP LR and similar EBA entrypoints.
- Phase 2:
  - `/render/table` renders a single table by ID.
  - Sidecar mapping JSON exists; cells highlight correctly given a set of fact identifiers.

### Risks & Mitigations
- Large DTS render cost → provide per-table rendering and cache results.
- Messages without explicit facts → show relevant template(s) and banner; skip cell highlight.
- Label language gaps → fallback to English; optionally surface a language toggle.

### Notes
- All resolution remains offline using the single resolver seam (packages + catalogs).
- Do not mutate user uploads; any temp files live under `backend/temp/`.

## Web UI — Validation & Templates (MVP)

### Stack & Hosting
- **Frontend**: React + Vite + TypeScript SPA, served under `/ui` by FastAPI
- **Backend**: Existing FastAPI APIs; reuse `/api/v1/validate`, `/api/v1/preflight`, `/api/v1/render/*`
- **Look & Feel**: Follow the referenced GUI closely for layout and components; centralize theme tokens for easy restyle

### Features
- **Upload & Actions**: File picker; buttons for Run Preflight, Run Validation (full), Render Templates
- **Progress/Timings**: Show elapsed time per action; display `run_id` and links (e.g., `tables_index_url`)
- **Summary**: Totals and per-group counts (`xbrl21`, `dimensions`, `calculation`, `formulas`, `eba_filing`, `filing_rules_preflight`)
- **Messages Table**: Columns {category, rule_id, severity, friendly_location, message}; filter/search; click → deep-link to Template Viewer with highlights
- **Template Viewer Tab**: Left nav of tables; right iframe of table HTML; highlight cells via query params; tooltips with rule summary on hover

### Backend Enhancements
- **ValidationResponse**: add `tables_index_url` (done); enrich messages with `rule_id`, `category`, `severity`, `conceptNs`, `conceptLn`, `contextRef`, and `friendly_location` {table_id, table_label, row_label, col_label}
- **Mapping JSON**: include `row_label` and `col_label` alongside `rowIndex`, `colIndex`, and `facts` for each cell
- **Error Routing (optional)**: `/api/v1/render/for-error?run_id=&rule_id=&message_id=` returns candidate {table_id, link_url, matched_facts[]}
- **Runs Status**: `/api/v1/runs/{run_id}` (polling) with {status, timings, links}; consider WebSocket later

### Retention
- Keep run artifacts for **3 days**; cap storage (2–5 GiB) with LRU deletion; surface TTL in UI help

### Acceptance (UI MVP)
- Users can upload a file, run preflight and validation, see timings, summary counts, and a full messages table
- Clicking a message opens the Template Viewer focused on the right table; matching cells are highlighted; tooltip shows error summary

### Risks & Mitigations
- Not all errors reference facts → show banner + candidate tables; no exact highlight
- Some tables lack explicit row/column labels → fallback to indices or nearest available header text
- Large DTS → rely on render-per-table when tableset is heavy; cache per run

### Status (2025-09-25)
- Completed (backend)
  - Tableset rendering (offline) and static mount at `/static/tables` (3-day TTL, disk cap)
  - Mapping JSON per table with: rowIndex/colIndex, rowLabel/colLabel (fallbacks), per-cell facts, tableLabel
  - Mapping JSON extended with: rowCode/colCode and qualifiers (effective dimension members per cell); codes derived from structural nodes with fallbacks
  - Runs artifacts: `messages.json` persisted per run with assigned message IDs
  - Message enrichment: best-effort `conceptNs`/`conceptLn`; friendly location attached where mapping matches; `tables_index_url` in validate response
  - Endpoints: `/render/tableset`, `/render/table`, `/render/for-error` (returns candidate tables and deep-link URLs), `/runs/{run_id}`, `/runs/{run_id}/message/{message_id}`
  - Error text support in viewer via `errorText` query param and messageId fetch
  - V-code parsing: extract `{TableId,rowCode,colCode}` and canonical `rule_id` (e.g., v0704_m_1); set per-entry `category=formulas` for v-codes; compose `readable_message`

- Completed (UI)
  - `/ui` scaffold with upload/actions (preflight, validation, render), timings, summary counters
  - Messages table with filters; shows severity, category, rule id, and readable_message; clicking a message deep-links to the template viewer with highlights (by concept/context or row/col codes) and error text
  - Template viewer: Excel-like tabs loaded from `tables.json`, lazy-load per-table iframes, highlighting and inline error text panel
  - Messages grid layout uses auto widths and full borders for aligned headers/columns
  - Preflight tab with results summary and items

### Next TODOs (Phase 2)
None critical. Optional enhancements:
- Candidate ranking improvements for ambiguous matches; prefer exact code match > concept/context.
- Additional integration/UI polish tests and docs.
 
### Operations Notes (Prometheus)
- Protect `/metrics` in production via ingress/network policy; default disabled.
- Keep labels bounded; no `run_id`, `file`, or `message_id` labels to avoid cardinality blowups.
- Maintain env parity: enable/disable consistently across dev/stage/prod; keep metric names/types stable.

## Backlog — Prioritized Improvements and Critical Issues (2025-09-26)

### Critical (fix first)
- Concurrency safety of Arelle usage:
  - Run validations under a per-request lock or move execution to a worker/subprocess pool; Arelle `modelManager` is not thread-safe for concurrent loads/validations.
  - Touchpoints: `app/main.py` (global `arelle_service`), `app/services/arelle_service.py` (load/validate entrypoints).
- Secure debug endpoints and CORS:
  - Gate `/api/v1/debug/catalog` and `/api/v1/debug/probe` behind auth/feature flag; restrict `CORS` in production.
  - Touchpoints: `app/main.py` (CORS), `app/api/routes_validation.py` (debug endpoints).
- Enforce upload limits and streaming IO:
  - Enforce `limits.max_upload_mb`, reject early; stream uploads to disk in chunks instead of `await file.read()`.
  - Touchpoint: `app/api/routes_validation.py` (upload handling).
- Wire offline HTTP-attempt tracking:
  - Connect Arelle web fetching to `_record_http_fetch_attempt` so `get_offline_status()` records violations; hook `WebCache`/transform or opener.
  - Touchpoint: `app/services/arelle_service.py`.
- Fix potential NameError in validation profile path:
  - Ensure variables used in the VR fallback branch are defined regardless of profile; avoid referencing undefined `loaded_any` when formulas are disabled.
  - Touchpoint: `app/services/arelle_service.py::validate_instance`.
- Add retention for logs and injected temp files:
  - Apply TTL/size cap to `backend/logs/*.json|*.txt` and injected instance files under `backend/temp/` similar to tables GC.
  - Touchpoints: `app/services/arelle_service.py::_write_validation_logs`, temp file creation in `load_instance`.

### High priority
- Make tableset rendering opt-in:
  - Do not auto-render tables in `/validate`; gate via profile/flag or a dedicated endpoint parameter.
  - Touchpoint: `app/api/routes_validation.py`.
- Config-driven `/taxonomies` and `/profiles`:
  - Serve data from `backend/config/eba_taxonomies.yaml` and `backend/config/app.yaml` instead of placeholders.
  - Touchpoint: `app/api/routes_validation.py`.
- Apply logging and limits from config at startup:
  - Respect `logging.level/file` and `limits.*` from `app.yaml` in `setup_logging()` and request handling.
  - Touchpoints: `app/main.py`, `app/utils/logging.py`.
- Correct DPM detection usage and naming:
  - Use DPM architecture markers/DTS URLs to trigger DTS-first for MICA/RF 4.0; rename `DMPDetectionService` → `DPMDetectionService` for accuracy.
  - Touchpoints: `app/services/dmp_detect.py`, `app/api/routes_validation.py` (MICA detection), references across code.
- Sanity of `offline_roots` paths:
  - Fix fallbacks pointing to non-existent `backend/third_party/xbrl-2003/*` or ensure mirrors exist; prefer Full Taxonomy mirrors already mapped.
  - Touchpoint: `backend/config/app.yaml`.
- Keep instance rewrite dev-only:
  - Default `allow_instance_rewrite` to false for prod profiles; ensure temp-copy injection only under feature flag.
  - Touchpoints: `backend/config/app.yaml`, `app/services/arelle_service.py`.

### Medium priority
- Subprocess worker model and resource guards:
  - Execute validations in short-lived subprocesses with timeouts and RSS caps; centralize spawn interface.
  - Touchpoints: new worker module; integration in routes/service.
- Consolidate message enrichment in service:
  - Move friendly location/v-code enrichment into `ArelleService` behind a flag; persist enriched data in per-run logs.
  - Touchpoints: `app/services/arelle_service.py`, current enrichment in `routes_validation.py`.
- Testing coverage:
  - Add tests for parallel validations (concurrency), retention/GC for `logs` and `temp`, and negative tests asserting offline invariants (no HTTP attempts recorded).
  - Touchpoints: `backend/tests/*`.
- Optional online mode scaffolding (flagged):
  - Allow-list hosts, timeouts/retries, local web cache, provenance logging; add offline/online parity tests for at least one entrypoint.
  - Touchpoints: `app/services/arelle_service.py`, config, tests.

### Low priority
- Naming and documentation polish:
  - DPM vs DMP consistency, README examples to reflect config-driven endpoints, label dev-only endpoints; minor import deduplication and cleanup in routes.
  - Touchpoints: `backend/README.md`, `app/api/routes_validation.py`.

### Acceptance notes (for the above backlog)
- Offline determinism preserved by default; any optional online behavior guarded by explicit flags and allow-lists.
- Single resolver seam maintained (packages + catalogs + config offline roots).
- No mutation of uploads; any schema injection occurs via temp copies under `backend/temp/` and is feature-flagged.
- Observability: emit structured start/complete events with `trace_id`/`run_id`, include offline resolution evidence and storage GC actions.

## Upload Limits & Streaming (2025-09-27)

- Enforcement:
  - App limit via `limits.max_upload_mb` in `backend/config/app.yaml` (default 150 MB)
  - Global early 413 middleware checks `Content-Length` and rejects oversized requests before routing
  - Endpoint-level streaming with hard cap and chunked reads; small uploads use `SpooledTemporaryFile` to minimize disk I/O
- Implementation:
  - Global guard: `app/main.py` adds `MaxBodyByHeaderMiddleware` configured from `app.yaml`
  - Routes: `app/api/routes_validation.py` streams uploads using `_save_upload_streaming` (now with `SpooledTemporaryFile`)
- Edge/Ingress guidance:
  - Nginx: `client_max_body_size 200m;`
  - K8s Nginx Ingress: `nginx.ingress.kubernetes.io/proxy-body-size: "200m"`
  - Ensure edge and app limits match to avoid inconsistent behavior
- Tests to add:
  - Oversized with/without `Content-Length` header → 413
  - Large file memory profile remains bounded; small files benefit from spooling

## Offline Invariant Enforcement (2025-09-27)

- Default: strict offline; any `http/https` attempt fails the run
- Hooks installed in `app/services/arelle_service.py`:
  - `WebCache.TransformURL` for catalog/package and `offline_roots` mappings
  - Interceptors on `webCache.opener.open` (record + block network) and `webCache.getfilename` (allow local mapping; network blocked by opener)
- Metrics & provenance:
  - `metrics.offline_attempt_count` and `metrics.offline_attempted_urls` included in validation results
  - `get_offline_status()` returns offline state and attempts; surfaced in preflight and validate flows
- Notes:
  - Optional online mode (allow-list + timeouts/retries + provenance) remains future work behind a feature flag
  - Parity tests must assert zero attempts in offline mode across entrypoints