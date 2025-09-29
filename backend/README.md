# Backend – EBA XBRL Validator (RF 4.0)

## Run

```bash
cd "/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/backend"
python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
```

## Validate RF 4.0 (examples)

Single file (COREP LR sample from official 4.0 set):

```bash
F="/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_COREP040000_COREPLR_2024-12-31_20241211134749200.xbrl"
curl -s -X POST http://localhost:8001/api/v1/validate \
  -F "file=@$F" -F "profile=full" -F "entrypoint=corep_lr" | jq
```

Loop all files in a folder:

```bash
OUT="/tmp/resp_$$.json"
for f in "/Users/Yoran/Cursor files/XBRL Validator/Context data/Taxonomy documentation/github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files"/*.xbrl; do
  echo "Validating: $f"
  code=$(curl -s -X POST http://localhost:8001/api/v1/validate \
    -F "file=@$f" -F "profile=full" -F "entrypoint=corep_lr" \
    -o "$OUT" -w "%{http_code}")
  echo "HTTP $code"
  if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
    python3 - "$OUT" <<'PY'
import sys, json
r=json.load(open(sys.argv[1],'r',encoding='utf-8',errors='replace'))
print(f"errors={len(r.get('errors',[]))} warnings={len(r.get('warnings',[]))} seen≈{r.get('metrics',{}).get('coverage',{}).get('rule_ids_seen_approx',0)}")
PY
  else
    head -c 400 "$OUT"; echo
  fi
done
```

## List Frameworks & Entrypoints

```bash
curl -s http://localhost:8001/api/v1/taxonomies | jq
```

Notes:
- The endpoint enumerates frameworks from the local Full Taxonomy mirror (present files) and merges additional RF 4.0 entrypoints declared in `backend/config/eba_taxonomies.yaml`.
- If a framework is declared in YAML but missing locally, it will be listed; validation still requires local offline resolution to succeed.

## Baseline (Assertions Coverage)

```bash
python3 backend/scripts/enumerate_assertions.py --entrypoint-id corep_lr --taxonomy-version 4.0.0.0
```

## Troubleshooting

- 500 with "No module named ...": ensure dependencies installed: `pip install -r backend/requirements.txt`.
- "parse_failed" in loops: capture HTTP code with curl `-w` and write body to a temp file, then parse with Python (see loop above).
- Frameworks missing from `/taxonomies`: verify the Full Taxonomy folders exist under `.../crr/fws/<framework>/4.0/mod/`. If not, add entrypoints in `config/eba_taxonomies.yaml` so they appear, then mirror files to enable validation.

## Offline Taxonomy Setup

Edit `backend/config/eba_taxonomies.yaml` to include RF 4.0 packages (directories containing `META-INF/taxonomyPackage.xml`). Example:

  ```yaml
eba:
  rf40:
    packages:
      - github_work/eba-taxonomies/taxonomies/4.0/Full Taxonomy
      - github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Dictionary_4.0.0.0
      - github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0
      - github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Severity_4.0.0.0
    entrypoints:
      - id: corep_lr
        label: COREP Leverage Ratio
        xsd: http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/mod/corep_lr.xsd
```

HTTP is blocked by default; all resolution is via package catalogs.

## Validation API

- POST `/validate` – upload and validate an instance. Response includes `metrics.coverage`.
- GET `/taxonomies`, `/profiles`, `/health`.

## Message Output

- Template header
- Rule (symbolic operator) with column context
- Check (pass/fail)
- Gap (with percent)
- Long RHS collapsed; LR ≥ single‑RHS uses |rhs|
- Zero‑padded row/column codes

## Coverage & Baselines

- Baseline generator: `backend/scripts/enumerate_assertions.py`
  - Example:
    ```bash
    python3 backend/scripts/enumerate_assertions.py --entrypoint-id corep_lr --taxonomy-version 4.0.0.0
    ```
- Runtime coverage appears under `metrics.coverage` and is compared to `backend/config/assertion_baseline.json`.
- Missing/stale baselines trigger background generation on first validate.

## Troubleshooting

- If the server doesn’t start, run uvicorn in the foreground to see the traceback.
- Ensure RF 4.0 package directories contain `META-INF/taxonomyPackage.xml` and `META-INF/catalog.xml`.
