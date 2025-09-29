"""
Validation API Routes

Handles XBRL validation endpoints including file upload, 
taxonomy listing, and validation by entrypoint.
"""

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import JSONResponse
from typing import Optional, List
import tempfile
import json
import uuid
import logging
import uuid
from pathlib import Path
import re
import yaml

from app.models.validation_models import ValidationRequest, ValidationResponse, TaxonomyInfo, PreflightResponse, EntrypointInfo
from app.utils.paths import ensure_upload_path
from app.services.dmp_detect import DMPDetectionService
from app.services.ingest_xml import XMLIngestService
from app.services.arelle_service_templates import render_eba_tableset, render_single_table
from app.utils.retention import gc_tables_dir
from app.utils.progress import ProgressStore

logger = logging.getLogger(__name__)
router = APIRouter()

# Centralized upload limits and streaming helpers
def _max_upload_bytes() -> int:
    try:
        from app.utils.config_loader import load_config
        # Try repo_root/backend/config/app.yaml, then backend/config fallback
        base_repo = Path(__file__).resolve().parents[3]
        cfg_path = base_repo / "backend" / "config" / "app.yaml"
        if not cfg_path.exists():
            # Try backend/config/app.yaml relative to backend
            cfg_path = Path(__file__).resolve().parents[2] / "config" / "app.yaml"
        cfg = load_config(cfg_path)
        mb = int(((cfg.get("limits", {}) or {}).get("max_upload_mb", 150)))
        return mb * 1024 * 1024
    except Exception:
        return 150 * 1024 * 1024

def _early_reject_on_content_length(request: Request, max_bytes: int) -> None:
    cl = request.headers.get("content-length")
    if not cl:
        return
    try:
        if int(cl) > max_bytes:
            raise HTTPException(status_code=413, detail=f"Upload too large (>{max_bytes // (1024*1024)} MB)")
    except ValueError:
        # Ignore malformed header; enforce during streaming
        return

async def _save_upload_streaming(file: UploadFile, dest: Path, max_bytes: int, chunk_size: int = 1024 * 1024) -> None:
    written = 0
    # Use SpooledTemporaryFile so tiny uploads stay in memory, larger spill to disk
    try:
        with tempfile.SpooledTemporaryFile(max_size=min(max_bytes, 8 * 1024 * 1024)) as spool:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise HTTPException(status_code=413, detail=f"Upload too large (>{max_bytes // (1024*1024)} MB)")
                spool.write(chunk)
            spool.seek(0)
            with open(dest, "wb") as buffer:
                while True:
                    data = spool.read(1024 * 1024)
                    if not data:
                        break
                    buffer.write(data)
    except Exception:
        try:
            if dest.exists():
                dest.unlink()
        except Exception:
            pass
        raise
@router.get("/progress")
async def get_progress(request: Request, job_id: str):
    try:
        prog = getattr(request.app.state, 'progress_store', None)
        if not prog:
            raise HTTPException(status_code=503, detail="Progress store not available")
        st = prog.get(job_id)
        if not st:
            raise HTTPException(status_code=404, detail="Job not found")
        return JSONResponse(content=st)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Progress query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Progress query failed: {str(e)}")
@router.post("/preflight", response_model=PreflightResponse)
async def preflight_only(
    request: Request,
    file: UploadFile = File(...),
    light: bool = Form(True),
    client_run_id: Optional[str] = Form(None)
):
    """
    Run pre-flight checks only (no full validation or formula engine).
    Strictly offline.
    """
    try:
        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Save upload
        base_dir = Path(__file__).resolve().parents[2]
        upload_dir = base_dir / "uploads"
        upload_dir.mkdir(exist_ok=True)
        unique_name = f"{Path(file.filename).stem}_{uuid.uuid4().hex[:8]}{Path(file.filename).suffix}"
        upload_path = upload_dir / unique_name
        # Stream upload to disk with early header-based rejection and hard cap
        max_bytes = _max_upload_bytes()
        _early_reject_on_content_length(request, max_bytes)
        await _save_upload_streaming(file, upload_path, max_bytes)

        # Load instance only
        model_xbrl, _ = arelle_service.load_instance(str(upload_path))

        # Run preflight
        from app.services.filing_rules import run_preflight
        offline_status = arelle_service.get_offline_status()
        import time
        t0 = time.time()
        # Progress tracking (coarse-grained)
        prog = getattr(request.app.state, 'progress_store', None)
        job_id = client_run_id or uuid.uuid4().hex
        if prog:
            prog.start(job_id, task="preflight", message="Starting preflight checks")
            prog.update(job_id, 10, "Parsing instance and DTS")
        pf = run_preflight(model_xbrl, {"offline_status": offline_status}, light=light)
        dur = int((time.time() - t0) * 1000)
        if prog:
            prog.update(job_id, 90, "Aggregating results")

        # Optional DTS evidence snapshot
        dts_ev = arelle_service._log_dts_evidence(model_xbrl)

        resp = PreflightResponse(
            passed=pf.get("passed", 0),
            failed=pf.get("failed", 0),
            items=pf.get("items", []),
            offline_status=offline_status,
            duration_ms=dur,
            dts_evidence=dts_ev
        )
        if prog:
            prog.finish(job_id, success=True, message="Preflight complete")
        return resp
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Preflight endpoint failed: {e}")
        try:
            prog = getattr(request.app.state, 'progress_store', None)
            if prog and client_run_id:
                prog.error(client_run_id, message=f"Preflight error: {e}")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Preflight failed: {str(e)}")

@router.post("/validate", response_model=ValidationResponse)
async def validate_filing(
    request: Request,
    file: UploadFile = File(...),
    profile: Optional[str] = Form("fast"),
    entrypoint: Optional[str] = Form(None),
    client_run_id: Optional[str] = Form(None)
):
    """
    Upload and validate an XBRL filing.
    
    Supports multipart file upload with auto-detection of format.
    Runs validation with specified profile (fast/full/debug).
    """
    try:
        logger.info(f"Received validation request for file: {file.filename}")

        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Save upload under backend/uploads
        base_dir = Path(__file__).resolve().parents[2]  # backend/
        upload_dir = base_dir / "uploads"
        upload_dir.mkdir(exist_ok=True)
        unique_name = f"{Path(file.filename).stem}_{uuid.uuid4().hex[:8]}{Path(file.filename).suffix}"
        upload_path = upload_dir / unique_name
        # Stream upload to disk with early header-based rejection and hard cap
        max_bytes = _max_upload_bytes()
        _early_reject_on_content_length(request, max_bytes)
        await _save_upload_streaming(file, upload_path, max_bytes)
        logger.info(f"Saved uploaded file to: {upload_path}")

        # Progress
        prog = getattr(request.app.state, 'progress_store', None)
        job_id = client_run_id or uuid.uuid4().hex
        if prog:
            prog.start(job_id, task="validate", message="Uploading and preflighting")

        # Perform XML preflight checks
        try:
            xml_service = XMLIngestService()
            preflight_result = xml_service.preflight_check(str(upload_path))
            logger.info(f"XML preflight result: {preflight_result}")
            
            # Check if instance rewrite is enabled to allow dictionary injection
            allow_instance_rewrite = arelle_service._config.get('allow_instance_rewrite', False)
            logger.info(f"Instance rewrite enabled: {allow_instance_rewrite}")
            
            if preflight_result.get("status") != "success":
                error_msg = preflight_result.get("error", "XML preflight check failed")
                
                # If instance rewrite is enabled and this is a dictionary schema issue, allow it to proceed
                if (allow_instance_rewrite and 
                    preflight_result.get("eba_met_usage", False) and 
                    not preflight_result.get("dict_met_referenced", False)):
                    logger.info("Instance rewrite enabled - allowing dictionary injection to proceed")
                else:
                    logger.error(f"XML preflight failed: {error_msg}")
                    raise HTTPException(status_code=400, detail=f"XML preflight check failed: {error_msg}")
                
        except Exception as e:
            logger.error(f"XML preflight check failed: {e}")
            raise HTTPException(status_code=400, detail=f"XML preflight check failed: {str(e)}")

        # Detect DPM version (heuristic)
        try:
            dmp = DMPDetectionService().detect_dmp_version(str(upload_path))
            dpm_version = dmp.get("dmp_version", "unknown")
            logger.info(f"DPM detection result: {dmp}")
        except Exception as e:
            logger.error(f"DPM detection failed: {e}")
            dpm_version = "unknown"

        if prog:
            prog.update(job_id, 25, "Loading instance")

        # Detect if this is a MICA file and use DTS-first loading
        dts_first_schemas = None
        if dpm_version == "rf40" or "MICA" in str(upload_path).upper():
            logger.info("Detected RF 4.0 MICA file, enabling DTS-first loading")
            dts_first_schemas = [
                "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/mica/4.0/mod/mica_its.xsd",
                "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
            ]
            logger.info(f"DTS-first schemas: {dts_first_schemas}")

        # Load and validate (subprocess isolation for concurrency safety)
        try:
            if prog:
                prog.update(job_id, 55, "Running validation")
            # Prepare subprocess task payload
            from app.services.validation_worker import run_validation_task
            from concurrent.futures import ProcessPoolExecutor
            # Compose config subset safe to send to child process
            child_config = dict(arelle_service._config or {})
            # Attach cache_dir explicitly for child
            child_config["cache_dir"] = str((base_dir / "cache").resolve())
            # Package paths for child process
            pkg_paths = []
            try:
                cfg_path = Path(__file__).resolve().parents[3] / "backend" / "config" / "eba_taxonomies.yaml"
                import yaml as _yaml
                with open(cfg_path, "r") as _f:
                    tax_cfg = _yaml.safe_load(_f) or {}
                pkgs = (tax_cfg.get("eba", {}) or {}).get("rf40", {}).get("packages", []) or []
                pkg_paths = [str((Path(__file__).resolve().parents[3] / p).resolve()) for p in pkgs]
            except Exception:
                pkg_paths = []

            task = {
                "file_path": str(upload_path),
                "profile": profile,
                "config": child_config,
                "package_paths": pkg_paths,
                "dts_first_schemas": dts_first_schemas,
            }
            # Single-process pool for isolation; scale to N later if desired
            with ProcessPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(run_validation_task, task)
                results = fut.result()
            logger.info(f"Validation results received (status={results.get('status')}): counts e={len(results.get('errors',[]))}, w={len(results.get('warnings',[]))}")
        except Exception as e:
            logger.error(f"Validation failed (subprocess): {e}")
            raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

        # Shape response
        trace_id = uuid.uuid4().hex
        run_id = uuid.uuid4().hex

        # Resolve catalog messages like message:v4460_m_0 using configured catalog
        try:
            mc = getattr(request.app.state, 'message_catalog', None)
            # Read flags for message handling
            msgs_cfg = ((getattr(request.app.state, 'arelle_service', None) or {}).get('_config', {}) if hasattr(request.app.state, 'arelle_service') else {})
            try:
                if hasattr(request.app.state, 'arelle_service'):
                    msgs_cfg = (request.app.state.arelle_service._config.get('features', {}) or {}).get('messages', {}) or {}
                else:
                    msgs_cfg = {}
            except Exception:
                msgs_cfg = {}
            hide_raw_keys = bool((msgs_cfg or {}).get('hide_raw_keys', True))
            # basic counters without prometheus; we can swap later
            resolved_count = 0
            unresolved_count = 0

            # Iterate all messages once for raw preservation and token extraction (independent of catalog availability)
            for key in ("errors", "warnings"):
                for e in results.get(key, []) or []:
                    msg_key = e.get('message') or ''
                    # Preserve original raw message before any mutation
                    try:
                        if 'raw_message' not in e or not e.get('raw_message'):
                            e['raw_message'] = msg_key
                    except Exception:
                        pass
                    # Extract rule_id from token if present
                    try:
                        import re as _re_tok
                        m_tok = _re_tok.search(r"message:([A-Za-z0-9_\-.]+)", msg_key)
                        if m_tok and (not e.get('rule_id')):
                            e['rule_id'] = m_tok.group(1)
                            if e['rule_id'].startswith('v'):
                                e['category'] = e.get('category') or 'formulas'
                    except Exception:
                        pass

            # Catalog resolve if available
            if mc:
                for key in ("errors", "warnings"):
                    for e in results.get(key, []) or []:
                        msg_key = e.get('message') or ''
                        params = {
                            'table_id': e.get('table_id') or '',
                            'rowCode': e.get('rowCode') or e.get('rowDisplayCode') or '',
                            'colCode': e.get('colCode') or e.get('colDisplayCode') or '',
                            'conceptNs': e.get('conceptNs') or '',
                            'conceptLn': e.get('conceptLn') or '',
                            'contextRef': e.get('contextRef') or '',
                        }
                        # Try direct key, then search inside the text for a message: token
                        resolved = mc.resolve(msg_key, params=params)
                        if not resolved:
                            try:
                                import re as _re
                                m = _re.search(r"message:([A-Za-z0-9_\-.]+)", msg_key)
                                if m:
                                    resolved = mc.resolve(f"message:{m.group(1)}", params=params)
                                    e['rule_id'] = e.get('rule_id') or m.group(1)
                            except Exception:
                                pass
                        if resolved:
                            e['catalog_message'] = resolved
                            resolved_count += 1
                        else:
                            if msg_key.startswith('message:'):
                                unresolved_count += 1
                                if hide_raw_keys:
                                    rid_fallback = e.get('rule_id') or msg_key.split(':',1)[-1] or 'unknown'
                                    e['message'] = f"Validation rule {rid_fallback} failed"
                            else:
                                if hide_raw_keys:
                                    try:
                                        import re as _re2
                                        e['message'] = _re2.sub(r"message:[A-Za-z0-9_\-.]+", "", msg_key).strip()
                                    except Exception:
                                        pass

            # attach simple metrics into results
            results.setdefault('metrics', {})['messages_resolved_count'] = (
                results.get('metrics', {}).get('messages_resolved_count', 0) + resolved_count
            )
            results.setdefault('metrics', {})['messages_unresolved_count'] = (
                results.get('metrics', {}).get('messages_unresolved_count', 0) + unresolved_count
            )
            try:
                mx = getattr(request.app.state, 'metrics', None)
                if mx:
                    mx.inc_messages_resolved(resolved_count)
                    mx.inc_messages_unresolved(unresolved_count)
            except Exception:
                logger.debug("Prometheus increment failed", exc_info=True)
        except Exception:
            logger.debug("Catalog message resolution skipped due to error", exc_info=True)

        if prog:
            prog.update(job_id, 80, "Rendering templates")
        # Optionally render tableset after validation for convenience
        tables_index_url = None
        try:
            from app.services.arelle_service_templates import render_eba_tableset
            tables_root = base_dir / "temp" / "tables" / run_id
            tables_root.mkdir(parents=True, exist_ok=True)
            # Reload a fresh model for rendering only (keep validation isolated in subprocess)
            model_xbrl_render = None
            try:
                model_xbrl_render, _fc = arelle_service.load_instance(str(upload_path), dts_first_schemas=dts_first_schemas)
                index_path = render_eba_tableset(model_xbrl_render, tables_root, lang="en")
            finally:
                try:
                    if model_xbrl_render is not None:
                        model_xbrl_render.close()
                except Exception:
                    pass
            tables_index_url = f"/static/tables/{run_id}/index.html"
            logger.info(f"Rendered tableset for run {run_id}: {tables_index_url}")
            # Enrich messages with friendly location using mapping JSON
            try:
                # Build quick indexes from mapping.json files
                # 1) (conceptNs,conceptLn,contextRef) -> [{table_id,rowLabel,colLabel}]
                mapping_index = {}
                # 2) Per-table display-code (rowDisplayCode,colDisplayCode) -> labels
                mapping_by_display = {}
                # 3) Per-table column display code -> column label (robust fallback)
                mapping_col_label = {}
                # 4) Per-table column index -> column label (derived from mapping cells)
                mapping_col_by_index = {}
                # 5) Per-table row index -> row label (derived from mapping cells)
                mapping_row_by_index = {}
                # 6) Table id/name -> table label
                table_label_map = {}
                for mp in tables_root.glob('*.mapping.json'):
                    try:
                        m = json.loads(mp.read_text(encoding='utf-8'))
                        table_id = m.get('tableId') or mp.stem.replace('.mapping','')
                        table_name = m.get('tableName') or ''
                        table_label = m.get('tableLabel') or ''
                        if table_label:
                            table_label_map[table_id] = table_label
                            if table_name:
                                table_label_map[table_name] = table_label
                        for cell in m.get('cells', []) or []:
                            row_label = cell.get('rowLabel','')
                            col_label = cell.get('colLabel','')
                            rdc = str(cell.get('rowDisplayCode','')) if cell.get('rowDisplayCode') is not None else ''
                            cdc = str(cell.get('colDisplayCode','')) if cell.get('colDisplayCode') is not None else ''
                            # Map column index -> label (first non-empty wins)
                            try:
                                ci = int(cell.get('colIndex'))
                                mapping_col_by_index.setdefault(table_id, {})
                                if col_label and ci not in mapping_col_by_index[table_id]:
                                    mapping_col_by_index[table_id][ci] = col_label
                                if table_name:
                                    mapping_col_by_index.setdefault(table_name, {})
                                    if col_label and ci not in mapping_col_by_index[table_name]:
                                        mapping_col_by_index[table_name][ci] = col_label
                                # Map row index -> label
                                ri = int(cell.get('rowIndex'))
                                mapping_row_by_index.setdefault(table_id, {})
                                if row_label and ri not in mapping_row_by_index[table_id]:
                                    mapping_row_by_index[table_id][ri] = row_label
                                if table_name:
                                    mapping_row_by_index.setdefault(table_name, {})
                                    if row_label and ri not in mapping_row_by_index[table_name]:
                                        mapping_row_by_index[table_name][ri] = row_label
                            except Exception:
                                pass
                            if rdc or cdc:
                                mapping_by_display.setdefault(table_id, {})[(rdc, cdc)] = {
                                    'rowLabel': row_label,
                                    'colLabel': col_label,
                                    'rowDisplayCode': rdc,
                                    'colDisplayCode': cdc,
                                }
                                if table_name:
                                    mapping_by_display.setdefault(table_name, {})[(rdc, cdc)] = {
                                        'rowLabel': row_label,
                                        'colLabel': col_label,
                                        'rowDisplayCode': rdc,
                                        'colDisplayCode': cdc,
                                    }
                            if cdc:
                                mapping_col_label.setdefault(table_id, {})[cdc] = col_label
                                if table_name:
                                    mapping_col_label.setdefault(table_name, {})[cdc] = col_label
                            for f in cell.get('facts', []) or []:
                                key = (
                                    f.get('conceptNamespace') or '',
                                    f.get('conceptLocalName') or '',
                                    f.get('contextRef') or ''
                                )
                                mapping_index.setdefault(key, []).append({
                                    'table_id': table_id,
                                    'rowLabel': row_label,
                                    'colLabel': col_label,
                                    'rowDisplayCode': rdc,
                                    'colDisplayCode': cdc,
                                })
                    except Exception:
                        continue
                def attach_friendly(entries):
                    for e in entries:
                        ns = e.get('conceptNs') or ''
                        ln = e.get('conceptLn') or ''
                        cx = e.get('contextRef') or ''
                        # v-code based lookup by table_id/rowCode/colCode first
                        if e.get('table_id') and (e.get('rowCode') or e.get('colCode')):
                            try:
                                mp = tables_root / f"{e['table_id']}.mapping.json"
                                if mp.exists():
                                    mloc = json.loads(mp.read_text(encoding='utf-8'))
                                    for cell in mloc.get('cells', []) or []:
                                        rc_ok = str(cell.get('rowCode',''))==str(e.get('rowCode','')) if e.get('rowCode') else True
                                        cc_ok = str(cell.get('colCode',''))==str(e.get('colCode','')) if e.get('colCode') else True
                                        if rc_ok and cc_ok:
                                            e['rowLabel'] = e.get('rowLabel') or cell.get('rowLabel')
                                            e['colLabel'] = e.get('colLabel') or cell.get('colLabel')
                                            e['qualifiers'] = e.get('qualifiers') or cell.get('qualifiers')
                                            break
                            except Exception:
                                pass
                        if not (ns and ln):
                            continue
                        matches = mapping_index.get((ns, ln, cx)) or mapping_index.get((ns, ln, '')) or []
                        if matches:
                            best = matches[0]
                            e['table_id'] = best.get('table_id')
                            e['rowLabel'] = e.get('rowLabel') or best.get('rowLabel')
                            e['colLabel'] = e.get('colLabel') or best.get('colLabel')
                def build_readable(entries):
                    import re
                    code_pat = re.compile(r"\{([A-Za-z0-9_.]+),\s*(\d{3,4}),\s*(\d{3,4}),\s*\}")
                    def _to_int_rb(s: str) -> int:
                        try:
                            import re as _re_num
                            from decimal import Decimal, ROUND_HALF_UP, getcontext
                            getcontext().prec = 34
                            raw = str(s).strip() if s is not None else ''
                            if not raw:
                                return 0
                            t = raw.replace('\u00A0', ' ').strip()
                            # Normalize unicode minus and remove grouping spaces
                            t = t.replace('−', '-')
                            t = t.replace(' ', '')
                            if t.startswith('≈'):
                                t = t[1:].strip()
                            # Parentheses negatives
                            if t.startswith('(') and t.endswith(')'):
                                t = '-' + t[1:-1]
                            m = _re_num.match(r'^([+\-]?\d+(?:[\.,]\d+)?)\s*×\s*10\^(?:\+)?([+\-]?\d+)$', t)
                            if m:
                                mant = m.group(1).replace(',', '.')
                                exp = int(m.group(2))
                                if abs(exp) > 12:
                                    return 0
                                val = Decimal(mant) * (Decimal(10) ** exp)
                                if val.copy_abs() > (Decimal(10) ** 12):
                                    return 0
                                return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                            m = _re_num.match(r'^([+\-]?\d+(?:[\.,]\d+)?)\s*[eE]\s*([+\-]?\d+)$', t)
                            if m:
                                mant = m.group(1).replace(',', '.')
                                exp = int(m.group(2))
                                if abs(exp) > 12:
                                    return 0
                                val = Decimal(mant) * (Decimal(10) ** exp)
                                if val.copy_abs() > (Decimal(10) ** 12):
                                    return 0
                                return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                            if _re_num.match(r'^\d{1,3}(?:\.\d{3})+(?:,\d+)?$', t):
                                dt = t.replace('.', '').replace(',', '.')
                                val = Decimal(dt)
                                return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                            if _re_num.match(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$', t):
                                dt = t.replace(',', '')
                                val = Decimal(dt)
                                return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                            if _re_num.match(r'^[+\-]?\d+[\.,]\d+$', t):
                                dt = t.replace(',', '.')
                                val = Decimal(dt)
                                return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                            if _re_num.match(r'^[+\-]?\d+$', t):
                                if len(t.lstrip('+-')) > 15:
                                    return 0
                                return int(t)
                            return 0
                        except Exception:
                            return 0
                    def fmt_num(n: int) -> str:
                        try:
                            return f"{int(n):,}"
                        except Exception:
                            return str(n)
                    def get_col_label(table_key: str, display_code: str) -> str:
                        def resolve_for(tbl: str) -> str:
                            # Attempt direct mapping via display tuple if available
                            try:
                                by_disp = mapping_by_display.get(tbl, {}) or {}
                                for (_rdc_any, cdc_any), info in by_disp.items():
                                    if cdc_any and cdc_any == str(display_code):
                                        lbl = info.get('colLabel') or ''
                                        if lbl:
                                            return lbl
                            except Exception:
                                pass
                            # Fallback: explicit col display code -> label map
                            try:
                                lbl = (mapping_col_label.get(tbl, {}) or {}).get(str(display_code), '')
                                if lbl:
                                    return lbl
                            except Exception:
                                pass
                            # Fallback: derive index from display code (0010->0, 0020->1, ...)
                            try:
                                idx = max(0, int(str(display_code)) // 10 - 1)
                                lbl = (mapping_col_by_index.get(tbl, {}) or {}).get(idx, '')
                                if lbl:
                                    return lbl
                            except Exception:
                                pass
                            return ''
                        # Try provided key
                        lbl0 = resolve_for(table_key)
                        if lbl0:
                            return lbl0
                        # Try one alternate variant without recursion
                        if table_key.startswith('eba_t'):
                            return resolve_for(table_key[len('eba_t'):])
                        else:
                            return resolve_for('eba_t' + table_key)
                    def get_row_label(table_key: str, display_code: str) -> str:
                        def resolve_for(tbl: str) -> str:
                            # Prefer display tuple when available
                            try:
                                by_disp = mapping_by_display.get(tbl, {}) or {}
                                for (rdc_any, _cdc_any), info in by_disp.items():
                                    if rdc_any and rdc_any == str(display_code):
                                        lbl = info.get('rowLabel') or ''
                                        if lbl:
                                            return lbl
                            except Exception:
                                pass
                            # Fallback: derive index from display code (0010->0, 0020->1, ...)
                            try:
                                idx = max(0, int(str(display_code)) // 10 - 1)
                                lbl = (mapping_row_by_index.get(tbl, {}) or {}).get(idx, '')
                                if lbl:
                                    return lbl
                            except Exception:
                                pass
                            return ''
                        lbl0 = resolve_for(table_key)
                        if lbl0:
                            return lbl0
                        if table_key.startswith('eba_t'):
                            return resolve_for(table_key[len('eba_t'):])
                        else:
                            return resolve_for('eba_t' + table_key)

                    def replace_codes_with_labels(text: str) -> str:
                        if not text:
                            return text
                        def _repl(mo):
                            tbl = mo.group(1)
                            rdc = mo.group(2)
                            cdc = mo.group(3)
                            # Resolve labels best-effort
                            rl = get_row_label(tbl, rdc)
                            cl = get_col_label(tbl, cdc)
                            if rl or cl:
                                rdc_pad = rdc.zfill(4) if rdc.isdigit() else rdc
                                cdc_pad = cdc.zfill(3) if cdc.isdigit() else cdc
                                return f"{{{tbl},{rdc_pad} \"{rl}\",{cdc_pad} \"{cl}\",}}"
                            return mo.group(0)
                        try:
                            return code_pat.sub(_repl, text)
                        except Exception:
                            return text
                    def short_label(lbl: str) -> str:
                        try:
                            if not lbl:
                                return ''
                            parts = [p.strip() for p in lbl.split('/') if p and p.strip()]
                            cand = parts[-1] if parts else lbl
                            # Collapse repeated whitespace and stray punctuation
                            cand = ' '.join(cand.split())
                            cand = cand.strip(' "')
                            return cand
                        except Exception:
                            return lbl or ''
                    def html_escape(s: str) -> str:
                        if s is None:
                            return ''
                        return (
                            str(s)
                            .replace('&', '&amp;')
                            .replace('<', '&lt;')
                            .replace('>', '&gt;')
                            .replace('"', '&quot;')
                            .replace("'", '&#39;')
                        )
                    for e in entries:
                        raw = e.get('message','') or ''
                        # Attempt to derive header context from first v-code occurrence
                        header = ''
                        q = e.get('qualifiers') or []
                        qtxt = ''
                        if q:
                            try:
                                qtxt = ' | ' + '; '.join([f"{i.get('dimension')}: {i.get('member')}" for i in q if i])
                            except Exception:
                                qtxt = ''
                        try:
                            first = next(code_pat.finditer(raw), None)
                            if first:
                                tbl, rdc, cdc = first.group(1), first.group(2), first.group(3)
                                rl = get_row_label(tbl, rdc)
                                cl = get_col_label(tbl, cdc)
                                parts = []
                                if tbl:
                                    tlabel = table_label_map.get(tbl, '')
                                    parts.append(f"Template {tbl}" + (f" - \"{tlabel}\"" if tlabel else ""))
                                if rl:
                                    parts.append(f"Row {rdc.zfill(4) if rdc.isdigit() else rdc} \"{rl}\"")
                                if cl:
                                    parts.append(f"Column {cdc.zfill(3) if cdc.isdigit() else cdc} \"{cl}\"")
                                header = ' — '.join([p for p in parts if p]) + qtxt
                        except Exception:
                            header = ''
                        # Build structured lines
                        template_line = header.split(' — ')[0] if header else ''
                        rule_line = ''
                        found_line = ''
                        fix_line = ''

                        # Rule line from tokens and operator
                        try:
                            toks = list(code_pat.findall(raw))  # [(tbl, rdc, cdc), ...]
                            op = None
                            m_op = re.search(r"}\s*(=|>=|<=|>|<)\s*{", raw)
                            if m_op:
                                op = m_op.group(1)
                            # Secondary operator detection from catalog or explicit rule text
                            if not op:
                                try:
                                    scan_txt = (e.get('catalog_message') or '') + ' ' + (header or '')
                                    if '≥' in scan_txt or ' must be ≥' in scan_txt:
                                        op = '>='
                                    elif '≤' in scan_txt or ' must be ≤' in scan_txt:
                                        op = '<='
                                    elif ' must equal' in scan_txt or '=' in scan_txt:
                                        op = '='
                                    elif ' must be >' in scan_txt or '>' in scan_txt:
                                        op = '>'
                                    elif ' must be <' in scan_txt or '<' in scan_txt:
                                        op = '<'
                                except Exception:
                                    pass
                            # Tertiary operator detection from failure clause if present
                            if not op:
                                try:
                                    m_fail_op = re.search(r"Fails because\s+([^\n\.]+?)(?:\s+is not true|\.)", raw)
                                    clause0 = m_fail_op.group(1) if m_fail_op else ''
                                    if ('≥' in clause0) or ('>=' in clause0):
                                        op = '>='
                                    elif ('≤' in clause0) or ('<=' in clause0):
                                        op = '<='
                                    elif (' = ' in clause0):
                                        op = '='
                                    elif (' > ' in clause0):
                                        op = '>'
                                    elif (' < ' in clause0):
                                        op = '<'
                                except Exception:
                                    pass
                            op_txt = {
                                '=': 'must equal',
                                '>=': 'must be ≥',
                                '<=': 'must be ≤',
                                '>': 'must be >',
                                '<': 'must be <',
                            }.get(op, 'must satisfy the defined relation')
                            if toks:
                                tbl0, rdc0, _cdc0 = toks[0]
                                rl0 = short_label(get_row_label(tbl0, rdc0)) or ''
                                rhs_rows = []
                                rhs_codes_labels = []
                                for (_t, rdcx, _c) in toks[1:]:
                                    rlx = short_label(get_row_label(tbl0, rdcx)) or ''
                                    rhs_rows.append(f"Row {rdcx}" + (f" \"{rlx}\"" if rlx else ""))
                                    rhs_codes_labels.append((rdcx, rlx))
                                lhs_txt = f"Row {rdc0}" + (f" \"{rl0}\"" if rl0 else "")
                                if rhs_rows:
                                    # Special concise phrasing for base ≥ |deduction|
                                    if op_txt == 'must be ≥' and len(rhs_codes_labels) == 1:
                                        rc1, rl1 = rhs_codes_labels[0]
                                        lhs_disp = f"Row {rdc0.zfill(4) if rdc0.isdigit() else rdc0}" + (f" – {rl0}" if rl0 else "")
                                        rhs_disp = f"Row {rc1.zfill(4) if rc1.isdigit() else rc1}" + (f" – {rl1}" if rl1 else "")
                                        rule_line = f"Rule: {lhs_disp} must be ≥ |{rhs_disp}| (deduction magnitude)"
                                    else:
                                        if (op is None or op_txt == 'must satisfy the defined relation') and len(rhs_codes_labels) > 1:
                                            rule_line = f"Rule: {lhs_txt} must satisfy the defined relation over the sum of related rows"
                                        else:
                                            rule_line = f"Rule: {lhs_txt} {op_txt} " + " + ".join(rhs_rows)
                        except Exception:
                            pass

                        # Found line with diff and percentage when meaningful
                        try:
                            # Restrict to the failure clause to avoid picking up timestamps/line numbers
                            m_fail = re.search(r"Fails because\s+([^\n\.]+?)(?:\s+is not true|\.)", raw)
                            clause = m_fail.group(1) if m_fail else ''
                            # Extract robust numeric tokens (EU/US grouping, decimals, scientific, ×10^)
                            tok_pat = re.compile(r"[+\-]?(?:\d{1,3}(?:[\.,\s]\d{3})+|\d+)(?:[\.,]\d+)?(?:\s*[eE]\s*[+\-]?\d+|\s*×\s*10\^\s*[+\-]?\d+)?")
                            nums = []
                            for tok in tok_pat.findall(clause or ''):
                                v = _to_int_rb(tok)
                                nums.append(v)
                            if len(nums) >= 2:
                                lhs = nums[0]
                                rhs_terms = nums[1:]
                                diff = lhs - sum(rhs_terms)
                                rhs_disp = (
                                    f"sum of {len(rhs_terms)} rows: {fmt_num(sum(rhs_terms))}"
                                    if len(rhs_terms) > 5
                                    else " + ".join(fmt_num(v) for v in rhs_terms)
                                )
                                pct_txt = ''
                                # Mirror |rhs| denominator rule for ≥/≤ with single RHS
                                try:
                                    if len(rhs_terms) == 1 and op_txt in ('must be ≥', 'must be ≤'):
                                        denom = abs(rhs_terms[0])
                                    else:
                                        denom = sum(rhs_terms)
                                except Exception:
                                    denom = sum(rhs_terms)
                                if denom:
                                    pct = (diff/denom)*100.0
                                    if abs(pct) >= 0.1:
                                        pct_txt = f" ({pct:+.1f}%)"
                                found_line = f"Found: {fmt_num(lhs)} vs {rhs_disp} (difference {diff:+,})." + (pct_txt or '')
                        except Exception:
                            pass
                        # Fix line when we can infer LHS and components
                        try:
                            toks = list(code_pat.findall(raw))
                            if toks:
                                tbl, rdc0, cdc0 = toks[0]
                                col_label0 = get_col_label(tbl, cdc0)
                                row_label0 = short_label(get_row_label(tbl, rdc0))
                                rhs_rows = [(rdc, short_label(get_row_label(tbl, rdc))) for (_t, rdc, _c) in toks[1:]]
                                if rhs_rows:
                                    cdc_disp = cdc0.zfill(3) if cdc0.isdigit() else cdc0
                                    rdc0_disp = rdc0.zfill(4) if rdc0.isdigit() else rdc0
                                    col_hint = f"Column {cdc_disp} \"{col_label0}\"" if col_label0 else f"Column {cdc_disp}"
                                    lhs_txt = f"Row {rdc0_disp}" + (f" \"{row_label0}\"" if row_label0 else "")
                                    def pad_row_code(code: str) -> str:
                                        return code.zfill(4) if code.isdigit() else code
                                    rhs_txt = ", ".join([f"Row {pad_row_code(r)}" + (f" \"{rl}\"" if rl else "") for (r, rl) in rhs_rows])
                                    fix_line = f"Fix: Check the figures under {col_hint} and correct either the total ({lhs_txt}) or one/both components ({rhs_txt})."
                        except Exception:
                            pass
                        # Compose final structured message
                        # Compose HTML with bold labels, remove source/url traces
                        parts_out = []
                        # Place catalog-resolved message first if available
                        try:
                            cat = e.get('catalog_message')
                            if cat:
                                parts_out.append(f"<strong>Message</strong> {html_escape(cat)}")
                        except Exception:
                            pass
                        if template_line:
                            # Rebuild header using Template keyword
                            tpl_txt = template_line
                            parts_out.append(f"<strong>Template</strong> {html_escape(tpl_txt.replace('Template ', '')) if tpl_txt.startswith('Template ') else html_escape(tpl_txt)}")
                        if rule_line:
                            parts_out.append(f"<strong>Rule</strong> {html_escape(rule_line.replace('Rule: ', ''))}")
                        if found_line:
                            parts_out.append(f"<strong>Found</strong> {html_escape(found_line.replace('Found: ', ''))}")
                        if fix_line:
                            parts_out.append(f"<strong>Fix</strong> {html_escape(fix_line.replace('Fix: ', ''))}")
                        final_msg = "<br>".join([p for p in parts_out if p])
                        # If hiding raw keys and no catalog text, strip any message:v#### tokens from fallback
                        try:
                            if hide_raw_keys and not e.get('catalog_message'):
                                import re as _re3
                                cleaned = _re3.sub(r"message:[A-Za-z0-9_\-.]+", "", final_msg or html_escape(raw)).strip()
                                final_msg = cleaned
                        except Exception:
                            pass

                        # Replace base message; keep original for traceability
                        e['raw_message'] = raw
                        # Prefer finalized message (sanitized); if empty, keep as empty rather than raw
                        e['message'] = final_msg or ""
                        e['readable_message'] = e['message']
                attach_friendly(results.get('errors', []))
                attach_friendly(results.get('warnings', []))
                build_readable(results.get('errors', []))
                build_readable(results.get('warnings', []))
                # Pre-prune obvious nonactionable entries before concise build
                try:
                    import re as _re_pre
                    def _pre_keep(e):
                        if e.get('rule_id'):
                            return True
                        msg = (e.get('message') or '')
                        txt = _re_pre.sub(r"<[^>]+>", " ", msg)
                        if ('Rule' in txt and 'Found' in txt and _re_pre.search(r"\d", txt)):
                            return True
                        # allow if we have table/row/col context
                        if e.get('table_id') and (e.get('rowCode') or e.get('colCode')):
                            return True
                        return False
                    results['errors'] = [e for e in (results.get('errors', []) or []) if _pre_keep(e)]
                    results['warnings'] = [w for w in (results.get('warnings', []) or []) if _pre_keep(w)]
                except Exception:
                    pass
                # Build concise, user-friendly messages
                def build_concise(entries):
                    try:
                        import re as _re_c
                        import html as _html_c
                        def _clean_label(lbl: str) -> str:
                            if not lbl:
                                return ''
                            parts = [p.strip() for p in lbl.split('/') if p and p.strip()]
                            txt = parts[-1] if parts else lbl
                            txt = ' '.join(txt.split())
                            return txt.strip(' "')
                        def _to_int(s: str) -> int:
                            try:
                                import re as _re_num
                                from decimal import Decimal, ROUND_HALF_UP, getcontext
                                getcontext().prec = 34
                                raw = str(s).strip() if s is not None else ''
                                if not raw:
                                    return 0
                                # normalize spaces and unicode minus, remove grouping spaces
                                t = raw.replace('\u00A0', ' ').strip()
                                t = t.replace('−', '-')
                                t = t.replace(' ', '')
                                # parentheses negatives
                                if t.startswith('(') and t.endswith(')'):
                                    t = '-' + t[1:-1]
                                # strip leading approximation symbol
                                if t.startswith('≈'):
                                    t = t[1:].strip()
                                # Handle a×10^b format
                                m = _re_num.match(r'^([+\-]?\d+(?:[\.,]\d+)?)\s*×\s*10\^(?:\+)?([+\-]?\d+)$', t)
                                if m:
                                    mant = m.group(1).replace(',', '.')
                                    exp = int(m.group(2))
                                    if abs(exp) > 12:
                                        return 0
                                    val = Decimal(mant) * (Decimal(10) ** exp)
                                    if val.copy_abs() > (Decimal(10) ** 12):
                                        return 0
                                    return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                                # Handle scientific E notation
                                m = _re_num.match(r'^([+\-]?\d+(?:[\.,]\d+)?)\s*[eE]\s*([+\-]?\d+)$', t)
                                if m:
                                    mant = m.group(1).replace(',', '.')
                                    exp = int(m.group(2))
                                    if abs(exp) > 12:
                                        return 0
                                    val = Decimal(mant) * (Decimal(10) ** exp)
                                    if val.copy_abs() > (Decimal(10) ** 12):
                                        return 0
                                    return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                                # EU grouping: 68.000,50
                                if _re_num.match(r'^\d{1,3}(?:\.\d{3})+(?:,\d+)?$', t):
                                    dt = t.replace('.', '').replace(',', '.')
                                    val = Decimal(dt)
                                    return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                                # US grouping: 68,000.50
                                if _re_num.match(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$', t):
                                    dt = t.replace(',', '')
                                    val = Decimal(dt)
                                    return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                                # Plain decimal with , or .
                                if _re_num.match(r'^[+\-]?\d+[\.,]\d+$', t):
                                    dt = t.replace(',', '.')
                                    val = Decimal(dt)
                                    return int(val.to_integral_value(rounding=ROUND_HALF_UP))
                                # Pure integer
                                if _re_num.match(r'^[+\-]?\d+$', t):
                                    # avoid absurd magnitudes from corrupted tokens
                                    if len(t.lstrip('+-')) > 15:
                                        return 0
                                    return int(t)
                                return 0
                            except Exception:
                                return 0
                        def _fmt_hnum(n: int) -> str:
                            try:
                                if n is None:
                                    return ''
                                n = int(n)
                                if n == 0:
                                    return '0'
                                absn = abs(n)
                                # Use scientific for very large numbers
                                if absn >= 10**9:
                                    import math
                                    exp = int(math.floor(math.log10(absn)))
                                    mant = absn / (10**exp)
                                    # 3 sig figs
                                    mant_s = f"{mant:.2f}".rstrip('0').rstrip('.')
                                    sign = '-' if n < 0 else ''
                                    return f"{sign}≈ {mant_s}×10^{exp}"
                                # else thousands grouping
                                return f"{n:,}"
                            except Exception:
                                return str(n)
                        def _median(vals):
                            if not vals:
                                return 0
                            v = sorted(vals)
                            n = len(v)
                            m = n//2
                            return (v[m] if n%2==1 else (v[m-1]+v[m])//2)
                        def _mad(vals, med):
                            if not vals:
                                return 0
                            dev = sorted([abs(x-med) for x in vals])
                            n = len(dev)
                            m = n//2
                            return (dev[m] if n%2==1 else (dev[m-1]+dev[m])//2) or 1
                        def _fmt_col(e):
                            cdc_raw = str(e.get('colCode') or '')
                            cdc = (cdc_raw.zfill(3) if cdc_raw.isdigit() else cdc_raw)
                            cl = _clean_label((e.get('colLabel') or '').strip())
                            return f"Col {cdc}" + (f" – {cl}" if cl else "") if cdc else (cl or '')
                        def _fmt_row(e):
                            rdc_raw = str(e.get('rowCode') or '')
                            rdc = (rdc_raw.zfill(4) if rdc_raw.isdigit() else rdc_raw)
                            rl = _clean_label((e.get('rowLabel') or '').strip())
                            return f"Row {rdc}" + (f" – {rl}" if rl else "") if rdc else (rl or '')
                        def _ensure_labels(e):
                            try:
                                tbl = e.get('table_id') or ''
                                # Row label fallback via index
                                if (not e.get('rowLabel')) and tbl and e.get('rowCode'):
                                    try:
                                        idx = max(0, int(str(e.get('rowCode'))) // 10 - 1)
                                        rl = (mapping_row_by_index.get(tbl, {}) or {}).get(idx, '')
                                        if rl:
                                            e['rowLabel'] = rl
                                    except Exception:
                                        pass
                                # Col label fallback via display code
                                if (not e.get('colLabel')) and tbl and e.get('colCode'):
                                    cdc = str(e.get('colCode'))
                                    lbl = (mapping_col_label.get(tbl, {}) or {}).get(cdc, '')
                                    if not lbl:
                                        try:
                                            idx = max(0, int(cdc) // 10 - 1)
                                            lbl = (mapping_col_by_index.get(tbl, {}) or {}).get(idx, '')
                                        except Exception:
                                            lbl = ''
                                    if lbl:
                                        e['colLabel'] = lbl
                            except Exception:
                                pass
                        def _strip_tags(html: str) -> str:
                            if not html:
                                return ''
                            txt = _re_c.sub(r"<[^>]+>", " ", html)
                            return _html_c.unescape(' '.join(txt.split()))
                        def _extract_between(text: str, start: str, stops: list) -> str:
                            if not text:
                                return ''
                            s = text.find(start)
                            if s < 0:
                                return ''
                            s += len(start)
                            end_pos = len(text)
                            for st in stops:
                                p = text.find(st, s)
                                if p >= 0:
                                    end_pos = min(end_pos, p)
                            return text[s:end_pos].strip()
                        def _extract_rows(msg: str, limit: int = 3):
                            if not msg:
                                return []
                            items = []
                            for m in _re_c.finditer(r"Row\s+(\d{3,4})\s+\"([^\"]+)\"", msg):
                                items.append((m.group(1), m.group(2)))
                                if len(items) >= limit:
                                    break
                            return items
                        for e in (entries or []):
                            # Build concise message with title, rule, found, causes, top contributors, fix
                            base_html = e.get('message') or ''
                            base_text = _strip_tags(base_html)
                            e['has_fact_values'] = False
                            _ensure_labels(e)
                            r_txt = []
                            # Title
                            try:
                                tbl = e.get('table_id') or ''
                                tlabel = (table_label_map.get(tbl, '') if tbl else '').strip()
                                if tlabel and tlabel.startswith(f"{tbl}:"):
                                    tlabel = tlabel[len(tbl)+1:].strip()
                                if tbl:
                                    title_line = f"<strong>Title</strong> {tbl}"
                                    if tlabel:
                                        title_line += f" — {tlabel}"
                                    r_txt.append(title_line)
                            except Exception:
                                pass
                            # Extract rule text and decide on collapsing long RHS
                            rule_extracted = _extract_between(base_text, "Rule ", ["Found ", "Fix ", "Template ", "Message "])
                            # normalize codes in rule text to include leading zeros
                            try:
                                import re as _re_nc
                                def pad_row(m):
                                    code = m.group(1)
                                    return f"Row {code.zfill(4)}"
                                def pad_col(m):
                                    code = m.group(1)
                                    return f"Column {code.zfill(3)}"
                                rule_extracted = _re_nc.sub(r"Row\s+(\d{1,4})", pad_row, rule_extracted)
                                rule_extracted = _re_nc.sub(r"Column\s+(\d{1,4})", pad_col, rule_extracted)
                            except Exception:
                                pass
                            # Determine operator
                            op = 'satisfies the relation'
                            if '<=' in rule_extracted or '≤' in rule_extracted:
                                op = 'must be ≤'
                            elif '>=' in rule_extracted or '≥' in rule_extracted:
                                op = 'must be ≥'
                            elif ' = ' in rule_extracted or 'must equal' in rule_extracted:
                                op = 'must equal'
                            # Count RHS terms
                            rhs_terms_count = 0
                            try:
                                rows_all = list(_re_c.finditer(r"Row\s+\d{3,4}", rule_extracted))
                                if rows_all:
                                    rhs_terms_count = max(0, len(rows_all) - 1)
                            except Exception:
                                rhs_terms_count = 0
                            # Parse row codes and labels from rule (LHS first)
                            rule_rows = []  # [(code,label)]
                            try:
                                for m in _re_c.finditer(r"Row\s+(\d{3,4})\s+\"([^\"]+)\"", rule_extracted):
                                    rule_rows.append((m.group(1), _clean_label(m.group(2))))
                            except Exception:
                                rule_rows = []
                            # Try to detect operator from extracted rule text to improve fallback clarity
                            op_from_rule = None
                            try:
                                if ('≥' in rule_extracted) or ('>=' in rule_extracted):
                                    op_from_rule = '>='
                                elif ('≤' in rule_extracted) or ('<=' in rule_extracted):
                                    op_from_rule = '<='
                                elif (' must equal' in rule_extracted) or (' = ' in rule_extracted):
                                    op_from_rule = '='
                                elif (' must be >' in rule_extracted) or (' > ' in rule_extracted):
                                    op_from_rule = '>'
                                elif (' must be <' in rule_extracted) or (' < ' in rule_extracted):
                                    op_from_rule = '<'
                            except Exception:
                                op_from_rule = None
                            # Compose rule line in symbolic form with column context
                            try:
                                # Determine operator symbol
                                op_eff = (op or op_from_rule or '').strip()
                                op_sym = '='
                                if op_eff in ('must be ≥', '>=', '≥'):
                                    op_sym = '≤' if False else '≥'
                                elif op_eff in ('must be ≤', '<=', '≤'):
                                    op_sym = '≤'
                                elif op_eff in ('must equal', '='):
                                    op_sym = '='
                                elif op_eff in ('>', 'must be >'):
                                    op_sym = '>'
                                elif op_eff in ('<', 'must be <'):
                                    op_sym = '<'
                                # LHS/RHS rows
                                lhs_code = None
                                lhs_label_sym = ''
                                rhs_codes = []
                                if rule_rows:
                                    lhs_code, lhs_label_sym = rule_rows[0]
                                    rhs_codes = [rc for rc, _lbl in rule_rows[1:]]
                                # Fallback to entry codes if no parse
                                if not lhs_code:
                                    lhs_code = (str(e.get('rowCode') or '')).zfill(4) if str(e.get('rowCode') or '').isdigit() else (e.get('rowCode') or '')
                                    lhs_label_sym = _clean_label(e.get('rowLabel') or '')
                                # Column context
                                col_code_sym = (str(e.get('colCode') or '')).zfill(3) if str(e.get('colCode') or '').isdigit() else (e.get('colCode') or '')
                                col_label_sym = _clean_label(e.get('colLabel') or '')
                                # Build RHS expression
                                if rhs_codes:
                                    if len(rhs_codes) > 5:
                                        rhs_expr = f"sum of {len(rhs_codes)} rows"
                                    else:
                                        rhs_expr = ' + '.join([f"Row {str(rc).zfill(4) if str(rc).isdigit() else rc}" for rc in rhs_codes])
                                else:
                                    rhs_expr = ''
                                lhs_expr = f"Row {str(lhs_code).zfill(4) if str(lhs_code).isdigit() else lhs_code} \"{lhs_label_sym}\"" if lhs_label_sym else f"Row {str(lhs_code).zfill(4) if str(lhs_code).isdigit() else lhs_code}"
                                rule_line = f"<strong>Rule</strong> {lhs_expr} {op_sym} {rhs_expr}".rstrip()
                                if col_code_sym or col_label_sym:
                                    col_txt = f" for column {col_code_sym}" if col_code_sym else " for column"
                                    if col_label_sym:
                                        col_txt += f" \"{col_label_sym}\""
                                    rule_line += col_txt
                                # Rule-specific clarity hooks for large LR rules
                                try:
                                    rid = e.get('rule_id') or ''
                                    clar = {
                                        'v4456_m_0': ' (sum of off‑balance sheet and derivatives adjustments)',
                                        'v4457_m_0': ' (sum of off‑balance sheet and derivatives adjustments)'
                                    }.get(rid, '')
                                    if clar:
                                        rule_line += clar
                                except Exception:
                                    pass
                            except Exception:
                                # Fallback to previously built rule_line if symbolic fails
                                rule_row = _fmt_row(e)
                                rule_col = _fmt_col(e)
                                rule_line = f"<strong>Rule</strong> {rule_row} ({rule_col})"
                            r_txt.append(rule_line)
                            # Found line -> multi-line: Found, Compare, Difference
                            found = _extract_between(base_text, "Found ", ["Fix ", "Rule ", "Template ", "Message "])
                            lhs_val = 0
                            rhs_vals = []
                            if found:
                                try:
                                    r_txt.append(f"<strong>Found</strong> {found}")
                                except Exception:
                                    pass
                            # Likely causes
                            causes = []
                            try:
                                # Pair RHS rows to values when lengths match
                                components = []
                                if rule_rows and len(rule_rows) >= 2 and rhs_vals and (len(rhs_vals) >= (len(rule_rows)-1)):
                                    rhs_rows = rule_rows[1:]
                                    k = min(len(rhs_rows), len(rhs_vals))
                                    components = [
                                        {
                                            'row': rhs_rows[i][0],
                                            'label': rhs_rows[i][1],
                                            'value': abs(int(rhs_vals[i]))
                                        } for i in range(k)
                                    ]
                                # compute stats
                                values = [c['value'] for c in components if c['value'] is not None]
                                s = sum(values)
                                med = _median(values)
                                n = len(values)
                                # Scale hint: only if extreme ratio or dispersion
                                if s and lhs_val:
                                    R = abs(lhs_val)/abs(s)
                                    if R < 1/1000 or R > 1000 or (med and max(values) >= 1000*med):
                                        causes.append(f"Scale mismatch likely (lhs {lhs_val:,} vs sum {s:,}).")
                                # Sign hint for '(–) rows (LHS)
                                try:
                                    lhs_label_clean = rule_rows[0][1] if rule_rows else ''
                                except Exception:
                                    lhs_label_clean = ''
                                if lhs_label_clean.startswith('(–)') and lhs_val > 0:
                                    causes.append(f"'(–) row should be negative but is +{lhs_val:,}.")
                                # Missing/zero
                                if s:
                                    for c in components:
                                        share = c['value']/s if s else 0
                                        if c['value'] == 0 or share < 0.005:
                                            causes.append(f"Missing/zero on Row {c['row']} – {c['label']}.")
                                # Outlier/dominant
                                dominant_msgs = []
                                if n >= 1 and s:
                                    # compute shares
                                    comps_sorted = sorted(components, key=lambda x: x['value'], reverse=True)
                                    top = comps_sorted[0]
                                    share_top = top['value']/s
                                    if n <= 3:
                                        if (n>1 and top['value'] >= 2*(comps_sorted[1]['value'] or 1) and share_top >= 0.80) or (n>1 and top['value'] >= 1.5*(comps_sorted[1]['value'] or 1) and share_top >= 0.90):
                                            dominant_msgs.append(f"Outlier: Row {top['row']} {top['label']} = {top['value']:,} (~{share_top*100:.0f}%).")
                                    elif n <= 7:
                                        if med and top['value'] >= 4*med and share_top >= 0.60:
                                            dominant_msgs.append(f"Outlier: Row {top['row']} {top['label']} = {top['value']:,} (~{share_top*100:.0f}%).")
                                        elif share_top >= 0.50:
                                            dominant_msgs.append(f"Top driver: Row {top['row']} {top['label']} = {top['value']:,} (~{share_top*100:.0f}%).")
                                    else:
                                        mad = _mad(values, med)
                                        if med and mad and top['value'] >= 5*med and (abs(top['value']-med)/mad) >= 3 and share_top >= 0.40:
                                            dominant_msgs.append(f"Outlier: Row {top['row']} {top['label']} = {top['value']:,} (~{share_top*100:.0f}%).")
                                causes.extend(dominant_msgs)
                            except Exception:
                                pass
                            if causes:
                                r_txt.append("<strong>Likely causes</strong> " + " ".join(causes))
                            # Top contributors (first 3 rows)
                            source_for_rows = (rule_extracted or base_text)
                            rows = _extract_rows(source_for_rows, limit=3)
                            # Show top contributors only when many components
                            if rows and rhs_terms_count > 5:
                                # If we have components with values, include value and share
                                comp_map = {}
                                try:
                                    s = sum(rhs_vals) if rhs_vals else 0
                                    if rhs_vals and rule_rows and len(rule_rows) >= 2:
                                        rhs_rows = rule_rows[1:]
                                        for i in range(min(len(rhs_rows), len(rhs_vals))):
                                            comp_map[rhs_rows[i][0]] = (rhs_vals[i], (rhs_vals[i]/s) if s else 0)
                                except Exception:
                                    comp_map = {}
                                items = []
                                for rc, rl in rows:
                                    if rc in comp_map:
                                        val, share = comp_map[rc]
                                        items.append(f"{rc} {rl}: {_fmt_hnum(val)} (~{share*100:.0f}%)")
                                    else:
                                        items.append(f"{rc} {rl}")
                                contribs = "; ".join(items)
                                r_txt.append(f"<strong>Top contributors</strong> {contribs}.")
                            # Groups included (for long RHS on LR table C_47.00)
                            try:
                                if (e.get('table_id') == 'C_47.00') and rhs_terms_count > 5:
                                    r_txt.append("Groups included: SFTs; Derivatives; Off‑balance sheet (CCF); Adjustments.")
                            except Exception:
                                pass
                            # Fix steps
                            fixes = []
                            try:
                                if any('scale mismatch' in c.lower() for c in causes):
                                    fixes.append("Align unit/scale (use the same units in Column).")
                                if any('' in c for c in causes):
                                    fixes.append("Enter '(–) rows as negatives (correct the sign).")
                                miss = [c for c in causes if c.startswith('Missing/zero on Row')]
                                fixes.extend([m.replace('Missing/zero on ', 'Fill ') for m in miss])
                                outs = [c for c in causes if c.startswith('Outlier:') or c.startswith('Top driver:')]
                                if outs:
                                    fixes.append("Review and correct outlier/dominant row values if due to mapping or scale.")
                            except Exception:
                                pass
                            # Only show Fix when we have specific, actionable hints or a precise column/rows instruction
                            if fixes:
                                r_txt.append("<strong>Fix</strong> " + " ".join(fixes) + " Recalculate and resubmit.")
                            e['readable_message'] = "<br>".join(r_txt)
                    except Exception:
                        pass
                build_concise(results.get('errors', []))
                build_concise(results.get('warnings', []))
                # Prefer entries with parsed fact values per rule_id (drop generic duplicates)
                try:
                    drop_nonactionable = True
                    try:
                        drop_nonactionable = bool((msgs_cfg or {}).get('drop_nonactionable', True))
                    except Exception:
                        drop_nonactionable = True
                    if drop_nonactionable:
                        def _prefer_fact_values(entries):
                            by_rid = {}
                            for e in (entries or []):
                                rid = e.get('rule_id') or ''
                                by_rid.setdefault(rid, []).append(e)
                            kept = []
                            dropped = 0
                            dropped_by_rid = {}
                            for rid, group in by_rid.items():
                                if rid and rid.startswith('v'):
                                    with_vals = [g for g in group if g.get('has_fact_values')]
                                    if with_vals:
                                        # deduplicate by final text
                                        seen = set()
                                        for g in with_vals:
                                            key = (g.get('readable_message') or g.get('message') or '').strip()
                                            if key not in seen:
                                                seen.add(key)
                                                kept.append(g)
                                        d = len(group) - len(with_vals)
                                        if d > 0:
                                            dropped += d
                                            dropped_by_rid[rid] = dropped_by_rid.get(rid, 0) + d
                                    else:
                                        # no numeric facts parsed; keep unique entries only
                                        seen = set()
                                        for g in group:
                                            key = (g.get('readable_message') or g.get('message') or '').strip()
                                            if key not in seen:
                                                seen.add(key)
                                                kept.append(g)
                                else:
                                    kept.extend(group)
                            # metrics
                            results.setdefault('metrics', {}).setdefault('nonactionable', {})
                            na = results['metrics']['nonactionable']
                            na['dropped_total'] = na.get('dropped_total', 0) + dropped
                            if dropped_by_rid:
                                br = na.get('dropped_by_rule_id', {})
                                for k, v in dropped_by_rid.items():
                                    br[k] = br.get(k, 0) + v
                                na['dropped_by_rule_id'] = br
                            return kept
                        results['errors'] = _prefer_fact_values(results.get('errors', []))
                        results['warnings'] = _prefer_fact_values(results.get('warnings', []))
                except Exception:
                    pass
                # Prune non-actionable entries that slipped through (generic/empty)
                try:
                    def _keep(e):
                        rm = (e.get('readable_message') or '').lower()
                        if e.get('rule_id'):
                            return True
                        if 'rule' in rm and 'found' in rm and any(ch.isdigit() for ch in rm):
                            return True
                        if 'this cell' in rm:
                            return False
                        return False
                    errs = results.get('errors', []) or []
                    warns = results.get('warnings', []) or []
                    results['errors'] = [e for e in errs if _keep(e)]
                    results['warnings'] = [w for w in warns if _keep(w)]
                except Exception:
                    pass
                # Final sanitizer: ensure no raw message:v#### tokens leak when hide_raw_keys=true
                try:
                    if hide_raw_keys:
                        import re as _re_final
                        pat = _re_final.compile(r"message:[A-Za-z0-9_\-.]+")
                        def clean_entry(e):
                            for k in ("message", "readable_message"):
                                v = e.get(k)
                                if isinstance(v, str) and pat.search(v):
                                    e[k] = pat.sub("", v).strip()
                        for e in (results.get('errors', []) or []):
                            clean_entry(e)
                        for w in (results.get('warnings', []) or []):
                            clean_entry(w)
                except Exception:
                    pass
                # Assign message IDs and persist consolidated messages under tables dir
                def assign_ids(entries):
                    for e in entries:
                        if 'id' not in e:
                            e['id'] = uuid.uuid4().hex
                assign_ids(results.get('errors', []))
                assign_ids(results.get('warnings', []))
                try:
                    messages_path = tables_root / 'messages.json'
                    # Normalize codes in entries (fields) for consistency with padded display
                    def _pad_entry_codes(entry):
                        try:
                            rc = entry.get('rowCode')
                            cc = entry.get('colCode')
                            if isinstance(rc, str) and rc.isdigit():
                                entry['rowCode'] = rc.zfill(4)
                            if isinstance(cc, str) and cc.isdigit():
                                entry['colCode'] = cc.zfill(3)
                        except Exception:
                            pass
                        return entry
                    errs_out = [_pad_entry_codes(dict(e)) for e in (results.get('errors', []) or [])]
                    warns_out = [_pad_entry_codes(dict(w)) for w in (results.get('warnings', []) or [])]
                    payload = {
                        'run_id': run_id,
                        'errors': errs_out,
                        'warnings': warns_out
                    }
                    messages_path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
                except Exception:
                    logger.warning('Failed to write messages.json for run %s', run_id)
            except Exception as _e:
                logger.debug(f"Friendly location enrichment skipped: {_e}")
        except Exception as e:
            logger.warning(f"Tableset render skipped/failed for run {run_id}: {e}")

        # Final defense-in-depth sanitizer before response
        try:
            import re as _re_final2
            pat2 = _re_final2.compile(r"\bmessage:[A-Za-z0-9_\-.]+\b")
            def _clean_entry(e):
                if hide_raw_keys:
                    for k in ("message", "readable_message"):
                        v = e.get(k)
                        if isinstance(v, str) and pat2.search(v):
                            e[k] = pat2.sub("", v).strip()
                return e
            errors_clean = [_clean_entry(dict(e)) for e in (results.get("errors", []) or [])]
            warnings_clean = [_clean_entry(dict(w)) for w in (results.get("warnings", []) or [])]
        except Exception:
            errors_clean = (results.get("errors", []) or [])
            warnings_clean = (results.get("warnings", []) or [])

        # Ensure minimum usability: rule_id and readable_message should never be empty
        def _ensure_minimum(entries):
            for e in entries:
                try:
                    # Try to recover rule_id from raw_message/message
                    if not e.get('rule_id'):
                        raw = e.get('raw_message') or e.get('message') or ''
                        try:
                            import re as _re_min
                            m = _re_min.search(r"message:([A-Za-z0-9_\-.]+)", raw)
                            if m:
                                e['rule_id'] = m.group(1)
                                if e['rule_id'].startswith('v') and not e.get('category'):
                                    e['category'] = 'formulas'
                        except Exception:
                            pass
                    # Build fallback readable message if missing/blank
                    rm = (e.get('readable_message') or '').strip()
                    if not rm:
                        rid = e.get('rule_id') or 'unknown'
                        table_id = e.get('table_id') or ''
                        row = e.get('rowLabel') or e.get('rowCode') or ''
                        col = e.get('colLabel') or e.get('colCode') or ''
                        name = f"Template {table_id}" if table_id else "This template"
                        loc = ''
                        if row or col:
                            if row and col:
                                loc = f" at row {row}, column {col}"
                            elif row:
                                loc = f" at row {row}"
                            else:
                                loc = f" at column {col}"
                        fallback = (
                            f"{name}: validation rule {rid} failed{loc}. "
                            f"Fix: open the LR template and correct the values so the rule is satisfied. "
                            f"Refer to EBA COREP LR validation rules for details (rule {rid})."
                        )
                        e['readable_message'] = fallback
                        # If base message is blank, provide the same fallback
                        if not (e.get('message') or '').strip():
                            e['message'] = fallback
                except Exception:
                    continue
        _ensure_minimum(errors_clean)
        _ensure_minimum(warnings_clean)

        # Optional: baseline cross-check (approx) using seen rule_ids vs baseline present count
        try:
            baseline_path = Path(__file__).resolve().parents[2] / "config" / "assertion_baseline.json"
            if baseline_path.exists():
                try:
                    baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
                except Exception:
                    baseline = {}
                # entrypoint id provided by client form
                ep_id = entrypoint or ''
                # derive taxonomy/version key
                tax_ver = str(dpm_version or 'unknown')
                base_key = f"{tax_ver}:{ep_id}" if ep_id else None
                # seen unique rule_ids (approx; failures only)
                seen = set()
                for _e in (results.get("errors", []) or []):
                    rid = _e.get('rule_id')
                    if rid:
                        seen.add(rid)
                for _w in (results.get("warnings", []) or []):
                    rid = _w.get('rule_id')
                    if rid:
                        seen.add(rid)
                results.setdefault('metrics', {}).setdefault('coverage', {})
                cov = results['metrics']['coverage']
                cov['rule_ids_seen_approx'] = len(seen)
                if base_key and isinstance(baseline, dict) and base_key in baseline:
                    try:
                        cov['baseline_present_count'] = int((baseline[base_key] or {}).get('present_count', 0))
                        cov['baseline_hash'] = (baseline[base_key] or {}).get('hash', '')
                        cov['entrypoint_id'] = ep_id
                        cov['taxonomy_version'] = tax_ver
                        cov['approx_complete'] = (cov.get('rule_ids_seen_approx', 0) <= cov.get('baseline_present_count', 0))
                    except Exception:
                        pass
                else:
                    # Fire-and-forget baseline generation in background if missing
                    try:
                        import subprocess as _sp
                        script_path = Path(__file__).resolve().parents[2] / 'scripts' / 'enumerate_assertions.py'
                        args = ['python3', str(script_path), '--entrypoint-id', ep_id or '', '--taxonomy-version', tax_ver]
                        _sp.Popen(args, stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
                    except Exception:
                        pass
        except Exception:
            pass

        resp = ValidationResponse(
            status=results.get("status", "error"),
            trace_id=trace_id,
            run_id=run_id,
            duration_ms=results.get("duration_ms", 0),
            facts_count=int(results.get("facts_count", 0)),
            dpm_version=dpm_version,
            is_csv=False,
            errors=[{**e, "severity": e.get("severity","error")} for e in errors_clean],
            warnings=[{**w, "severity": w.get("severity","warning")} for w in warnings_clean],
            metadata={"profile": profile},
            dts_evidence=results.get("dts_evidence", {}),
            metrics=results.get("metrics", {}),
            tables_index_url=tables_index_url
        )
        if prog:
            prog.finish(job_id, success=True, message="Validation complete")
        return resp
        
    except HTTPException:
        # Re-raise HTTPExceptions (like preflight failures) as-is
        try:
            prog = getattr(request.app.state, 'progress_store', None)
            if prog and client_run_id:
                prog.error(client_run_id, message="Validation error")
        except Exception:
            pass
        raise
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        try:
            prog = getattr(request.app.state, 'progress_store', None)
            if prog and client_run_id:
                prog.error(client_run_id, message=f"Validation error: {e}")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

@router.post("/render/tableset")
async def render_tableset(
    request: Request,
    file: UploadFile = File(...),
    lang: Optional[str] = Form("en"),
    errorText: Optional[str] = Form(None),
    client_run_id: Optional[str] = Form(None)
):
    """
    Upload an instance, load it offline, and render the EBA tableset to HTML.
    Returns the URL to the generated index.html under the static mount.
    """
    try:
        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Save upload under backend/uploads
        base_dir = Path(__file__).resolve().parents[2]  # backend/
        upload_dir = base_dir / "uploads"
        upload_dir.mkdir(exist_ok=True)
        unique_name = f"{Path(file.filename).stem}_{uuid.uuid4().hex[:8]}{Path(file.filename).suffix}"
        upload_path = upload_dir / unique_name
        # Stream upload to disk with limits
        max_bytes = _max_upload_bytes()
        _early_reject_on_content_length(request, max_bytes)
        await _save_upload_streaming(file, upload_path, max_bytes)

        # Progress
        prog = getattr(request.app.state, 'progress_store', None)
        job_id = client_run_id or uuid.uuid4().hex
        if prog:
            prog.start(job_id, task="render", message="Loading instance")

        # Load instance (reuse existing loader; DTS-first optionality left to defaults)
        model_xbrl, _facts = arelle_service.load_instance(str(upload_path))

        # Prepare output directory under backend/temp/tables/<run-id>
        run_id = uuid.uuid4().hex[:8]
        tables_root = base_dir / "temp" / "tables" / run_id
        tables_root.mkdir(parents=True, exist_ok=True)

        # Render tableset
        if prog:
            prog.update(job_id, 40, "Rendering templates")
        index_path = render_eba_tableset(model_xbrl, tables_root, lang=lang or "en")
        # Opportunistic GC pass after successful render
        try:
            gc_tables_dir(base_dir / "temp" / "tables", ttl_days=3, max_bytes=5 * 1024 * 1024 * 1024)
        except Exception:
            logger.warning("Post-render GC pass failed")

        # Form public URL via static mount
        index_url = f"/static/tables/{run_id}/index.html"
        resp = {
            "status": "success",
            "run_id": run_id,
            "index_url": index_url,
            "path": str(index_path)
        }
        # If errorText was provided, add a convenience link with error text included for tooltips
        if errorText:
            try:
                from urllib.parse import urlencode
                q = urlencode({"errorText": errorText})
                resp["index_url_with_error"] = f"{index_url}?{q}"
            except Exception:
                pass
        if prog:
            prog.finish(job_id, success=True, message="Render complete")
        return JSONResponse(content=resp)
    except HTTPException:
        try:
            prog = getattr(request.app.state, 'progress_store', None)
            if prog and client_run_id:
                prog.error(client_run_id, message="Render error")
        except Exception:
            pass
        raise
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error("Render tableset failed: %s", tb)
        try:
            prog = getattr(request.app.state, 'progress_store', None)
            if prog and client_run_id:
                prog.error(client_run_id, message=f"Render error: {e}")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Render tableset failed: {repr(e)}\n{tb}")

@router.post("/render/table")
async def render_table(
    request: Request,
    file: UploadFile = File(...),
    table_id: str = Form(...),
    lang: Optional[str] = Form("en")
):
    """
    Upload an instance and render a single table into the static tables directory.
    Returns the per-table URL and path.
    """
    try:
        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Save upload
        base_dir = Path(__file__).resolve().parents[2]
        upload_dir = base_dir / "uploads"
        upload_dir.mkdir(exist_ok=True)
        unique_name = f"{Path(file.filename).stem}_{uuid.uuid4().hex[:8]}{Path(file.filename).suffix}"
        upload_path = upload_dir / unique_name
        # Stream upload to disk with limits
        max_bytes = _max_upload_bytes()
        _early_reject_on_content_length(request, max_bytes)
        await _save_upload_streaming(file, upload_path, max_bytes)

        # Load instance
        model_xbrl, _facts = arelle_service.load_instance(str(upload_path))

        # Allocate run dir for single-table rendering
        run_id = uuid.uuid4().hex[:8]
        tables_root = base_dir / "temp" / "tables" / run_id
        tables_root.mkdir(parents=True, exist_ok=True)

        out_file = tables_root / f"{table_id}.html"
        html_path = render_single_table(model_xbrl, out_file, table_id_or_name=table_id, lang=lang or "en")

        index_url = f"/static/tables/{run_id}/{table_id}.html"
        return JSONResponse(content={
            "status": "success",
            "run_id": run_id,
            "table_id": table_id,
            "url": index_url,
            "path": html_path
        })
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error("Render table failed: %s", tb)
        raise HTTPException(status_code=500, detail=f"Render table failed: {repr(e)}\n{tb}")

@router.get("/render/for-error")
async def render_for_error(
    run_id: str,
    conceptNs: Optional[str] = None,
    conceptLn: Optional[str] = None,
    contextRef: Optional[str] = None,
    messageId: Optional[str] = None,
):
    """
    Given a run_id and optional concept/context, return candidate tables
    and deep-link URLs for highlighting.
    """
    try:
        base_dir = Path(__file__).resolve().parents[2]  # backend/
        tables_root = base_dir / "temp" / "tables" / run_id
        if not tables_root.exists():
            raise HTTPException(status_code=404, detail="run_id not found")

        results = []
        for mapping_path in tables_root.glob("*.mapping.json"):
            try:
                data = json.loads(mapping_path.read_text(encoding="utf-8"))
                table_id = data.get("tableId") or mapping_path.stem.replace(".mapping", "")
                cells = data.get("cells", [])
                matched = []
                for idx, cell in enumerate(cells):
                    for f in cell.get("facts", []) or []:
                        if conceptNs and f.get("conceptNamespace") != conceptNs:
                            continue
                        if conceptLn and f.get("conceptLocalName") != conceptLn:
                            continue
                        if contextRef and f.get("contextRef") != contextRef:
                            continue
                        matched.append({
                            "rowIndex": cell.get("rowIndex"),
                            "colIndex": cell.get("colIndex"),
                            "rowLabel": cell.get("rowLabel"),
                            "colLabel": cell.get("colLabel"),
                            "fact": f
                        })
                        break
                if matched:
                    params = []
                    if conceptNs: params.append(("conceptNs", conceptNs))
                    if conceptLn: params.append(("conceptLn", conceptLn))
                    if contextRef: params.append(("contextRef", contextRef))
                    # Attach error text if available via messages.json and messageId
                    if messageId:
                        try:
                            msg_data = json.loads((tables_root / 'messages.json').read_text(encoding='utf-8'))
                            all_msgs = (msg_data.get('errors', []) or []) + (msg_data.get('warnings', []) or [])
                            for m in all_msgs:
                                if m.get('id') == messageId and m.get('message'):
                                    params.append(("errorText", m.get('message')))
                                    break
                        except Exception:
                            pass
                    qp = "&".join([f"{k}={v}" for k,v in params])
                    url = f"/static/tables/{run_id}/{table_id}.html" + (f"?{qp}" if qp else "")
                    results.append({
                        "table_id": table_id,
                        "url": url,
                        "matches": matched
                    })
            except Exception:
                continue
        return JSONResponse(content={"run_id": run_id, "candidates": results})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"for-error failed: {e}")
        raise HTTPException(status_code=500, detail=f"for-error failed: {str(e)}")

@router.get("/taxonomies", response_model=List[TaxonomyInfo])
async def list_taxonomies():
    """List configured taxonomies and entry points."""
    try:
        # Preferred: derive frameworks and entrypoints dynamically from Full Taxonomy offline root
        import yaml, os
        project_root = Path(__file__).resolve().parents[2].parents[0]
        app_cfg_path = Path(__file__).resolve().parents[2] / "config" / "app.yaml"
        app_cfg = yaml.safe_load(app_cfg_path.read_text(encoding="utf-8")) or {}
        offline_roots = (app_cfg.get("offline_roots") or [])
        full_local_root = None
        full_url_prefix = None
        for m in offline_roots:
            up = (m.get("url_prefix") or "")
            lp = (m.get("local_root") or "")
            if up.startswith("http://www.eba.europa.eu/eu/fr/xbrl/") and "Full Taxonomy" in lp:
                full_url_prefix = up.rstrip("/") + "/"
                full_local_root = (project_root / lp).resolve()
                break
        results: List[TaxonomyInfo] = []
        if full_local_root and full_local_root.exists():
            fws_root = (full_local_root / "crr" / "fws")
            if fws_root.exists():
                # Enumerate framework directories under fws
                for fw_dir in sorted([p for p in fws_root.iterdir() if p.is_dir()]):
                    framework = fw_dir.name
                    vdir = fw_dir / "4.0"
                    mod_dir = vdir / "mod"
                    entrypoints: List[EntrypointInfo] = []
                    if mod_dir.exists():
                        for xsd_path in sorted(mod_dir.glob("*.xsd")):
                            # Build HTTP URL from local path by replacing local_root prefix
                            try:
                                rel = xsd_path.resolve().relative_to(full_local_root)
                                http_xsd = (full_url_prefix + str(rel).replace(os.sep, "/"))
                            except Exception:
                                http_xsd = ""
                            ep_id = xsd_path.stem
                            ep_label = ep_id.replace("_", " ").upper()
                            entrypoints.append(EntrypointInfo(id=ep_id, label=ep_label, xsd=http_xsd))
                    # Only include frameworks with entrypoints discovered
                    if entrypoints:
                        results.append(TaxonomyInfo(
                            id=framework,
                            label=f"EBA {framework.upper()}",
                            version="4.0.0.0",
                            entrypoints=entrypoints
                        ))
        # Also scan configured package directories (Reporting Frameworks) for entrypoints
        try:
            cfg_path_yaml = Path(__file__).resolve().parents[2] / "config" / "eba_taxonomies.yaml"
            data_yaml = yaml.safe_load(cfg_path_yaml.read_text(encoding="utf-8")) or {}
        except Exception:
            data_yaml = {}
        package_dirs: List[Path] = []
        try:
            for top_key, top_val in (data_yaml.items() if isinstance(data_yaml, dict) else []):
                if not isinstance(top_val, dict):
                    continue
                for tax_id, tax_cfg in top_val.items():
                    if not isinstance(tax_cfg, dict):
                        continue
                    for p in (tax_cfg.get("packages") or []):
                        p_abs = (Path(__file__).resolve().parents[3] / p).resolve()
                        if p_abs.exists() and p_abs.is_dir():
                            package_dirs.append(p_abs)
        except Exception:
            package_dirs = []

        def _append_framework(framework_id: str, entrypoints_found: List[EntrypointInfo]):
            if not entrypoints_found:
                return
            # Update existing or append new
            for t in results:
                if t.id == framework_id:
                    # merge unique by id
                    existing_ids = {e.id for e in t.entrypoints}
                    for e in entrypoints_found:
                        if e.id not in existing_ids:
                            t.entrypoints.append(e)
                    return
            results.append(TaxonomyInfo(
                id=framework_id,
                label=f"EBA {framework_id.upper()}",
                version="4.0.0.0",
                entrypoints=entrypoints_found
            ))

        if package_dirs:
            for root_dir in package_dirs:
                # typical path: <pkg>/www.eba.europa.eu/eu/fr/xbrl/crr/fws/*/4.0/mod/*.xsd
                candidates = [
                    root_dir / "www.eba.europa.eu" / "eu" / "fr" / "xbrl" / "crr" / "fws",
                    root_dir / "www.eurofiling.info" / "eu" / "fr" / "xbrl" / "crr" / "fws",
                ]
                for fws_root in candidates:
                    if not fws_root.exists():
                        continue
                    for fw_dir in sorted([p for p in fws_root.iterdir() if p.is_dir()]):
                        vdir = fw_dir / "4.0" / "mod"
                        if not vdir.exists():
                            continue
                        eps_col: List[EntrypointInfo] = []
                        for xsd_path in sorted(vdir.glob("*.xsd")):
                            try:
                                rel = xsd_path.resolve().relative_to(root_dir)
                                # Construct best-effort HTTP URL using the path under package
                                http_xsd = "http://" + str(rel).replace(os.sep, "/")
                            except Exception:
                                http_xsd = ""
                            ep_id = xsd_path.stem
                            ep_label = ep_id.replace("_", " ").upper()
                            eps_col.append(EntrypointInfo(id=ep_id, label=ep_label, xsd=http_xsd))
                        _append_framework(fw_dir.name, eps_col)

        # Merge YAML-declared entrypoints into framework groups (even if files are not mirrored locally)
        try:
            for top_key, top_val in (data_yaml.items() if isinstance(data_yaml, dict) else []):
                if not isinstance(top_val, dict):
                    continue
                for tax_id, tax_cfg in top_val.items():
                    if not isinstance(tax_cfg, dict):
                        continue
                    eps_cfg = (tax_cfg.get("entrypoints") or [])
                    framework_to_eps: Dict[str, List[EntrypointInfo]] = {}
                    for ep in eps_cfg:
                        xsd = str((ep or {}).get("xsd") or "")
                        ep_id = str(ep.get("id") or "")
                        ep_label = str(ep.get("label") or ep_id).strip() or ep_id
                        # derive framework from xsd path if possible: .../fws/<framework>/4.0/mod/...
                        fw = None
                        m = re.search(r"/fws/([^/]+)/4\\.0/", xsd)
                        if m:
                            fw = m.group(1)
                        if not fw:
                            # assign to taxonomy id group if framework not derivable
                            fw = str(tax_id)
                        framework_to_eps.setdefault(fw, []).append(EntrypointInfo(id=ep_id, label=ep_label, xsd=xsd))
                    for fw, eps_list in framework_to_eps.items():
                        _append_framework(fw, eps_list)
        except Exception:
            pass

        # Fallback to YAML declared entrypoints if still empty
        if not results:
            cfg_path = Path(__file__).resolve().parents[2] / "config" / "eba_taxonomies.yaml"
            data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
            for top_key, top_val in (data.items() if isinstance(data, dict) else []):
                if not isinstance(top_val, dict):
                    continue
                for tax_id, tax_cfg in top_val.items():
                    if not isinstance(tax_cfg, dict):
                        continue
                    label = f"{top_key.upper()} {tax_id.upper()}"
                    version = "unknown"
                    try:
                        eps = (tax_cfg.get("entrypoints") or [])
                        for ep in eps:
                            xsd = (ep or {}).get("xsd") or ""
                            m = re.search(r"/(\d+\.\d+)/", xsd)
                            if m:
                                version = m.group(1) + ".0.0"
                                break
                    except Exception:
                        pass
                    entrypoints: List[EntrypointInfo] = []
                    for ep in (tax_cfg.get("entrypoints") or []):
                        try:
                            entrypoints.append(EntrypointInfo(
                                id=str(ep.get("id") or ""),
                                label=str(ep.get("label") or ""),
                                xsd=str(ep.get("xsd") or "")
                            ))
                        except Exception:
                            continue
                    results.append(TaxonomyInfo(
                        id=str(tax_id),
                        label=label,
                        version=version,
                        entrypoints=entrypoints
                    ))
        return results
    except Exception as e:
        logger.error(f"Failed to list taxonomies: {e}")
        raise HTTPException(status_code=500, detail="Failed to list taxonomies")

@router.get("/profiles")
async def list_profiles():
    """List available validation profiles and flags."""
    try:
        # TODO: Load from config/app.yaml
        return {
            "profiles": {
                "fast": {
                    "formulas": False,
                    "csv_constraints": False,
                    "trace": False
                },
                "full": {
                    "formulas": True,
                    "csv_constraints": True,
                    "trace": False
                },
                "debug": {
                    "formulas": True,
                    "csv_constraints": True,
                    "trace": True
                }
            }
        }
    except Exception as e:
        logger.error(f"Failed to list profiles: {e}")
        raise HTTPException(status_code=500, detail="Failed to list profiles")

@router.get("/debug/catalog")
async def catalog_introspection(request: Request):
    """Return catalog mapping introspection (Phase 2A)."""
    try:
        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Gate debug endpoints via config flag
        try:
            debug_enabled = bool(((arelle_service._config or {}).get('features', {}) or {}).get('debug_endpoints', False))
        except Exception:
            debug_enabled = False
        if not debug_enabled:
            raise HTTPException(status_code=404, detail="Not found")
        data = arelle_service.get_catalog_introspection()
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Catalog introspection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Catalog introspection failed: {str(e)}")

@router.get("/debug/message")
async def debug_lookup_message(request: Request, id: str):
    """Lookup a catalog message id (e.g., v4460_m_0) and return resolved text.

    Accepts both raw id and keys like message:v4460_m_0.
    """
    try:
        # Gate via config flag
        try:
            arelle_service = getattr(request.app.state, 'arelle_service', None)
            debug_enabled = bool(((arelle_service._config or {}).get('features', {}) or {}).get('debug_endpoints', False))
        except Exception:
            debug_enabled = False
        if not debug_enabled:
            raise HTTPException(status_code=404, detail="Not found")
        mc = getattr(request.app.state, 'message_catalog', None)
        if not mc:
            raise HTTPException(status_code=503, detail="Message catalog not available")
        key = id if id.startswith('message:') else f"message:{id}"
        resolved = mc.resolve(key) or ''
        return JSONResponse(content={
            "id": id,
            "key": key,
            "resolved": resolved,
            "lang": getattr(mc, 'lang', 'en')
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Debug message lookup failed: {e}")
        raise HTTPException(status_code=500, detail=f"Debug message lookup failed: {str(e)}")

@router.post("/debug/probe")
async def probe_url_resolution(request: Request, url: str = Form(...)):
    """Probe URL resolution for debugging and visibility."""
    try:
        arelle_service = getattr(request.app.state, 'arelle_service', None)
        if not arelle_service:
            raise HTTPException(status_code=503, detail="Arelle service not available")

        # Gate via config flag
        try:
            debug_enabled = bool(((arelle_service._config or {}).get('features', {}) or {}).get('debug_endpoints', False))
        except Exception:
            debug_enabled = False
        if not debug_enabled:
            raise HTTPException(status_code=404, detail="Not found")
        probe_results = arelle_service.probe_url_resolution(url)
        return JSONResponse(content=probe_results)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"URL resolution probe failed: {e}")
        raise HTTPException(status_code=500, detail=f"URL resolution probe failed: {str(e)}")

@router.post("/validate/byEntrypoint")
async def validate_by_entrypoint(
    entrypoint_id: str = Form(...),
    profile: Optional[str] = Form("fast")
):
    """
    Sanity validation by entry point (no user data).
    
    Validates that the specified entrypoint can be loaded and 
    basic taxonomy validation passes.
    """
    try:
        logger.info(f"Validating entrypoint: {entrypoint_id} with profile: {profile}")
        
        # TODO: Implement entrypoint validation
        return {
            "status": "success",
            "entrypoint_id": entrypoint_id,
            "profile": profile,
            "message": "Entrypoint validation not yet implemented"
        }
        
    except Exception as e:
        logger.error(f"Entrypoint validation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Entrypoint validation failed: {str(e)}")
