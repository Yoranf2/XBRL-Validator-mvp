"""
EBA XBRL Validator Backend - FastAPI Application

Main application entry point for the EBA XBRL validation service.
Supports RF 4.0 baseline with strict offline operation.
"""

import sys
import yaml
from pathlib import Path

# Define project paths
BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # project root

# Wire Arelle imports early - add third_party/arelle to Python path
ARELLE_PATH = PROJECT_ROOT / "third_party" / "arelle"
if ARELLE_PATH.exists():
    sys.path.insert(0, str(ARELLE_PATH))
else:
    # Will be logged properly after logging is set up
    pass

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn
import logging

from app.api.routes_validation import router as validation_router
from app.utils.config_loader import load_config
from app.utils.logging import setup_logging
from app.utils.retention import gc_tables_dir
from app.utils.progress import ProgressStore
from app.services.message_catalog import MessageCatalog
from app.utils.metrics import Metrics

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await startup_event()
    try:
        yield
    finally:
        # Shutdown
        await shutdown_event()


# Create FastAPI application with lifespan
app = FastAPI(
    title="EBA XBRL Validator",
    description="Backend service for validating EBA filings with strict offline operation",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Global early 413 guard based on Content-Length header
class MaxBodyByHeaderMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, max_bytes: int):
        super().__init__(app)
        self.max_bytes = max_bytes
    async def dispatch(self, request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl:
            try:
                if int(cl) > self.max_bytes:
                    return JSONResponse(
                        status_code=413,
                        content={"detail": f"Upload too large (>{self.max_bytes // (1024*1024)} MB)"}
                    )
            except ValueError:
                pass
        return await call_next(request)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(validation_router, prefix="/api/v1")

# Mount static files for tables viewer
TABLES_DIR = BASE_DIR / "temp" / "tables"
TABLES_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static/tables", StaticFiles(directory=str(TABLES_DIR), html=True), name="tables-static")

# Mount simple UI assets
UI_DIR = BASE_DIR / "ui"
if UI_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(UI_DIR), html=True), name="ui-static")

# Configure global Content-Length guard using app.yaml limits.max_upload_mb
try:
    cfg = load_config(PROJECT_ROOT / "backend" / "config" / "app.yaml")
    max_mb = int(((cfg.get("limits", {}) or {}).get("max_upload_mb", 150)))
except Exception:
    max_mb = 150
app.add_middleware(MaxBodyByHeaderMiddleware, max_bytes=max_mb * 1024 * 1024)

@app.get("/health")
async def health_check():
    """Health check endpoint with service and Arelle build info."""
    try:
        # Get Arelle version information
        arelle_version = "unknown"
        arelle_available = False
        
        try:
            import arelle.Version
            arelle_version = getattr(arelle.Version, 'version', 'unknown')
            arelle_available = True
        except ImportError as e:
            logger.warning(f"Arelle not available: {e}")
            arelle_version = f"import_error: {e}"
        
        return {
            "status": "healthy",
            "service": "eba-xbrl-validator",
            "version": "0.1.0",
            "arelle_version": arelle_version,
            "arelle_available": arelle_available,
            "arelle_path": str(ARELLE_PATH),
            "offline_mode": True
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

async def startup_event():
    """Initialize application on startup."""
    logger.info("Starting EBA XBRL Validator Backend")
    
    # Log Arelle path status
    if ARELLE_PATH.exists():
        logger.info(f"Arelle path configured: {ARELLE_PATH}")
    else:
        logger.warning(f"Arelle path not found: {ARELLE_PATH}")
    
    # Ensure required directories exist (project-rooted)
    required_dirs = [
        BASE_DIR / "uploads",
        BASE_DIR / "temp", 
        BASE_DIR / "cache",
        BASE_DIR / "logs"
    ]
    
    for dir_path in required_dirs:
        dir_path.mkdir(exist_ok=True)
        logger.info(f"Ensured directory exists: {dir_path}")
    # Run a quick GC pass for tables output (TTL + cap)
    try:
        tables_dir = BASE_DIR / "temp" / "tables"
        gc_tables_dir(tables_dir, ttl_days=3, max_bytes=5 * 1024 * 1024 * 1024)
        logger.info("Tables directory GC pass completed")
    except Exception as e:
        logger.warning(f"Tables directory GC pass failed: {e}")
    
    # Initialize Arelle service
    try:
        from app.services.arelle_service import ArelleService
        
        # Create global Arelle service instance with proper cache directory
        cache_dir = BASE_DIR / "cache"
        app.state.arelle_service = ArelleService(cache_dir=cache_dir)
        
        # Load configuration from app.yaml
        try:
            cfg_path = PROJECT_ROOT / "backend" / "config" / "app.yaml"
            with open(cfg_path, "r") as f:
                app_config = yaml.safe_load(f) or {}
            
            # Extract flags for Arelle service
            config = {
                "offline": app_config.get("flags", {}).get("offline", True),
                "use_packages": app_config.get("flags", {}).get("use_packages", True),
                "allow_catalogs": app_config.get("flags", {}).get("allow_catalogs", True),
                "allow_instance_rewrite": app_config.get("flags", {}).get("allow_instance_rewrite", False),
                # Pass through offline roots and feature flags
                "offline_roots": app_config.get("offline_roots", []) or [],
                "features": app_config.get("features", {}) or {}
            }
            logger.info(f"Loaded configuration: {config}")
        except Exception as e:
            logger.warning(f"Failed to load app.yaml config, using defaults: {e}")
            config = {
                "offline": True,
                "use_packages": True,
                "allow_catalogs": True,
                "allow_instance_rewrite": False
            }
        
        app.state.arelle_service.initialize(config)
        logger.info("Arelle service initialized successfully")
        
        # Initialize progress store
        app.state.progress_store = ProgressStore()
        logger.info("Progress store initialized")

        # Load RF 4.0 taxonomy packages from config
        try:
            cfg_path = PROJECT_ROOT / "backend" / "config" / "eba_taxonomies.yaml"
            with open(cfg_path, "r") as f:
                tax_cfg = yaml.safe_load(f) or {}
            pkgs = (tax_cfg.get("eba", {}) or {}).get("rf40", {}).get("packages", []) or []
            abs_pkgs = [str((PROJECT_ROOT / p).resolve()) for p in pkgs]
            app.state.arelle_service.load_taxonomy_packages(abs_pkgs)
            logger.info(f"Loaded taxonomy packages: {abs_pkgs}")
        except Exception as e:
            logger.error(f"Failed loading taxonomy packages: {e}")
        # Initialize message catalog via configured source
        try:
            msgs_cfg = (config.get('features', {}) or {}).get('messages', {}) or {}
            src = (msgs_cfg.get('source') or 'auto').lower()
            lang = (msgs_cfg.get('lang') or 'en').lower()
            zip_globs = msgs_cfg.get('zip_globs') or []
            unpacked_roots = msgs_cfg.get('unpacked_roots') or []
            cache_cfg = msgs_cfg.get('cache') or {}
            cache_enabled = bool(cache_cfg.get('enabled', False))
            cache_path = cache_cfg.get('path')

            mc = MessageCatalog(lang=lang)

            # Decide source
            use_unpacked = False
            if src == 'unpacked':
                use_unpacked = True
            elif src == 'zip':
                use_unpacked = False
            else:  # auto
                try:
                    use_unpacked = any((Path(PROJECT_ROOT / p).exists() for p in unpacked_roots))
                except Exception:
                    use_unpacked = False

            loaded = 0
            if use_unpacked and unpacked_roots:
                roots_abs = [str((PROJECT_ROOT / p).resolve()) for p in unpacked_roots]
                loaded = mc.load_from_unpacked_roots(roots_abs)
                logger.info("Message catalog loaded from unpacked roots: %s (ids=%d, lang=%s)", roots_abs, mc.ids_loaded(), lang)
            elif zip_globs:
                # Resolve globs relative to project root
                patterns = [str(PROJECT_ROOT / g) for g in zip_globs]
                loaded = mc.bulk_load_from_zip_globs(patterns)
                logger.info("Message catalog loaded from zips: %s (ids=%d, lang=%s)", patterns, mc.ids_loaded(), lang)
            if mc.ids_loaded() == 0:
                # Fallback to previously loaded taxonomy package candidates if present
                try:
                    def _is_msg_pkg(path: str) -> bool:
                        up = str(path).upper()
                        return ('SEVERITY' in up) or ('REPORTING_FRAMEWORKS' in up) or ('REPORTING' in up)
                    msg_candidates = [p for p in abs_pkgs if _is_msg_pkg(p)]
                    for z in msg_candidates:
                        mc.load_from_severity_zip(z)
                    logger.info("Message catalog loaded from taxonomy packages: %s (ids=%d, lang=%s)", msg_candidates, mc.ids_loaded(), lang)
                except Exception:
                    pass

            # Optional: write JSON cache in dev
            try:
                if cache_enabled and cache_path and mc.ids_loaded() > 0:
                    out_path = (PROJECT_ROOT / cache_path)
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    import json
                    with open(out_path, 'w', encoding='utf-8') as fh:
                        json.dump({"lang": lang, "count": mc.ids_loaded()}, fh)
                    logger.info("Wrote message catalog cache metadata to %s", out_path)
            except Exception:
                logger.debug("Writing messages cache failed", exc_info=True)

            app.state.message_catalog = mc
            # Initialize metrics and set catalog gauge
            prom_cfg = (app_config.get('observability', {}) or {}).get('prometheus', {}) or {}
            prom_enabled = bool(prom_cfg.get('enabled', False))
            prom_ns = prom_cfg.get('namespace') or 'xbrl_validator'
            prom_path = prom_cfg.get('path') or '/metrics'
            app.state.metrics = Metrics(enabled=prom_enabled, namespace=prom_ns)
            if getattr(app.state, 'metrics', None):
                try:
                    app.state.metrics.set_catalog_ids_loaded(mc.ids_loaded())
                    app.state.metrics.mount_endpoint(
                        app,
                        path=prom_path,
                        require_secret=bool(prom_cfg.get('require_secret', False)),
                        secret_header=prom_cfg.get('secret_header') or 'X-Prom-Secret',
                        secret_value=prom_cfg.get('secret_value') or ''
                    )
                except Exception:
                    logger.debug("Metrics setup failed", exc_info=True)
        except Exception as e:
            logger.warning(f"Message catalog initialization failed: {e}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Arelle service: {e}")
        # Continue startup - service will report unhealthy but won't crash

async def shutdown_event():
    """Cleanup on application shutdown."""
    logger.info("Shutting down EBA XBRL Validator Backend")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
