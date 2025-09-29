"""
Arelle Service

Core service for XBRL validation using Arelle library.
Handles offline operation with package-first approach.
"""

import logging
import threading
import sys
import re
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import json
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urlunparse
from datetime import datetime
import uuid

# Get project root for dictionary schema paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # backend/app/services -> project root

logger = logging.getLogger(__name__)

class ArelleService:
    """
    Service for XBRL validation using Arelle.
    
    Implements offline-first validation with taxonomy packages
    and strict HTTP blocking for security.
    """
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize Arelle service with offline configuration."""
        self.cntlr = None
        self.model_manager = None
        self.cache_dir = cache_dir or Path(__file__).resolve().parents[2] / "cache"
        self.packages_loaded = False
        self._loaded_package_paths = []  # Store package paths for catalog resolver
        self._catalog_map = {}  # Cache for normalized URL prefix -> local path prefix
        self._catalog_entries = {
            "rewriteURI": [],       # List of {uriStartString, rewritePrefix, resolvedPrefix, catalog}
            "rewriteSystem": []     # List of {systemIdStartString, rewritePrefix, resolvedPrefix, catalog}
        }
        self._offline_mode = True  # Track offline mode for defensive checks
        self._http_fetch_attempts = []  # Track any HTTP fetch attempts for debugging
        self._config = {}  # Store configuration for feature flags
        self._registered_catalogs = []  # Store available catalog paths
        self._catalog_validation_results = {}  # Store catalog validation results
        self._catalog_verification_results = []  # Store catalog resolution test results
        # Concurrency guard: Arelle is not thread-safe across loads/validations
        self._lock = threading.RLock()
        
        logger.info("Initializing ArelleService")
        
    def initialize(self, config: Dict[str, Any]):
        """
        Initialize Arelle Controller and ModelManager with offline settings.
        
        Args:
            config: Configuration dictionary with offline and package settings
        """
        # Store config for feature flags
        self._config = config or {}
        # Flatten flags for backward compatibility and easier access
        try:
            flags = (self._config or {}).get("flags", {}) or {}
            for k, v in flags.items():
                # mirror flags onto top-level for legacy accesses
                self._config.setdefault(k, v)
        except Exception:
            pass
        try:
            # Ensure vendored Arelle is used (prefer submodule over any site-packages install)
            try:
                arelle_path = (PROJECT_ROOT / "third_party" / "arelle").resolve()
                if str(arelle_path) not in sys.path:
                    sys.path.insert(0, str(arelle_path))
                logger.info(f"Using vendored Arelle at: {arelle_path}")
            except Exception as e:
                logger.warning(f"Could not configure vendored Arelle path: {e}")
            # Import Arelle components
            from arelle import Cntlr
            from arelle.ModelFormulaObject import FormulaOptions
            
            logger.info("Initializing Arelle Controller with offline configuration")
            
            # Initialize Arelle Controller with in-memory buffer logging to capture Arelle messages
            self.cntlr = Cntlr.Cntlr(logFileName="logToBuffer")
            self.model_manager = self.cntlr.modelManager
            # Ensure formula options are initialized and default to enabled (overridden per profile at validation time)
            try:
                if getattr(self.model_manager, "formulaOptions", None) is None:
                    self.model_manager.formulaOptions = FormulaOptions()
                # default behavior: allow formulas; per profile we may disable
                self.model_manager.formulaOptions.formulaAction = "run"
            except Exception as e:
                logger.warning(f"Unable to initialize formula options: {e}")
            # Ensure taxonomy package manager is initialized so catalog remappings
            # are registered via WebCache.TransformURL plugin hook
            try:
                from arelle import PackageManager as _PM
                _PM.init(self.cntlr, loadPackagesConfig=False)
                logger.info("Arelle PackageManager initialized for catalog remappings")
            except Exception as pm_err:
                logger.warning(f"PackageManager init failed or unavailable: {pm_err}")

            # Install a minimal WebCache.TransformURL hook that uses PackageManager.mappedUrl
            try:
                from arelle import PackageManager
                from arelle.PluginManager import pluginMethodsForClasses

                def _wc_transform(cntlr, url, base, *args, **kwargs):
                    try:
                        if url and PackageManager.isMappedUrl(url):
                            mapped = PackageManager.mappedUrl(url)
                            logger.debug(f"WebCache.TransformURL mapped: {url} -> {mapped}")
                            # Return mapped but allow WebCache to continue processing
                            # so archive paths like path.zip/inner/file are handled.
                            return mapped, False
                        # Fallback to config-driven offline roots
                        try:
                            cfg_offline_roots = (self._config or {}).get("offline_roots", []) or []
                            if url and cfg_offline_roots:
                                # Consider dual-variants of the requested URL and prefixes
                                url_variants = self._generate_dual_variants(url, is_prefix=False)
                                for mapping in cfg_offline_roots:
                                    url_prefix = mapping.get("url_prefix") or ""
                                    local_root = mapping.get("local_root") or ""
                                    if not url_prefix or not local_root:
                                        continue
                                    prefix_variants = self._generate_dual_variants(url_prefix, is_prefix=True)
                                    for candidate in url_variants:
                                        for pv in prefix_variants:
                                            if candidate.startswith(pv):
                                                rel_path = candidate[len(pv):].lstrip('/')
                                                local_path = (PROJECT_ROOT / local_root / rel_path).resolve()
                                                logger.debug(f"OfflineRoot mapped: {url} -> {local_path}")
                                                return str(local_path), False
                        except Exception:
                            pass
                    except Exception:
                        pass
                    return url, False

                hook_list = pluginMethodsForClasses.setdefault("WebCache.TransformURL", [])
                # Avoid duplicate installation
                if all(getattr(fn, "__name__", "") != _wc_transform.__name__ for fn in hook_list):
                    hook_list.insert(0, _wc_transform)
                    logger.info("Installed WebCache.TransformURL hook for PackageManager mappings")
            except Exception as hook_err:
                logger.warning(f"Failed to install WebCache.TransformURL hook: {hook_err}")
            
            # Configure strict offline mode and HTTP blocking
            if hasattr(self.cntlr, 'webCache'):
                self.cntlr.webCache.workOffline = True
                self.cntlr.webCache.maxAgeSeconds = 0
                # Set cache directory to backend/cache
                cache_dir = self.cache_dir.resolve()
                self.cntlr.webCache.cacheDir = str(cache_dir)
                logger.info(f"Arelle cache directory set to: {cache_dir}")
                # Install hard offline interceptors to record and block any HTTP/HTTPS open
                try:
                    opener = getattr(self.cntlr.webCache, 'opener', None)
                    if opener and hasattr(opener, 'open'):
                        _orig_open = opener.open
                        def _offline_open(req, *args, **kwargs):
                            try:
                                url = getattr(req, 'full_url', None) or getattr(req, 'get_full_url', lambda: None)()
                            except Exception:
                                url = None
                            if not url:
                                try:
                                    url = str(req)
                                except Exception:
                                    url = ''
                            if isinstance(url, bytes):
                                try:
                                    url = url.decode('utf-8', 'ignore')
                                except Exception:
                                    url = ''
                            if isinstance(url, str) and url.lower().startswith(('http://', 'https://')):
                                try:
                                    self._record_http_fetch_attempt(url, context='opener.open')
                                except Exception:
                                    pass
                                raise RuntimeError(f"Offline mode: network fetch blocked for {url}")
                            return _orig_open(req, *args, **kwargs)
                        # Avoid double-wrapping
                        if getattr(opener.open, '__name__', '') != '_offline_open':
                            opener.open = _offline_open
                            logger.info("Installed offline interceptor for webCache.opener.open")
                except Exception as e:
                    logger.warning(f"Failed to install opener offline interceptor: {e}")
                # Intercept getfilename to avoid unintended network fetches, but allow local mappings
                try:
                    _wc = self.cntlr.webCache
                    if hasattr(_wc, 'getfilename'):
                        _orig_getfilename = _wc.getfilename
                        def _offline_getfilename(url, *args, **kwargs):
                            # Permit getfilename to resolve local mappings; actual network access is blocked by opener
                            try:
                                return _orig_getfilename(url, *args, **kwargs)
                            except Exception:
                                raise
                        if getattr(_wc.getfilename, '__name__', '') != '_offline_getfilename':
                            _wc.getfilename = _offline_getfilename
                            logger.info("Installed offline interceptor for webCache.getfilename")
                except Exception as e:
                    logger.warning(f"Failed to install getfilename offline interceptor: {e}")
            
            # Block HTTP/HTTPS requests for security
            if hasattr(self.cntlr, 'internetConnectivity'):
                self.cntlr.internetConnectivity = 'offline'
            
            # Configure additional offline settings
            if hasattr(self.cntlr, 'config'):
                self.cntlr.config['internetConnectivity'] = 'offline'
                self.cntlr.config['workOffline'] = True
                self.cntlr.config['allow_catalogs'] = True  # Enable catalog remapping for offline mode
            
            self.packages_loaded = False
            self._loaded_package_paths = []
            self._catalog_map = {}
            self._catalog_entries = {"rewriteURI": [], "rewriteSystem": []}
            self._offline_mode = True
            self._http_fetch_attempts = []
            logger.info("ArelleService initialized successfully with offline configuration")
            
        except ImportError as e:
            logger.error(f"Failed to import Arelle: {e}")
            raise RuntimeError(f"Arelle not available: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize ArelleService: {e}")
            raise
    
    def load_taxonomy_packages(self, package_paths: List[str]):
        """
        Load taxonomy package mappings from unpacked folders.
        
        Args:
            package_paths: List of paths to unpacked taxonomy packages
        """
        try:
            if not self.cntlr:
                raise RuntimeError("ArelleService not initialized - call initialize() first")
                
            logger.info(f"Loading taxonomy packages from: {package_paths}")
            
            from arelle import PackageManager
            any_loaded = False
            warned_catalog_once = False
            for package_path in package_paths:
                path = Path(package_path)
                if not path.exists():
                    logger.warning(f"Package path does not exist: {package_path}")
                    continue

                logger.info(f"Loading package: {package_path}")

                try:
                    if path.is_file() and path.suffix.lower() == ".zip":
                        # Load zip package directly
                        if PackageManager.addPackage(self.cntlr, str(path)):
                            any_loaded = True
                            self._loaded_package_paths.append(str(path))
                            logger.info(f"Loaded taxonomy zip: {package_path}")
                        else:
                            logger.warning(f"addPackage did not return package info for: {package_path}")
                        continue

                    # If directory: prefer explicit manifest path if present
                    manifest_meta = path / "META-INF" / "taxonomyPackage.xml"
                    manifest_root = path / "taxonomyPackage.xml"
                    if manifest_meta.exists():
                        if PackageManager.addPackage(self.cntlr, str(manifest_meta)):
                            any_loaded = True
                            self._loaded_package_paths.append(str(path))
                            logger.info(f"Loaded taxonomy manifest: {manifest_meta}")
                        else:
                            logger.warning(f"addPackage did not return package info for: {manifest_meta}")
                        continue
                    if manifest_root.exists():
                        if PackageManager.addPackage(self.cntlr, str(manifest_root)):
                            any_loaded = True
                            self._loaded_package_paths.append(str(path))
                            logger.info(f"Loaded taxonomy manifest: {manifest_root}")
                        else:
                            logger.warning(f"addPackage did not return package info for: {manifest_root}")
                        continue

                    logger.warning(f"Unsupported package path (not zip and no manifest): {package_path}")

                except Exception as pkg_error:
                    logger.error(f"Failed to load package {package_path}: {pkg_error}")
                    continue
            
            # Rebuild Arelle remappings so mappedUrl() is active
            try:
                PackageManager.rebuildRemappings(self.cntlr)
                logger.info("Rebuilt PackageManager remappings")
            except Exception as e:
                logger.warning(f"Failed to rebuild remappings: {e}")

            self.packages_loaded = any_loaded
            logger.info("Taxonomy package loading completed")

            # Optional: build internal catalog map for diagnostics only
            self._build_catalog_map()
            self._register_catalogs_with_arelle()
            self._validate_catalog_registration()
            
        except Exception as e:
            logger.error(f"Failed to load taxonomy packages: {e}")
            raise

    def _normalize_url(self, url: str, is_prefix: bool = False) -> str:
        """Normalize URLs for consistent catalog matching."""
        try:
            parsed = urlparse(url)
        except Exception:
            # Fallback: collapse duplicate slashes in plain strings
            normalized = re.sub(r'/+', '/', url)
            if is_prefix and not normalized.endswith('/'):
                normalized += '/'
            return normalized

        if not parsed.scheme or not parsed.netloc:
            # Non-URL (likely local path) - collapse duplicate slashes only
            normalized_path = re.sub(r'/+', '/', parsed.path or url)
            if is_prefix and not normalized_path.endswith('/'):
                normalized_path = (normalized_path + '/').replace('//', '/')
            return normalized_path

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        original_path = parsed.path or '/'
        path = re.sub(r'/+', '/', original_path) or '/'

        if is_prefix:
            if not path.endswith('/'):
                path = f"{path}/"
        else:
            if not original_path.endswith('/') and path.endswith('/') and path not in ('/', ''):
                path = path.rstrip('/')
                if not path:
                    path = '/'

        normalized = urlunparse((scheme, netloc, path, '', '', ''))
        return normalized

    def _generate_dual_variants(self, url: str, is_prefix: bool = False) -> List[str]:
        """
        Generate normalized URL variants with and without `/eu/fr`.
        Returns unique values, keeping the first item as the normalized original.
        """
        variants: List[str] = []
        try:
            normalized = self._normalize_url(url, is_prefix=is_prefix)
            variants.append(normalized)

            parsed = urlparse(normalized)
            if not parsed.scheme or not parsed.netloc:
                return variants

            path = parsed.path or '/'
            if not path.startswith('/'):
                path = f"/{path}"

            # Variant without /eu/fr prefix
            if path.startswith('/eu/fr/'):
                without_path = path[len('/eu/fr'):]
            elif path == '/eu/fr':
                without_path = '/'
            else:
                without_path = path

            if not without_path.startswith('/'):
                without_path = f"/{without_path}"

            # Variant with /eu/fr prefix
            if path.startswith('/eu/fr'):
                with_path = path
            else:
                base_path = path if path.startswith('/') else f"/{path}"
                with_path = f"/eu/fr{base_path}"

            candidate_paths = {path, without_path, with_path}

            for candidate_path in candidate_paths:
                candidate = urlunparse((parsed.scheme, parsed.netloc, candidate_path, '', '', ''))
                candidate_normalized = self._normalize_url(candidate, is_prefix=is_prefix)
                if candidate_normalized not in variants:
                    variants.append(candidate_normalized)

        except Exception as e:
            logger.debug(f"URL variant generation failed for {url}: {e}")

        return variants

    def _resolve_offline_local_path(self, url: str) -> Optional[Path]:
        """
        Resolve a remote URL to a local filesystem path using configured offline_roots.
        Returns the first existing local path or None.
        """
        try:
            cfg_offline_roots = (self._config or {}).get("offline_roots", []) or []
            if not cfg_offline_roots:
                return None
            url_variants = self._generate_dual_variants(url, is_prefix=False)
            for mapping in cfg_offline_roots:
                url_prefix = mapping.get("url_prefix") or ""
                local_root = mapping.get("local_root") or ""
                if not url_prefix or not local_root:
                    continue
                prefix_variants = self._generate_dual_variants(url_prefix, is_prefix=True)
                for candidate in url_variants:
                    for pv in prefix_variants:
                        if candidate.startswith(pv):
                            rel_path = candidate[len(pv):].lstrip('/')
                            local_path = (PROJECT_ROOT / local_root / rel_path).resolve()
                            if local_path.exists():
                                return local_path
            return None
        except Exception:
            return None

    def _has_formula_docs(self, model_xbrl: Any) -> bool:
        """
        Check whether formula linkbase documents are present in the DTS.
        """
        try:
            # Direct formula docs list if populated
            if hasattr(model_xbrl, 'formulaLinkbaseDocumentObjects') and model_xbrl.formulaLinkbaseDocumentObjects:
                if len(model_xbrl.formulaLinkbaseDocumentObjects) > 0:
                    return True
            # Fallback: scan loaded URLs for val/vr markers
            if hasattr(model_xbrl, 'modelManager') and hasattr(model_xbrl.modelManager, 'urlDocs'):
                for url in getattr(model_xbrl.modelManager, 'urlDocs', {}).keys():
                    u = str(url)
                    if '/val/' in u or 'vr-' in u:
                        return True
        except Exception:
            pass
        return False

    def _detect_val_prefixes_from_dts(self, model_xbrl: Any) -> List[str]:
        """
        Inspect DTS URLs and attempt to derive one or more validation (val) URL prefixes
        for known frameworks (currently COREP 4.0).
        """
        prefixes: List[str] = []
        try:
            dts_urls: List[str] = []
            if hasattr(model_xbrl, 'modelManager') and hasattr(model_xbrl.modelManager, 'urlDocs'):
                dts_urls.extend([str(u) for u in getattr(model_xbrl.modelManager, 'urlDocs', {}).keys()])
            if hasattr(model_xbrl, 'modelDocument') and model_xbrl.modelDocument and hasattr(model_xbrl.modelDocument, 'referencedDocumentNames'):
                dts_urls.extend([str(u) for u in model_xbrl.modelDocument.referencedDocumentNames])
            # Heuristic: find any corep/4.0 path and construct its val prefix
            for u in dts_urls:
                if '/crr/fws/corep/4.0/' in u:
                    base = u.split('/crr/fws/corep/4.0/')[0]
                    prefix = f"{base}/crr/fws/corep/4.0/val/"
                    if prefix not in prefixes:
                        prefixes.append(prefix)
            return prefixes
        except Exception:
            return prefixes

    def _explicitly_load_vr_docs_for_prefix(self, model_xbrl: Any, val_prefix_url: str, max_files: int = 0) -> List[str]:
        """
        Explicitly load VR linkbase documents for a given val URL prefix into the DTS (dev-only fallback).
        Returns list of loaded local file URIs.
        """
        loaded: List[str] = []
        try:
            # Resolve the val prefix directory locally
            local_val_dir = self._resolve_offline_local_path(val_prefix_url)
            if not local_val_dir or not local_val_dir.is_dir():
                return loaded
            # Find vr-*.xml files
            vr_files = sorted(local_val_dir.glob('vr-*.xml'))
            if max_files and max_files > 0:
                vr_files = vr_files[:max_files]
            # Load each file as supplemental discovered document
            from arelle import ModelDocument
            for p in vr_files:
                uri = f"file://{p.as_posix()}"
                try:
                    doc = ModelDocument.load(model_xbrl, uri, isDiscovered=True, isSupplemental=True)
                    if doc is not None:
                        loaded.append(uri)
                        logger.info(f"Explicitly loaded VR linkbase: {uri}")
                except Exception as _e:
                    logger.debug(f"Failed loading VR linkbase {uri}: {_e}")
            return loaded
        except Exception as e:
            logger.debug(f"VR explicit load failed for prefix {val_prefix_url}: {e}")
            return loaded

    def _add_catalog_mapping(self, original_url: str, resolved_prefix: Path, entry_type: str, catalog_path: Path, rewrite_prefix: str):
        """Register catalog mappings with normalized aliases."""
        variants = self._generate_dual_variants(original_url, is_prefix=True)
        entry_record = {
            "catalog": str(catalog_path),
            "rewritePrefix": rewrite_prefix,
            "resolvedPrefix": str(resolved_prefix),
            "normalized": variants[0] if variants else self._normalize_url(original_url, is_prefix=True),
            "variants": variants,
            "entryType": entry_type
        }

        if entry_type == "rewriteURI":
            entry_record["uriStartString"] = original_url
        else:
            entry_record["systemIdStartString"] = original_url

        self._catalog_entries.setdefault(entry_type, []).append(entry_record)

        for variant in variants:
            self._catalog_map[variant] = str(resolved_prefix)

    def _find_catalog_match(self, url: str) -> Optional[Tuple[str, str, Path]]:
        """Find the best matching catalog prefix for the given URL."""
        best_prefix = None
        best_local_prefix = None

        for prefix, local_prefix in self._catalog_map.items():
            if url.startswith(prefix):
                if best_prefix is None or len(prefix) > len(best_prefix):
                    best_prefix = prefix
                    best_local_prefix = local_prefix

        if best_prefix and best_local_prefix:
            relative_part = url[len(best_prefix):]
            relative_part = relative_part.lstrip('/')
            resolved_path = Path(best_local_prefix)
            if relative_part:
                resolved_path = resolved_path / relative_part
            return best_prefix, best_local_prefix, resolved_path

        return None
    
    def _register_catalogs_with_arelle(self):
        """
        Verify catalog availability and Arelle's catalog resolution capability.
        
        Since Arelle's WebCache doesn't have an explicit addCatalog method,
        we verify that catalogs are available and that Arelle can resolve URLs
        using the catalog mappings from loaded packages.
        """
        try:
            logger.info("Verifying catalog availability and Arelle's catalog resolution capability")
            
            if not hasattr(self.cntlr, 'webCache') or not self.cntlr.webCache:
                logger.error("Arelle webCache not available for catalog verification")
                return
            
            available_catalogs = []
            catalog_verification_results = []
            
            for package_path in self._loaded_package_paths:
                catalog_path = Path(package_path) / "META-INF" / "catalog.xml"
                
                if catalog_path.exists():
                    available_catalogs.append(str(catalog_path))
                    logger.info(f"Catalog available: {catalog_path}")
                    
                    # Test catalog resolution by checking if Arelle can resolve a known URL
                    try:
                        # Test with a known dictionary URL
                        test_url = "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
                        resolved_path = self._test_catalog_resolution(test_url)
                        
                        verification_result = {
                            "catalog_path": str(catalog_path),
                            "test_url": test_url,
                            "resolved_path": resolved_path,
                            "resolution_successful": resolved_path is not None
                        }
                        catalog_verification_results.append(verification_result)
                        
                        if resolved_path:
                            logger.info(f"Catalog resolution test successful: {test_url} -> {resolved_path}")
                        else:
                            logger.warning(f"Catalog resolution test failed for: {test_url}")
                            
                    except Exception as test_error:
                        logger.warning(f"Catalog resolution test error for {catalog_path}: {test_error}")
                        catalog_verification_results.append({
                            "catalog_path": str(catalog_path),
                            "test_url": test_url,
                            "resolved_path": None,
                            "resolution_successful": False,
                            "error": str(test_error)
                        })
                else:
                    logger.warning(f"Catalog not found for package: {package_path}/META-INF/catalog.xml")
            
            # Log verification summary
            successful_resolutions = sum(1 for result in catalog_verification_results if result.get("resolution_successful", False))
            logger.info(f"Catalog verification completed: {len(available_catalogs)} catalogs available, {successful_resolutions} successful resolutions")
            
            if available_catalogs:
                logger.info("Available catalogs:")
                for catalog in available_catalogs:
                    logger.info(f"  - {catalog}")
            
            # Store results for debugging
            self._registered_catalogs = available_catalogs
            self._catalog_verification_results = catalog_verification_results
            
        except Exception as e:
            logger.error(f"Failed to verify catalogs with Arelle: {e}")
            raise
    
    def _test_catalog_resolution(self, url: str) -> Optional[str]:
        """
        Test catalog resolution by attempting to resolve a URL to a local path.
        
        Args:
            url: URL to test for resolution
            
        Returns:
            Local file path if resolved, None otherwise
        """
        try:
            # Use our existing catalog map to test resolution
            return self._resolve_dict_url(url)
        except Exception as e:
            logger.debug(f"Catalog resolution test failed for {url}: {e}")
            return None
    
    def _validate_catalog_registration(self):
        """
        Validate catalog availability and resolution capability.
        
        Since Arelle doesn't expose explicit catalog registration,
        we validate that catalogs are available and can resolve URLs.
        """
        try:
            logger.info("Validating catalog availability and resolution capability")
            
            if not hasattr(self.cntlr, 'webCache') or not self.cntlr.webCache:
                logger.error("Arelle webCache not available for validation")
                raise RuntimeError("Arelle webCache not available")
            
            # Check catalog availability
            available_catalogs = getattr(self, '_registered_catalogs', [])
            catalog_count = len(available_catalogs)
            
            logger.info(f"Found {catalog_count} available catalogs")
            
            # Log available catalogs
            if available_catalogs:
                logger.info("Available catalogs:")
                for i, catalog in enumerate(available_catalogs):
                    logger.info(f"  {i+1}. {catalog}")
            else:
                logger.warning("No catalogs found")
            
            # Check catalog resolution capability
            verification_results = getattr(self, '_catalog_verification_results', [])
            successful_resolutions = sum(1 for result in verification_results if result.get("resolution_successful", False))
            
            logger.info(f"Catalog resolution tests: {successful_resolutions}/{len(verification_results)} successful")
            
            # Validate against expected count
            expected_catalogs = len(self._loaded_package_paths)
            if catalog_count < expected_catalogs:
                logger.warning(f"Expected {expected_catalogs} catalogs but only {catalog_count} available")
                logger.warning("Some catalogs may not be accessible")
            else:
                logger.info(f"Catalog availability validation passed: {catalog_count} catalogs available")
            
            # Store validation results for debugging
            self._catalog_validation_results = {
                "expected_count": expected_catalogs,
                "actual_count": catalog_count,
                "available_catalogs": available_catalogs,
                "verification_results": verification_results,
                "successful_resolutions": successful_resolutions,
                "validation_passed": catalog_count >= expected_catalogs and successful_resolutions > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to validate catalog availability: {e}")
            raise
    
    def _build_catalog_map(self):
        """
        Build catalog map from all loaded package catalogs.
        
        Reads META-INF/catalog.xml files from loaded packages and creates
        a URL -> local_path mapping for dictionary schemas.
        """
        try:
            logger.info("Building catalog map from loaded packages")
            self._catalog_map = {}
            self._catalog_entries = {"rewriteURI": [], "rewriteSystem": []}
            
            for package_path in self._loaded_package_paths:
                p = Path(package_path)
                # Case 1: unpacked dir with META-INF/catalog.xml
                catalog_path = p / "META-INF" / "catalog.xml"
                if catalog_path.exists():
                    logger.info(f"Processing catalog: {catalog_path}")
                    try:
                        tree = ET.parse(str(catalog_path))
                        root = tree.getroot()
                        # Handle XML namespace for catalog elements
                        ns = {'catalog': 'urn:oasis:names:tc:entity:xmlns:xml:catalog'}
                        # Process entries
                        for rewrite_elem in root.findall('.//catalog:rewriteURI', ns):
                            uri_start = rewrite_elem.get('uriStartString')
                            rewrite_prefix = rewrite_elem.get('rewritePrefix')
                            if uri_start and rewrite_prefix:
                                base_path = p / "META-INF"
                                resolved_path = (base_path / rewrite_prefix).resolve()
                                self._add_catalog_mapping(uri_start, resolved_path, "rewriteURI", catalog_path, rewrite_prefix)
                        for rewrite_elem in root.findall('.//catalog:rewriteSystem', ns):
                            system_start = rewrite_elem.get('systemIdStartString')
                            rewrite_prefix = rewrite_elem.get('rewritePrefix')
                            if system_start and rewrite_prefix:
                                base_path = p / "META-INF"
                                resolved_path = (base_path / rewrite_prefix).resolve()
                                self._add_catalog_mapping(system_start, resolved_path, "rewriteSystem", catalog_path, rewrite_prefix)
                        logger.info(f"Processed {len([e for e in root.findall('.//catalog:rewriteURI', ns)])} rewriteURI entries from {catalog_path}")
                    except ET.ParseError as e:
                        logger.warning(f"Failed to parse catalog {catalog_path}: {e}")
                    except Exception as e:
                        logger.warning(f"Error processing catalog {catalog_path}: {e}")
                    continue
                # Case 2: zip package - attempt to read META-INF/catalog.xml from zip
                if p.is_file() and p.suffix.lower() == ".zip":
                    import zipfile
                    try:
                        with zipfile.ZipFile(str(p)) as zf:
                            candidates = [n for n in zf.namelist() if n.endswith('META-INF/catalog.xml')]
                            for name in candidates:
                                try:
                                    fh = zf.open(name)
                                    try:
                                        tree = ET.parse(fh)
                                    finally:
                                        fh.close()
                                    root = tree.getroot()
                                    ns = {'catalog': 'urn:oasis:names:tc:entity:xmlns:xml:catalog'}
                                    base_prefix_in_zip = str(Path(name).parent)
                                    for rewrite_elem in root.findall('.//catalog:rewriteURI', ns):
                                        uri_start = rewrite_elem.get('uriStartString')
                                        rewrite_prefix = rewrite_elem.get('rewritePrefix')
                                        if uri_start and rewrite_prefix:
                                            resolved_path = p / base_prefix_in_zip / rewrite_prefix
                                            self._add_catalog_mapping(uri_start, resolved_path, "rewriteURI", p, rewrite_prefix)
                                    for rewrite_elem in root.findall('.//catalog:rewriteSystem', ns):
                                        system_start = rewrite_elem.get('systemIdStartString')
                                        rewrite_prefix = rewrite_elem.get('rewritePrefix')
                                        if system_start and rewrite_prefix:
                                            resolved_path = p / base_prefix_in_zip / rewrite_prefix
                                            self._add_catalog_mapping(system_start, resolved_path, "rewriteSystem", p, rewrite_prefix)
                                    logger.info(f"Processed catalog from zip: {p}!{name}")
                                except Exception as ze:
                                    logger.warning(f"Failed processing catalog entry in zip {p}!{name}: {ze}")
                    except Exception as e:
                        logger.warning(f"Failed processing zip catalog for {p}: {e}")
                    
            
            logger.info(
                f"Catalog map built with {len(self._catalog_entries['rewriteURI'])} rewriteURI entries "
                f"and {len(self._catalog_entries['rewriteSystem'])} rewriteSystem entries"
            )
            
        except Exception as e:
            logger.error(f"Failed to build catalog map: {e}")
            self._catalog_map = {}
    
    def _check_offline_violations(self):
        """
        Check for any HTTP fetch attempts in offline mode and fail closed if detected.
        """
        if not self._offline_mode:
            return
            
        if self._http_fetch_attempts:
            error_msg = f"Offline mode violation: {len(self._http_fetch_attempts)} HTTP fetch attempts detected"
            logger.error(error_msg)
            for attempt in self._http_fetch_attempts:
                logger.error(f"  HTTP fetch attempt: {attempt}")
            raise RuntimeError(f"{error_msg}. Check catalog mappings and ensure all schemas resolve to local files.")
    
    def _record_http_fetch_attempt(self, url: str, context: str = ""):
        """
        Record an HTTP fetch attempt for offline mode violation detection.
        
        Args:
            url: The URL that was attempted to be fetched
            context: Additional context about where the fetch was attempted
        """
        if self._offline_mode:
            attempt_info = f"{url}"
            if context:
                attempt_info += f" (context: {context})"
            self._http_fetch_attempts.append(attempt_info)
            logger.warning(f"HTTP fetch attempt recorded in offline mode: {attempt_info}")
    
    def _get_package_version_info(self, local_path: str) -> Optional[Dict[str, str]]:
        """
        Get package version information from taxonomyPackage.xml if available.
        
        Args:
            local_path: Local path to the schema file
            
        Returns:
            Dictionary with package version info or None if not available
        """
        try:
            # Find the package root by looking for META-INF/taxonomyPackage.xml
            path = Path(local_path)
            
            # Walk up the directory tree to find META-INF/taxonomyPackage.xml
            for parent in [path] + list(path.parents):
                package_manifest = parent / "META-INF" / "taxonomyPackage.xml"
                if package_manifest.exists():
                    try:
                        tree = ET.parse(str(package_manifest))
                        root = tree.getroot()
                        
                        # Extract package information
                        package_info = {}
                        
                        # Get package name and version
                        if hasattr(root, 'tag'):
                            # Handle different XML namespaces
                            for elem in root.iter():
                                if elem.tag.endswith('name') and elem.text:
                                    package_info['name'] = elem.text
                                elif elem.tag.endswith('version') and elem.text:
                                    package_info['version'] = elem.text
                                elif elem.tag.endswith('uri') and elem.text:
                                    package_info['uri'] = elem.text
                        
                        # Fallback: try to get info from root attributes
                        if not package_info and hasattr(root, 'attrib'):
                            package_info.update(root.attrib)
                        
                        # Add relative path from package root
                        try:
                            rel_path = path.relative_to(parent)
                            package_info['relative_path'] = str(rel_path)
                        except ValueError:
                            pass
                        
                        return package_info if package_info else None
                        
                    except ET.ParseError:
                        continue
                    except Exception:
                        continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract package version info for {local_path}: {e}")
            return None
    
    def _resolve_dict_url(self, url: str) -> Optional[str]:
        """
        Resolve a dictionary URL to local file path using catalog mappings.
        
        Args:
            url: Dictionary schema URL (e.g., http://www.eba.europa.eu/xbrl/crr/dict/met/met.xsd)
            
        Returns:
            Local file path if resolved, None otherwise
        """
        try:
            # Prefer Arelle PackageManager remappings (works for zip-internal paths)
            try:
                from arelle import PackageManager
                if PackageManager.isMappedUrl(url):
                    mapped = PackageManager.mappedUrl(url)
                    logger.debug(f"PackageManager mapped URL: {url} -> {mapped}")
                    return mapped
            except Exception:
                pass

            # Fallback: internal catalog map (directories only)
            if url.startswith(('http://', 'https://')):
                normalized_variants = self._generate_dual_variants(url, is_prefix=False)
                for candidate in normalized_variants:
                    match = self._find_catalog_match(candidate)
                    if not match:
                        continue
                    _, _, resolved_path = match
                    if resolved_path.exists() and resolved_path.is_file():
                        return str(resolved_path)
                # As last resort, apply offline_roots mapping directly
                try:
                    cfg_offline_roots = (self._config or {}).get("offline_roots", []) or []
                    for candidate in normalized_variants:
                        for mapping in cfg_offline_roots:
                            url_prefix = mapping.get("url_prefix") or ""
                            local_root = mapping.get("local_root") or ""
                            if not url_prefix or not local_root:
                                continue
                            prefix_variants = self._generate_dual_variants(url_prefix, is_prefix=True)
                            for pv in prefix_variants:
                                if candidate.startswith(pv):
                                    rel_path = candidate[len(pv):].lstrip('/')
                                    local_path = (PROJECT_ROOT / local_root / rel_path).resolve()
                                    if local_path.exists():
                                        return str(local_path)
                except Exception:
                    pass
                logger.debug(f"No local mapping found for URL (including variants): {url}")
                return None
            # Non-HTTP: treat as local path
            if Path(url).exists():
                logger.debug(f"Local path found: {url}")
                return url
            logger.debug(f"Local path not found: {url}")
            return None
        except Exception as e:
            logger.warning(f"Error resolving URL {url}: {e}")
            return None

    def get_catalog_introspection(self) -> Dict[str, Any]:
        """
        Return a structured view of catalog mappings for introspection.
        Behavior-neutral (read-only). Useful for Phase 2A.
        """
        try:
            return {
                "packages": self._loaded_package_paths.copy(),
                "rewriteURI_count": len(self._catalog_entries.get("rewriteURI", [])),
                "rewriteSystem_count": len(self._catalog_entries.get("rewriteSystem", [])),
                "rewriteURI": self._catalog_entries.get("rewriteURI", []).copy(),
                "rewriteSystem": self._catalog_entries.get("rewriteSystem", []).copy(),
            }
        except Exception as e:
            logger.warning(f"Failed to build catalog introspection: {e}")
            return {"error": str(e)}
    
    def probe_url_resolution(self, url: str) -> Dict[str, Any]:
        """
        Probe URL resolution for debugging and visibility.
        
        Tests URL variants and catalog mappings to help diagnose resolution issues.
        
        Args:
            url: URL to probe for resolution
            
        Returns:
            Dictionary with resolution probe results
        """
        try:
            probe_results = {
                "original_url": url,
                "normalized_url": self._normalize_url(url),
                "variants": self._generate_dual_variants(url),
                "catalog_matches": [],
                "resolution_successful": False,
                "resolved_path": None,
                "file_exists": False,
                "probe_timestamp": datetime.now().isoformat()
            }
            
            # Test each variant for catalog resolution
            for variant in probe_results["variants"]:
                match_result = self._find_catalog_match(variant)
                
                match_info = {
                    "variant": variant,
                    "matched": match_result is not None
                }
                
                if match_result:
                    prefix, local_prefix, resolved_path = match_result
                    match_info.update({
                        "matched_prefix": prefix,
                        "local_prefix": local_prefix,
                        "resolved_path": str(resolved_path),
                        "file_exists": resolved_path.exists() and resolved_path.is_file()
                    })
                    
                    # If this is the first successful resolution, record it
                    if not probe_results["resolution_successful"] and match_info["file_exists"]:
                        probe_results["resolution_successful"] = True
                        probe_results["resolved_path"] = str(resolved_path)
                        probe_results["file_exists"] = True
                
                probe_results["catalog_matches"].append(match_info)
            
            # Test direct resolution via _resolve_dict_url
            direct_resolution = self._resolve_dict_url(url)
            probe_results["direct_resolution"] = {
                "resolved_path": direct_resolution,
                "file_exists": direct_resolution and Path(direct_resolution).exists() if direct_resolution else False
            }
            
            return probe_results
            
        except Exception as e:
            logger.error(f"URL resolution probe failed for {url}: {e}")
            return {
                "original_url": url,
                "error": str(e),
                "probe_timestamp": datetime.now().isoformat()
            }
    
    def _preload_dts_schemas(self, schema_urls: List[str]) -> Optional[Dict[str, Any]]:
        """
        Preload schemas and return concept mappings for DTS-first loading.
        
        This implements DTS-first loading by preloading schemas and extracting
        their concepts, which can then be merged into the instance model.
        
        Args:
            schema_urls: List of schema URLs to preload
            
        Returns:
            Dictionary with preloaded concepts and metadata, or None if loading failed
        """
        try:
            import time
            from datetime import datetime
            
            logger.info(f"Preloading {len(schema_urls)} schemas for DTS-first loading")
            
            # Create a new model for the DTS
            from arelle import FileSource
            
            preloaded_concepts = {}
            schemas_loaded = 0
            provenance_log = []
            
            for schema_url in schema_urls:
                try:
                    start_time = time.time()
                    
                    # Resolve URL to local path using catalog
                    local_path = self._resolve_dict_url(schema_url)
                    
                    if local_path and Path(local_path).exists():
                        logger.info(f"Preloading schema: {schema_url} -> {local_path}")
                        
                        # Load schema as separate model to extract concepts
                        schema_file_source = FileSource.openFileSource(local_path, self.cntlr)
                        schema_model = self.model_manager.load(schema_file_source)
                        
                        load_duration_ms = int((time.time() - start_time) * 1000)
                        
                        if schema_model and hasattr(schema_model, 'qnameConcepts'):
                            # Extract concepts from this schema
                            schema_concepts = dict(schema_model.qnameConcepts)
                            preloaded_concepts.update(schema_concepts)
                            schemas_loaded += 1
                            
                            logger.info(f"Preloaded {len(schema_concepts)} concepts from {schema_url} ({load_duration_ms}ms)")
                            
                            # Record provenance
                            provenance_log.append({
                                "schema_url": schema_url,
                                "local_path": local_path,
                                "concepts_count": len(schema_concepts),
                                "load_duration_ms": load_duration_ms,
                                "status": "success",
                                "timestamp": datetime.now().isoformat()
                            })
                        else:
                            logger.warning(f"Failed to load schema or extract concepts: {schema_url}")
                            provenance_log.append({
                                "schema_url": schema_url,
                                "local_path": local_path,
                                "status": "failed",
                                "error": "schema load returned None or no concepts",
                                "timestamp": datetime.now().isoformat()
                            })
                    else:
                        logger.warning(f"Could not resolve schema URL for DTS preload: {schema_url}")
                        provenance_log.append({
                            "schema_url": schema_url,
                            "status": "unresolved",
                            "error": "no catalog mapping found",
                            "timestamp": datetime.now().isoformat()
                        })
                        
                except Exception as schema_error:
                    logger.warning(f"Failed to preload schema {schema_url}: {schema_error}")
                    provenance_log.append({
                        "schema_url": schema_url,
                        "status": "error",
                        "error": str(schema_error),
                        "timestamp": datetime.now().isoformat()
                    })
                    continue
            
            total_concepts = len(preloaded_concepts)
            logger.info(f"DTS preloading completed: {schemas_loaded}/{len(schema_urls)} schemas loaded, {total_concepts} total concepts")
            
            # Log provenance summary
            logger.info("=== DTS Preloading Provenance ===")
            for entry in provenance_log:
                logger.info(f"Schema: {entry['schema_url']}")
                logger.info(f"  Status: {entry['status']}")
                if entry['status'] == 'success':
                    logger.info(f"  Local path: {entry['local_path']}")
                    logger.info(f"  Concepts: {entry['concepts_count']}")
                    logger.info(f"  Load time: {entry['load_duration_ms']}ms")
                elif entry.get('error'):
                    logger.info(f"  Error: {entry['error']}")
                logger.info(f"  Timestamp: {entry['timestamp']}")
            logger.info("=== End DTS Preloading Provenance ===")
            
            # Verify we have the expected concepts
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            eba_met_concepts = [
                concept for qname, concept in preloaded_concepts.items()
                if qname.namespaceURI == eba_met_ns
            ]
            
            if eba_met_concepts:
                logger.info(f"DTS preloading successful: {len(eba_met_concepts)} eba_met concepts available")
            else:
                logger.warning("DTS preloading completed but no eba_met concepts found")
            
            return {
                "concepts": preloaded_concepts,
                "schemas_loaded": schemas_loaded,
                "total_concepts": total_concepts,
                "eba_met_concepts": len(eba_met_concepts),
                "provenance": provenance_log
            }
            
        except Exception as e:
            logger.error(f"Failed to preload DTS schemas: {e}")
            return None
    
    def _choose_resolvable_schema_url(self, schema_urls: List[str]) -> Optional[Tuple[str, str]]:
        """
        Choose the first schema URL that resolves to an existing local path via Arelle webCache.
        Returns (chosen_url, local_file_path) if successful, else None.
        """
        try:
            if not hasattr(self, "cntlr") or not self.cntlr or not hasattr(self.cntlr, "webCache"):
                logger.warning("webCache not available; cannot choose resolvable schema URL")
                return None

            logger.info(f"Testing {len(schema_urls)} schema URL variants via webCache.getfilename")

            for schema_url in schema_urls:
                try:
                    local_path = self.cntlr.webCache.getfilename(schema_url, normalize=True, filenameOnly=True)
                except Exception as e:
                    logger.debug(f"webCache.getfilename failed for {schema_url}: {e}")
                    local_path = None

                if local_path and Path(local_path).exists():
                    logger.info(f"webCache resolved {schema_url} -> {local_path}")
                    return schema_url, str(local_path)
                else:
                    logger.debug(f"webCache did not resolve to existing path for {schema_url}: {local_path}")

            logger.warning("No schema URL variants resolved to existing local files via webCache; trying internal probe fallback")

            # Fallback: use internal probe (catalog introspection path) to find an existing file
            for schema_url in schema_urls:
                try:
                    probe = self.probe_url_resolution(schema_url)
                    if probe.get("resolution_successful") and probe.get("file_exists"):
                        resolved_path = probe.get("resolved_path") or probe.get("direct_resolution", {}).get("resolved_path")
                        if resolved_path and Path(resolved_path).exists():
                            logger.info(f"Probe fallback resolved {schema_url} -> {resolved_path}")
                            return schema_url, str(resolved_path)
                except Exception as e:
                    logger.debug(f"Probe fallback failed for {schema_url}: {e}")

            logger.warning("No schema URL variants resolved via webCache or probe fallback")
            return None

        except Exception as e:
            logger.error(f"Error choosing resolvable schema URL via webCache: {e}")
            return None

    def _detect_missing_dictionary_namespaces_with_injection(self, file_path: str) -> Optional[Tuple[List[str], bool, bool]]:
        """
        Detect missing dictionary namespaces and prepare for injection.
        
        Args:
            file_path: Path to XBRL instance file
            
        Returns:
            Tuple of (missing_schema_urls, injection_used, temp_fallback_used) or None if no injection needed
        """
        try:
            # Check if DTS-first injection is enabled
            # Flags may be present at top-level (flattened) and under flags
            flags = (self._config or {}).get('flags', {}) or {}
            # Prefer nested flags overrides; fall back to top-level for backward compatibility
            enable_dts_first = flags.get('enable_dts_first_api', self._config.get('enable_dts_first_api', False))
            inject_schema_refs = flags.get('dts_first_inject_schema_refs', self._config.get('dts_first_inject_schema_refs', False))
            allow_rewrite = flags.get('allow_instance_rewrite', self._config.get('allow_instance_rewrite', False))
            
            logger.info(f"DTS-first flags - enable_dts_first_api: {enable_dts_first}, "
                       f"dts_first_inject_schema_refs: {inject_schema_refs}, "
                       f"allow_instance_rewrite: {allow_rewrite}")
            
            # Only proceed if DTS-first injection is enabled
            if not (enable_dts_first and inject_schema_refs):
                logger.debug("DTS-first injection not enabled")
                return None
            
            # Read file content for analysis
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for eba_met namespace usage
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            has_eba_met_namespace = f'xmlns:eba_met="{eba_met_ns}"' in content
            has_eba_met_elements = '<eba_met:' in content
            
            logger.info(f"eba_met namespace declared: {has_eba_met_namespace}, elements found: {has_eba_met_elements}")
            
            if has_eba_met_namespace and has_eba_met_elements:
                # Check if met.xsd is already referenced
                met_schema_referenced = 'dict/met/met.xsd' in content
                
                if not met_schema_referenced:
                    # Test both URL variants
                    schema_urls = [
                        "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd",
                        "http://www.eba.europa.eu/xbrl/crr/dict/met/met.xsd"
                    ]
                    
                    logger.info(f"Detected missing dictionary schema for eba_met namespace")
                    logger.info(f"Testing URL variants: {schema_urls}")
                    
                    return schema_urls, True, False  # injection_used=True, temp_fallback_used=False initially
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to detect missing namespaces with injection: {e}")
            return None

    def _detect_missing_dictionary_namespaces(self, file_path: str) -> List[str]:
        """
        Detect missing dictionary namespaces by parsing the instance file.
        
        Args:
            file_path: Path to XBRL instance file
            
        Returns:
            List of missing dictionary schema URLs that need to be injected
        """
        try:
            # Read the file as text to parse namespace declarations
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if eba_met namespace is declared and used
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            has_eba_met_namespace = f'xmlns:eba_met="{eba_met_ns}"' in content
            has_eba_met_elements = '<eba_met:' in content
            
            logger.info(f"eba_met namespace declared: {has_eba_met_namespace}, elements found: {has_eba_met_elements}")
            
            missing_schemas = []
            
            if has_eba_met_namespace and has_eba_met_elements:
                # Check if met.xsd is already referenced in schemaRef
                met_schema_referenced = 'dict/met/met.xsd' in content
                
                if not met_schema_referenced:
                    missing_schemas.append("http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd")
                    logger.info(f"Detected missing dictionary schema: met.xsd for eba_met namespace")
                else:
                    logger.info("met.xsd already referenced in instance")
            
            return missing_schemas
            
        except Exception as e:
            logger.warning(f"Failed to detect missing namespaces in {file_path}: {e}")
            return []
    
    def _create_in_memory_injection(self, original_content: str, schema_urls: List[str]) -> Optional[str]:
        """
        Create in-memory XML with injected schemaRef elements.
        
        Args:
            original_content: Original instance XML content
            schema_urls: List of schema URLs to inject
            
        Returns:
            Modified XML content with injected schemaRefs, or None if injection failed
        """
        try:
            logger.info(f"Creating in-memory injection for {len(schema_urls)} schema URLs")
            
            # Unconditionally inject provided URLs (Option A): Arelle will map during discovery
            resolvable_schemas: List[str] = list(schema_urls)
            
            # Build the injection content
            content = original_content
            
            # Find the first existing schemaRef to insert before it
            existing_schema_ref_pattern = '<link:schemaRef xlink:type="simple" xlink:href="'
            schema_ref_pos = content.find(existing_schema_ref_pattern)
            
            if schema_ref_pos == -1:
                logger.error("Could not find existing schemaRef to insert before")
                return None
            
            # Extract indentation from existing schemaRef
            line_start = content.rfind('\n', 0, schema_ref_pos) + 1
            line_content = content[line_start:schema_ref_pos]
            
            indentation = ''
            for char in line_content:
                if char in [' ', '\t']:
                    indentation += char
                else:
                    break
            
            # Inject all resolvable schemas
            injection_content = ''
            for schema_url in resolvable_schemas:
                injection_content += f'{indentation}<link:schemaRef xlink:type="simple" xlink:href="{schema_url}"/>\n'
                logger.info(f"Injected schemaRef: {schema_url}")
            
            # Insert before existing schemaRef
            modified_content = content[:schema_ref_pos] + injection_content + content[schema_ref_pos:]
            
            logger.info(f"Successfully created in-memory injection with {len(resolvable_schemas)} schemaRefs")
            return modified_content
            
        except Exception as e:
            logger.error(f"Failed to create in-memory injection: {e}")
            return None

    def _create_temp_instance_with_schema_refs(self, original_path: str, additional_schema_refs: List[str]) -> Optional[str]:
        """
        Create a temporary copy of the instance with additional schemaRef elements.
        
        Args:
            original_path: Path to original instance file
            additional_schema_refs: List of schema URLs to inject as schemaRefs
            
        Returns:
            Path to temporary instance file, or None if creation failed
        """
        try:
            import uuid
            from pathlib import Path
            
            # Ensure temp directory exists
            temp_dir = Path(__file__).resolve().parents[2] / "temp"
            temp_dir.mkdir(exist_ok=True)
            
            # Generate unique temp filename
            original_name = Path(original_path).stem
            temp_filename = f"{original_name}_injected_{uuid.uuid4().hex[:8]}.xbrl"
            temp_path = temp_dir / temp_filename
            
            # Read the original instance as text
            with open(original_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find the first existing schemaRef and insert new ones before it
            # This preserves all namespace prefixes and formatting
            for schema_url in additional_schema_refs:
                # Resolve URL to local path via catalog
                local_path = self._resolve_dict_url(schema_url)
                
                # Fallback: try config-driven offline_roots direct mapping if still unresolved
                if not local_path:
                    try:
                        cfg_offline_roots = (self._config or {}).get("offline_roots", []) or []
                        if cfg_offline_roots:
                            url_variants = self._generate_dual_variants(schema_url, is_prefix=False)
                            for mapping in cfg_offline_roots:
                                url_prefix = mapping.get("url_prefix") or ""
                                local_root = mapping.get("local_root") or ""
                                if not url_prefix or not local_root:
                                    continue
                                prefix_variants = self._generate_dual_variants(url_prefix, is_prefix=True)
                                for candidate in url_variants:
                                    for pv in prefix_variants:
                                        if candidate.startswith(pv):
                                            rel_path = candidate[len(pv):].lstrip('/')
                                            lp = (PROJECT_ROOT / local_root / rel_path).resolve()
                                            if lp.exists() and lp.is_file():
                                                local_path = str(lp)
                                                raise StopIteration
                    except StopIteration:
                        pass
                    except Exception:
                        pass
                
                # Detect if PackageManager knows this URL (zip-internal paths won't exist on FS)
                is_pm_mapped = False
                try:
                    from arelle import PackageManager
                    is_pm_mapped = PackageManager.isMappedUrl(schema_url)
                except Exception:
                    pass
                
                # Choose href to inject: prefer original URL when PackageManager maps it; otherwise inject file:// URI
                href_to_inject = schema_url
                if (not is_pm_mapped) and local_path and Path(local_path).exists():
                    lp_abs = Path(local_path).resolve()
                    href_to_inject = f"file://{lp_abs}"
                
                if is_pm_mapped or (local_path and Path(local_path).exists()):
                    # Create schemaRef element as text (indentation will be set later)
                    new_schema_ref = f'<link:schemaRef xlink:type="simple" xlink:href="{href_to_inject}"/>'+"\n"
                    
                    # Find the first existing schemaRef to insert before it
                    existing_schema_ref_pattern = '<link:schemaRef xlink:type="simple" xlink:href="'
                    schema_ref_pos = content.find(existing_schema_ref_pattern)
                    
                    if schema_ref_pos != -1:
                        # Find the start of the line to get the exact indentation
                        line_start = content.rfind('\n', 0, schema_ref_pos) + 1
                        line_content = content[line_start:schema_ref_pos]
                        
                        # Extract the indentation from the existing schemaRef line
                        indentation = ''
                        for char in line_content:
                            if char in [' ', '\t']:
                                indentation += char
                            else:
                                break
                        
                        # Create schemaRef with matching indentation
                        new_schema_ref = f'{indentation}<link:schemaRef xlink:type="simple" xlink:href="{href_to_inject}"/>'+"\n"
                        
                        # Insert before the existing schemaRef
                        content = content[:schema_ref_pos] + new_schema_ref + content[schema_ref_pos:]
                        logger.info(f"Injected schemaRef for {schema_url} -> {href_to_inject} (pm_mapped={is_pm_mapped}, local_path={local_path})")
                    else:
                        logger.warning(f"Could not find existing schemaRef to insert before for: {schema_url}")
                else:
                    logger.warning(f"Could not resolve schema URL for injection: {schema_url}")
            
            # Write the modified instance to temp file
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Created temp instance with injected schemaRefs: {temp_path}")
            return str(temp_path)
            
        except Exception as e:
            logger.error(f"Failed to create temp instance with schema refs: {e}")
            return None
    
    def _ensure_dictionary_schemas_loaded(self, model_xbrl: Any):
        """
        Ensure dictionary schemas are loaded for concept resolution.
        
        This method checks if eba_met namespace concepts are available,
        and if not, loads the required dictionary schemas using catalog resolution.
        
        Args:
            model_xbrl: Loaded ModelXbrl instance
        """
        try:
            import time
            import os
            from datetime import datetime
            
            # Check if eba_met concepts are already available
            eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
            eba_met_concepts = [
                concept for qname, concept in model_xbrl.qnameConcepts.items()
                if qname.namespaceURI == eba_met_ns
            ]
            
            if eba_met_concepts:
                logger.info(f"Found {len(eba_met_concepts)} eba_met concepts already loaded")
                return
            
            logger.info("No eba_met concepts found, loading dictionary schemas via catalog...")
            logger.info(f"Current model has {len(model_xbrl.qnameConcepts)} concepts total")
            
            # Define dictionary schema URLs to resolve
            dict_schema_urls = [
                "http://www.eba.europa.eu/xbrl/crr/dict/met/met.xsd",
                "http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"
            ]
            
            schemas_loaded = 0
            provenance_log = []
            
            for schema_url in dict_schema_urls:
                try:
                    start_time = time.time()
                    
                    # Resolve URL to local path using catalog
                    local_path = self._resolve_dict_url(schema_url)
                    
                    if local_path and Path(local_path).exists():
                        # Get file metadata for provenance
                        file_stat = os.stat(local_path)
                        file_size = file_stat.st_size
                        file_mtime = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                        
                        # Get package version info if available
                        package_info = self._get_package_version_info(local_path)
                        
                        logger.info(f"Resolved {schema_url} -> {local_path}")
                        
                        # Load schema into the same model as the instance
                        from arelle import FileSource
                        schema_file_source = FileSource.openFileSource(local_path, self.cntlr)
                        schema_model = model_xbrl.modelManager.load(schema_file_source)
                        
                        # Merge schema concepts into the instance model
                        if schema_model and hasattr(schema_model, 'qnameConcepts'):
                            for qname, concept in schema_model.qnameConcepts.items():
                                if qname not in model_xbrl.qnameConcepts:
                                    model_xbrl.qnameConcepts[qname] = concept
                            
                            # Also merge the schema model into the instance's DTS
                            if hasattr(schema_model, 'modelDocument') and schema_model.modelDocument:
                                if hasattr(model_xbrl, 'modelDocument') and model_xbrl.modelDocument:
                                    # Add schema model document to instance's DTS
                                    if not hasattr(model_xbrl.modelDocument, 'referencedNamespaces'):
                                        model_xbrl.modelDocument.referencedNamespaces = set()
                                    if hasattr(schema_model.modelDocument, 'referencedNamespaces'):
                                        model_xbrl.modelDocument.referencedNamespaces.update(schema_model.modelDocument.referencedNamespaces)
                        
                        load_duration_ms = int((time.time() - start_time) * 1000)
                        
                        if schema_model:
                            logger.info(f"Successfully loaded schema into instance model: {schema_url}")
                            schemas_loaded += 1
                            
                            # Record provenance information
                            provenance_entry = {
                                "resolved_url": schema_url,
                                "local_path": local_path,
                                "file_size_bytes": file_size,
                                "file_mtime": file_mtime,
                                "load_duration_ms": load_duration_ms,
                                "package_info": package_info,
                                "timestamp": datetime.now().isoformat(),
                                "status": "success"
                            }
                            provenance_log.append(provenance_entry)
                            
                            logger.info(f"Provenance: {schema_url} -> {local_path} ({file_size} bytes, {file_mtime})")
                        else:
                            logger.warning(f"Failed to load schema into instance model: {schema_url}")
                            provenance_entry = {
                                "resolved_url": schema_url,
                                "local_path": local_path,
                                "status": "failed",
                                "error": "schema_model is None",
                                "timestamp": datetime.now().isoformat()
                            }
                            provenance_log.append(provenance_entry)
                    else:
                        logger.debug(f"Could not resolve dictionary schema URL: {schema_url}")
                        provenance_entry = {
                            "resolved_url": schema_url,
                            "status": "unresolved",
                            "error": "no catalog mapping found",
                            "timestamp": datetime.now().isoformat()
                        }
                        provenance_log.append(provenance_entry)
                        
                except Exception as schema_error:
                    logger.warning(f"Failed to load dictionary schema {schema_url}: {schema_error}")
                    provenance_entry = {
                        "resolved_url": schema_url,
                        "status": "error",
                        "error": str(schema_error),
                        "timestamp": datetime.now().isoformat()
                    }
                    provenance_log.append(provenance_entry)
                    continue
            
            # Verify eba_met concepts are now available
            eba_met_concepts_after = [
                concept for qname, concept in model_xbrl.qnameConcepts.items()
                if qname.namespaceURI == eba_met_ns
            ]
            
            logger.info(f"After schema loading, model has {len(model_xbrl.qnameConcepts)} concepts total")
            if eba_met_concepts_after:
                logger.info(f"Successfully loaded {len(eba_met_concepts_after)} eba_met concepts via catalog resolution")
            else:
                logger.warning(f"Dictionary schema loading completed ({schemas_loaded} schemas loaded) but no eba_met concepts found")
                # Log some sample concepts to see what we have
                sample_concepts = list(model_xbrl.qnameConcepts.keys())[:5]
                logger.info(f"Sample concepts in model: {sample_concepts}")
                
                # Fail closed with clear error message
                if schemas_loaded == 0:
                    logger.error("No dictionary schemas could be resolved via catalog; check catalog mappings and package paths")
            
            # Log comprehensive provenance summary
            logger.info("=== Dictionary Schema Loading Provenance ===")
            for entry in provenance_log:
                logger.info(f"Schema: {entry['resolved_url']}")
                logger.info(f"  Status: {entry['status']}")
                if entry['status'] == 'success':
                    logger.info(f"  Local path: {entry['local_path']}")
                    logger.info(f"  File size: {entry['file_size_bytes']} bytes")
                    logger.info(f"  Modified: {entry['file_mtime']}")
                    logger.info(f"  Load time: {entry['load_duration_ms']}ms")
                    if entry.get('package_info'):
                        logger.info(f"  Package: {entry['package_info']}")
                elif entry.get('error'):
                    logger.info(f"  Error: {entry['error']}")
                logger.info(f"  Timestamp: {entry['timestamp']}")
            logger.info("=== End Provenance ===")
            
            # Strict enforcement: any HTTP attempt is an error in offline mode
            if self._http_fetch_attempts:
                self._check_offline_violations()
                
        except Exception as e:
            logger.error(f"Error ensuring dictionary schemas loaded: {e}")
            # Check for offline violations even on error
            try:
                self._check_offline_violations()
            except RuntimeError as offline_error:
                # Re-raise offline violations as they are critical
                raise offline_error
            # Don't raise other errors - this is a best-effort operation
    
    def get_offline_status(self) -> Dict[str, Any]:
        """
        Get offline mode status and any HTTP fetch attempts for debugging.
        
        Returns:
            Dictionary with offline mode status and violation information
        """
        return {
            "offline_mode": self._offline_mode,
            "http_fetch_attempts": self._http_fetch_attempts.copy(),
            "catalog_mappings_count": len(self._catalog_map),
            "packages_loaded": len(self._loaded_package_paths),
            "available_catalogs": self._registered_catalogs.copy(),
            "catalog_validation_results": self._catalog_validation_results.copy(),
            "catalog_verification_results": self._catalog_verification_results.copy()
        }
    
    def load_instance(self, file_path: str, dts_first_schemas: Optional[List[str]] = None, skip_val_injection: bool = False) -> Tuple[Optional[Any], int]:
        """
        Load XBRL instance and return ModelXbrl with facts count.
        
        Args:
            file_path: Path to XBRL instance file
            dts_first_schemas: Optional list of schema URLs to preload into DTS before instance loading
            skip_val_injection: Flag to skip val injection
            
        Returns:
            Tuple of (ModelXbrl instance, facts_count)
        """
        acquired = False
        try:
            try:
                self._lock.acquire()
                acquired = True
            except Exception:
                pass
            logger.info(f"Loading XBRL instance: {file_path}")
            
            if not self.cntlr:
                raise RuntimeError("ArelleService not initialized")
            
            if not self.packages_loaded:
                logger.warning("Taxonomy packages not loaded - proceeding anyway")
            
            # DTS-first loading: preload schemas and extract concepts before instance loading
            preloaded_dts = None
            if dts_first_schemas:
                logger.info(f"DTS-first loading enabled: preloading {len(dts_first_schemas)} schemas")
                preloaded_dts = self._preload_dts_schemas(dts_first_schemas)
                if not preloaded_dts:
                    logger.error("Failed to preload DTS schemas, falling back to standard loading")
            
            # Check for DTS-first injection (Option 2b)
            actual_file_path = file_path
            temp_file_created = False
            injection_used = False
            injected_urls = []
            temp_fallback_used = False
            
            if not preloaded_dts:
                # Try DTS-first injection first
                injection_result = self._detect_missing_dictionary_namespaces_with_injection(file_path)
                
                if injection_result:
                    schema_urls, injection_used, temp_fallback_used = injection_result
                    
                    if injection_used:
                        logger.info(f"DTS-first injection enabled, processing {len(schema_urls)} schema URLs")
                        
                        # Read original content
                        with open(file_path, 'r', encoding='utf-8') as f:
                            original_content = f.read()
                        
                        # Create in-memory injection
                        injected_content = self._create_in_memory_injection(original_content, schema_urls)
                        
                        if injected_content:
                            try:
                                # Write injected content to a temp file and use it
                                import uuid
                                from pathlib import Path
                                temp_dir = Path(__file__).resolve().parents[2] / "temp"
                                temp_dir.mkdir(exist_ok=True)
                                original_name = Path(file_path).stem
                                temp_filename = f"{original_name}_injected_{uuid.uuid4().hex[:8]}.xbrl"
                                temp_path = temp_dir / temp_filename
                                with open(temp_path, 'w', encoding='utf-8') as tf:
                                    tf.write(injected_content)
                                actual_file_path = str(temp_path)
                                temp_file_created = True
                                temp_fallback_used = True
                                injected_urls = schema_urls
                                logger.info(f"Wrote injected instance to temp file: {temp_path}")
                            except Exception as e:
                                logger.warning(f"Failed to write injected content to temp file: {e}")
                
                # Fallback to legacy instance rewrite if DTS-first injection not used
                elif self._config.get('allow_instance_rewrite', (self._config.get('flags', {}) or {}).get('allow_instance_rewrite', False)):
                    logger.info("Legacy instance rewrite enabled, checking for missing dictionary schemas")
                    
                    # Detect missing dictionary namespaces
                    missing_schemas = self._detect_missing_dictionary_namespaces(file_path)
                    logger.info(f"Detected {len(missing_schemas)} missing dictionary schemas: {missing_schemas}")
                    
                    if missing_schemas:
                        logger.info(f"Legacy instance rewrite enabled, detected {len(missing_schemas)} missing dictionary schemas")
                        
                        # Create temp copy with injected schemaRefs
                        temp_path = self._create_temp_instance_with_schema_refs(file_path, missing_schemas)
                        if temp_path:
                            actual_file_path = temp_path
                            temp_file_created = True
                            temp_fallback_used = True
                            injected_urls = missing_schemas
                            logger.info(f"Using temp instance with injected schemaRefs: {temp_path}")
                        else:
                            logger.warning("Failed to create temp instance, using original file")
                    else:
                        logger.info("No missing dictionary schemas detected, using original file")
            
            # Load XBRL instance using FileSource to enable package mappings
            from arelle import FileSource
            file_source = FileSource.openFileSource(actual_file_path, self.cntlr)
            
            # Load the instance model, ensuring taxonomy packages are registered for this load
            try:
                taxonomy_pkgs = getattr(self, '_loaded_package_paths', []) or None
            except Exception:
                taxonomy_pkgs = None
            model_xbrl = self.model_manager.load(file_source, taxonomyPackages=taxonomy_pkgs)

            # Optionally inject validation schemaRef for COREP modules if discovery missed val docs
            try:
                features = (self._config or {}).get("features", {}) or {}
                if features.get("conditional_val_schema_injection", False) and not skip_val_injection:
                    # Heuristic: if entrypoint belongs to a known module and no val docs present, inject that module's val xsd
                    file_lower = str(file_path).lower()
                    # Simple module registry: module key -> list of validation URLs
                    module_registry: Dict[str, List[str]] = {
                        "corep_lr": [
                            "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/val/corep_lr-val.xsd"
                        ],
                        "corep_of": [
                            "http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/corep/4.0/val/corep_of-val.xsd"
                        ],
                    }
                    # Detect module from filename
                    is_corep_lr = "corep" in file_lower and ("lr" in file_lower or "coreplr" in file_lower)
                    is_corep_of = "corep" in file_lower and ("of" in file_lower or "corepof" in file_lower)
                    has_val_docs = False
                    try:
                        if hasattr(self.model_manager, 'urlDocs'):
                            for uri in getattr(self.model_manager, 'urlDocs', {}).keys():
                                if "/val/" in uri or uri.endswith("-val.xsd"):
                                    has_val_docs = True
                                    break
                    except Exception:
                        pass
                    # Choose module
                    selected_key = None
                    if is_corep_lr:
                        selected_key = "corep_lr"
                    elif is_corep_of:
                        selected_key = "corep_of"
                    if selected_key and not has_val_docs:
                        logger.info(f"Conditional val schema injection: adding {selected_key} val xsd")
                        try:
                            val_urls = module_registry.get(selected_key, [])
                            if val_urls:
                                temp_val_path = self._create_temp_instance_with_schema_refs(actual_file_path, val_urls)
                                if temp_val_path:
                                    file_source = FileSource.openFileSource(temp_val_path, self.cntlr)
                                    model_xbrl = self.model_manager.load(file_source, taxonomyPackages=taxonomy_pkgs)
                                    temp_file_created = True
                                    actual_file_path = temp_val_path
                                    injected_urls.extend(val_urls)
                                    injection_used = True
                        except Exception as _e:
                            logger.warning(f"Val schema injection failed: {_e}")
            except Exception:
                pass
            
            if model_xbrl and preloaded_dts:
                # DTS-first: merge preloaded concepts into the instance model
                logger.info("Merging preloaded DTS concepts into instance model")
                try:
                    preloaded_concepts = preloaded_dts.get("concepts", {})
                    concepts_merged = 0
                    
                    for qname, concept in preloaded_concepts.items():
                        if qname not in model_xbrl.qnameConcepts:
                            model_xbrl.qnameConcepts[qname] = concept
                            concepts_merged += 1
                    
                    logger.info(f"DTS-first: merged {concepts_merged} preloaded concepts into instance model")
                    logger.info(f"Instance model now has {len(model_xbrl.qnameConcepts)} total concepts")
                    
                    # Verify eba_met concepts are now available
                    eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
                    eba_met_concepts = [
                        concept for qname, concept in model_xbrl.qnameConcepts.items()
                        if qname.namespaceURI == eba_met_ns
                    ]
                    logger.info(f"DTS-first: {len(eba_met_concepts)} eba_met concepts available in instance model")
                    
                except Exception as e:
                    logger.error(f"Error merging preloaded concepts: {e}")
                    logger.info("Continuing with standard instance loading")
            
            if model_xbrl is None:
                logger.error(f"Failed to load XBRL instance: {file_path}")
                return None, 0
            
            # Verify DTS evidence immediately after load
            if injection_used:
                logger.info("Verifying DTS evidence after injection")
                
                # Check if met.xsd is present and eba_met concepts are loaded
                eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
                eba_met_concepts = [
                    concept for qname, concept in model_xbrl.qnameConcepts.items()
                    if qname.namespaceURI == eba_met_ns
                ]
                
                met_xsd_present = len(eba_met_concepts) > 0
                
                if not met_xsd_present:
                    logger.error(f"DTS-first injection failed: met.xsd not present, eba_met_concepts_count = {len(eba_met_concepts)}")
                    # Log probe outcome for debugging
                    for url in injected_urls:
                        probe_result = self.probe_url_resolution(url)
                        logger.error(f"Probe result for {url}: {probe_result}")
                else:
                    logger.info(f"DTS-first injection successful: {len(eba_met_concepts)} eba_met concepts loaded")
            
            # Clean up temp file if created (disabled for debugging)
            if temp_file_created:
                logger.info(f"Temp file preserved for debugging: {actual_file_path}")
                # Store injection metadata for debugging (always set)
                try:
                    model_xbrl._injection_metadata = {
                        "injection_used": injection_used,
                        "injected_urls": injected_urls,
                        "temp_fallback_used": temp_fallback_used,
                        "temp_file_path": actual_file_path
                    }
                except Exception:
                    pass
                # try:
                #     Path(actual_file_path).unlink()
                #     logger.debug(f"Cleaned up temp file: {actual_file_path}")
                # except Exception as e:
                #     logger.warning(f"Failed to clean up temp file {actual_file_path}: {e}")
            
            # Note: Dictionary schemas are now loaded via temp file injection before instance loading
            # This ensures concepts are available during fact discovery
            
            # Final offline mode violation check after all loading operations
            self._check_offline_violations()
            
            # Count facts in the instance (use factsInInstance for all facts, including nested in tuples)
            facts_count = len(getattr(model_xbrl, "factsInInstance", [])) or len(getattr(model_xbrl, "facts", []))
            
            # Also log undefined facts for debugging
            undefined_facts = len(getattr(model_xbrl, "undefinedFacts", []))
            logger.info(f"Loaded instance with {facts_count} facts ({undefined_facts} undefined)")
            
            # Log contexts and units for debugging
            contexts_count = len(getattr(model_xbrl, "contexts", {}))
            units_count = len(getattr(model_xbrl, "units", {}))
            logger.info(f"Loaded {contexts_count} contexts and {units_count} units")
            
            # Log two mapped URL evidences as specified in development plan
            if hasattr(model_xbrl, 'modelDocument') and hasattr(model_xbrl.modelDocument, 'schemaLocationElements'):
                schema_locations = model_xbrl.modelDocument.schemaLocationElements
                if schema_locations:
                    # Convert to list if it's a set or other iterable
                    schema_list = list(schema_locations) if hasattr(schema_locations, '__iter__') else []
                    logger.info(f"URL mapping evidence 1: {schema_list[0] if len(schema_list) > 0 else 'none'}")
                    logger.info(f"URL mapping evidence 2: {schema_list[1] if len(schema_list) > 1 else 'none'}")

            # Return loaded model and facts count; do not build validation results here
            return model_xbrl, facts_count
            
        except Exception as e:
            logger.error(f"Failed to load instance {file_path}: {e}")
            raise
        finally:
            if acquired:
                try:
                    self._lock.release()
                except Exception:
                    pass
    
    def validate_instance(self, model_xbrl: Any, profile: str = "fast") -> Dict[str, Any]:
        """
        Validate XBRL instance with specified profile.
        
        Args:
            model_xbrl: Loaded ModelXbrl instance
            profile: Validation profile (fast/full/debug)
            
        Returns:
            Validation results dictionary
        """
        acquired = False
        try:
            try:
                self._lock.acquire()
                acquired = True
            except Exception:
                pass
            import time
            start_time = time.time()
            
            logger.info(f"Validating instance with profile: {profile}")
            
            if not model_xbrl:
                raise ValueError("No ModelXbrl instance provided")
            
            # Configure validation based on profile
            # Profile configurations from development plan:
            # fast: no formulas, no CSV constraints, no trace
            # full: formulas + CSV constraints, no trace  
            # debug: formulas + CSV constraints + trace
            
            validate_formulas = profile in ["full", "debug"]
            validate_csv_constraints = profile in ["full", "debug"]
            enable_trace = profile == "debug"
            
            logger.info(f"Profile settings - formulas: {validate_formulas}, csv: {validate_csv_constraints}, trace: {enable_trace}")
            
            # Configure formula execution per profile
            try:
                fo = getattr(model_xbrl.modelManager, "formulaOptions", None)
                if fo is None:
                    from arelle.ModelFormulaObject import FormulaOptions as _FormulaOptions
                    fo = _FormulaOptions()
                    model_xbrl.modelManager.formulaOptions = fo
                # map profile to formulaAction
                fo.formulaAction = "run" if validate_formulas else "none"
            except Exception as e:
                logger.warning(f"Could not configure formula options: {e}")

            # Step 2: If formulas enabled but no formula docs discovered, explicitly load VR linkbases (dev-only)
            try:
                features = (self._config or {}).get("features", {}) or {}
                if validate_formulas and features.get("conditional_val_schema_injection", False):
                    if not self._has_formula_docs(model_xbrl):
                        prefixes = self._detect_val_prefixes_from_dts(model_xbrl)
                        loaded_any: List[str] = []
                        for prefix in prefixes:
                            loaded_any.extend(self._explicitly_load_vr_docs_for_prefix(model_xbrl, prefix))
                # Under fast profile, ensure no formula VR fallback loads
                if profile == "fast":
                    try:
                        fo = getattr(model_xbrl.modelManager, "formulaOptions", None)
                        if fo is not None:
                            fo.formulaAction = "none"
                    except Exception:
                        pass
                        if loaded_any:
                            logger.info(f"Explicit VR fallback loaded {len(loaded_any)} documents")
                            try:
                                # Expose on model for evidence logging
                                model_xbrl._vr_fallback_loaded_files = loaded_any
                            except Exception:
                                pass
                        else:
                            logger.info("Explicit VR fallback found no documents to load")
            except Exception as _e:
                logger.debug(f"VR fallback step skipped due to error: {_e}")

            # Perform validation using Arelle with better error handling
            try:
                from arelle import Validate
                # Always load custom transforms immediately before validation per plan
                try:
                    model_xbrl.modelManager.loadCustomTransforms()
                except Exception as e:
                    logger.warning(f"Could not load custom transforms before validation: {e}")
                Validate.validate(model_xbrl)
                logger.info("Arelle validation completed successfully")
            except Exception as validation_error:
                logger.error(f"Arelle validation failed: {validation_error}")
                # Continue to collect results even if validation failed
                pass
            
            # Collect validation results
            errors = []
            warnings = []

            # First, harvest Arelle's buffered log entries if available
            try:
                from arelle.logging.handlers.LogToBufferHandler import LogToBufferHandler
                log_handler = getattr(self.cntlr, 'logHandler', None)
                if isinstance(log_handler, LogToBufferHandler):
                    # Get JSON and clear buffer
                    buffer_json = log_handler.getJson(clearLogBuffer=True)
                    try:
                        parsed = json.loads(buffer_json)
                        log_entries = parsed.get("log", []) if isinstance(parsed, dict) else []
                        for entry in log_entries:
                            level = (entry.get('level') or '').lower()
                            message_text = (entry.get('message') or {}).get('text') if isinstance(entry.get('message'), dict) else entry.get('message')
                            code = entry.get('code') or 'arelle'
                            refs = entry.get('refs') or []
                            if level in ('error', 'critical', 'fatal'):
                                errors.append({"code": code, "message": message_text or '', "severity": "error", "refs": refs})
                            elif level in ('warning', 'warn'):
                                warnings.append({"code": code, "message": message_text or '', "severity": "warning", "refs": refs})
                    except Exception:
                        pass
            except Exception:
                # If buffer handler not present or any issue, continue silently
                pass
            
            # Handle errors - check if it's iterable; append to what we collected from buffer
            if hasattr(model_xbrl, 'errors') and model_xbrl.errors:
                try:
                    for error in model_xbrl.errors:
                        errors.append({
                            "code": getattr(error, 'messageCode', 'unknown'),
                            "message": str(error),
                            "severity": "error"
                        })
                except (TypeError, AttributeError) as e:
                    logger.warning(f"Could not iterate over errors: {e}")
                    errors.append({
                        "code": "error_iteration_failed",
                        "message": f"Could not process errors: {e}",
                        "severity": "error"
                    })
            
            # Handle warnings - check if it's iterable
            if hasattr(model_xbrl, 'warnings') and model_xbrl.warnings:
                try:
                    for warning in model_xbrl.warnings:
                        warnings.append({
                            "code": getattr(warning, 'messageCode', 'unknown'),
                            "message": str(warning),
                            "severity": "warning"
                        })
                except (TypeError, AttributeError) as e:
                    logger.warning(f"Could not iterate over warnings: {e}")
                    warnings.append({
                        "code": "warning_iteration_failed",
                        "message": f"Could not process warnings: {e}",
                        "severity": "warning"
                    })
            
            # Get facts count - use factsInInstance for all facts including nested
            facts_count = 0
            if hasattr(model_xbrl, 'factsInInstance'):
                facts_count = len(model_xbrl.factsInInstance)
            elif hasattr(model_xbrl, 'facts'):
                facts_count = len(model_xbrl.facts)

            # If fast profile: remove formula-derived messages to enforce divergence
            if profile == "fast":
                try:
                    def _is_formula_entry(entry: Dict[str, Any]) -> bool:
                        code = str(entry.get("code", ""))
                        msg = str(entry.get("message", ""))
                        return code.startswith("message:v") or ("/val/" in msg or "vr-" in msg)
                    errors = [e for e in errors if not _is_formula_entry(e)]
                    warnings = [w for w in warnings if not _is_formula_entry(w)]
                except Exception:
                    pass
            
            # Add DTS evidence logging and enhanced metrics
            dts_evidence = self._log_dts_evidence(model_xbrl)
            enhanced_metrics = self._collect_enhanced_metrics(model_xbrl)
            # Preflight (balanced), before full validation results aggregation
            try:
                from app.services.filing_rules import run_preflight
                preflight = run_preflight(model_xbrl, {"offline_status": self.get_offline_status()})
                enhanced_metrics["filing_rules_preflight"] = preflight
                # Update category counts
                try:
                    cat = enhanced_metrics.get("category_counts") or {}
                    cat["filing_rules_preflight"] = preflight.get("failed", 0)
                    enhanced_metrics["category_counts"] = cat
                except Exception:
                    pass
            except Exception as _e:
                logger.warning(f"Preflight failed: {_e}")
            # Add convenience counts based on evidence
            try:
                val_urls = dts_evidence.get("val_doc_urls", []) if isinstance(dts_evidence, dict) else []
                enhanced_metrics["val_urls_count"] = len(val_urls)
            except Exception:
                pass
            # Derive category counts
            try:
                category_counts = self._classify_and_count_categories(errors + warnings)
                enhanced_metrics["category_counts"] = category_counts
            except Exception as _e:
                logger.debug(f"Category classification skipped: {_e}")
            # Normalize/minimize unusable entries and ensure minimum fields
            try:
                import re as _re_min
                # Feature flags controlling non-actionable handling
                _features = (self._config or {}).get("features", {}) or {}
                _drop_nonactionable: bool = bool(_features.get("drop_nonactionable", True))
                _sample_limit: int = int(_features.get("drop_nonactionable_sample_limit", 5))
                _dropped_count_errors = 0
                _dropped_count_warnings = 0
                _dropped_samples: List[Dict[str, Any]] = []

                def _snapshot(e: Dict[str, Any]) -> Dict[str, Any]:
                    return {
                        "code": e.get("code"),
                        "message": e.get("message"),
                        "severity": e.get("severity"),
                        "refs": e.get("refs"),
                        "table_id": e.get("table_id"),
                        "rowCode": e.get("rowCode"),
                        "colCode": e.get("colCode"),
                    }

                def _scrub(entries: List[Dict[str, Any]], is_errors: bool) -> List[Dict[str, Any]]:
                    cleaned: List[Dict[str, Any]] = []
                    for e in entries:
                        msg = (e.get("message") or "").strip()
                        code_val = str(e.get("code", ""))
                        # If message is blank, try to backfill from code
                        if not msg:
                            if code_val:
                                e["message"] = code_val
                            else:
                                e["message"] = "unknown"
                        # Extract rule_id from token if present
                        if not e.get("rule_id"):
                            m = _re_min.search(r"message:([A-Za-z0-9_\-.]+)", e.get("message", ""))
                            if m:
                                e["rule_id"] = m.group(1)
                                if e["rule_id"].startswith("v") and not e.get("category"):
                                    e["category"] = "formulas"
                        # Drop entries that remain completely non-actionable
                        if (str(e.get("message", "")).strip() == ""
                            and (str(e.get("code", "unknown")) == "unknown")
                            and not e.get("refs")
                            and not any(e.get(k) for k in ("table_id", "rowCode", "colCode", "conceptNs", "conceptLn"))):
                            if _drop_nonactionable:
                                if len(_dropped_samples) < _sample_limit:
                                    _dropped_samples.append(_snapshot(e))
                                if is_errors:
                                    _dropped_count_errors += 1
                                else:
                                    _dropped_count_warnings += 1
                                continue
                            else:
                                # Keep but mark as nonactionable for transparency
                                e["nonactionable"] = True
                        cleaned.append(e)
                    return cleaned
                errors = _scrub(errors, True)
                warnings = _scrub(warnings, False)
                # Record metrics about dropped entries (if any)
                try:
                    total_dropped = _dropped_count_errors + _dropped_count_warnings
                    if total_dropped > 0:
                        enhanced_metrics["dropped_nonactionable_count"] = (
                            enhanced_metrics.get("dropped_nonactionable_count", 0) + total_dropped
                        )
                        enhanced_metrics["dropped_nonactionable_breakdown"] = {
                            "errors": _dropped_count_errors,
                            "warnings": _dropped_count_warnings,
                        }
                        enhanced_metrics["dropped_nonactionable_samples"] = _dropped_samples
                except Exception:
                    pass
            except Exception:
                pass
            # Enrich messages with concept coordinates (best-effort)
            try:
                self._enrich_entries_with_concept_coords(model_xbrl, errors)
                self._enrich_entries_with_concept_coords(model_xbrl, warnings)
            except Exception as _e:
                logger.debug(f"Concept enrichment skipped: {_e}")
            # Parse v-code style cell references and attach table/row/col codes
            try:
                self._enrich_entries_with_vcode_coords(errors)
                self._enrich_entries_with_vcode_coords(warnings)
            except Exception as _e:
                logger.debug(f"v-code enrichment skipped: {_e}")
            # Summarize top error/warning codes into metrics.top_error_codes (default N=10)
            try:
                top_n = 10
                code_counts: List[Dict[str, Any]] = []
                # Count errors
                err_counts: Dict[str, int] = {}
                for e in errors:
                    code_val = str(e.get("code", "unknown"))
                    err_counts[code_val] = err_counts.get(code_val, 0) + 1
                # Count warnings
                warn_counts: Dict[str, int] = {}
                for w in warnings:
                    code_val = str(w.get("code", "unknown"))
                    warn_counts[code_val] = warn_counts.get(code_val, 0) + 1
                # Build combined list with severity
                for code_val, cnt in err_counts.items():
                    code_counts.append({"code": code_val, "severity": "error", "count": cnt})
                for code_val, cnt in warn_counts.items():
                    code_counts.append({"code": code_val, "severity": "warning", "count": cnt})
                # Prefer v-codes first; stable sort by count desc then code
                def sort_key(item: Dict[str, Any]):
                    code_s = item.get("code", "")
                    is_v = 1 if str(code_s).startswith("message:v") else 0
                    return (-is_v, -int(item.get("count", 0)), str(code_s))
                code_counts_sorted = sorted(code_counts, key=sort_key)
                # If empty, attempt to extract v-codes from message text when code=='unknown'
                if not code_counts_sorted:
                    import re
                    vpat = re.compile(r"\bmessage:(v\d+_[a-z]_?\d*)\b", re.IGNORECASE)
                    txt_counts: Dict[str, int] = {}
                    txt_sev: Dict[str, str] = {}
                    for entry in errors + warnings:
                        msg = str(entry.get("message", ""))
                        m = vpat.search(msg)
                        if m:
                            code_val = f"message:{m.group(1)}"
                            txt_counts[code_val] = txt_counts.get(code_val, 0) + 1
                            if code_val not in txt_sev:
                                txt_sev[code_val] = entry.get("severity", "error")
                    fallback = [
                        {"code": c, "severity": txt_sev.get(c, "error"), "count": cnt}
                        for c, cnt in txt_counts.items()
                    ]
                    fallback_sorted = sorted(fallback, key=lambda i: (-int(i.get("count", 0)), str(i.get("code",""))))
                    code_counts_sorted = fallback_sorted
                enhanced_metrics["top_error_codes"] = code_counts_sorted[:top_n]
            except Exception as _e:
                logger.warning(f"Failed to compute top_error_codes: {_e}")
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            results = {
                "status": "success" if not errors else "failed",
                "errors": errors,
                "warnings": warnings,
                "facts_count": facts_count,
                "duration_ms": duration_ms,
                "profile": profile,
                "dts_evidence": dts_evidence,
                "metrics": enhanced_metrics
            }
            # Attach offline attempt metrics and provenance
            try:
                offline = self.get_offline_status()
                results.setdefault("metrics", {})["offline_attempt_count"] = len(offline.get("http_fetch_attempts", []))
                if offline.get("http_fetch_attempts"):
                    results["metrics"]["offline_attempted_urls"] = offline.get("http_fetch_attempts")
            except Exception:
                pass
            
            # Persist categorized validation logs per run (formulas vs xbrl2_1)
            try:
                self._write_validation_logs(model_xbrl, results)
            except Exception as _e:
                logger.warning(f"Failed to write validation logs: {_e}")
            
            logger.info(f"Validation completed - errors: {len(errors)}, warnings: {len(warnings)}, duration: {duration_ms}ms")
            return results
            
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
            logger.error(f"Validation failed: {e}")
            return {
                "status": "error",
                "errors": [{"code": "validation_error", "message": str(e), "severity": "error"}],
                "warnings": [],
                "facts_count": 0,
                "duration_ms": duration_ms,
                "profile": profile,
                "dts_evidence": {},
                "metrics": {}
            }
        finally:
            if acquired:
                try:
                    self._lock.release()
                except Exception:
                    pass

    def _enrich_entries_with_concept_coords(self, model_xbrl: Any, entries: List[Dict[str, Any]]) -> None:
        """
        Best-effort extraction of concept namespace/localName (and maybe contextRef) from message text.
        If a QName like prefix:local is present (e.g., eba_met:ei4), we resolve the local name by
        scanning model_xbrl.qnameConcepts for a matching localName, preferring EBA MET namespaces.
        """
        try:
            import re
            qname_pat = re.compile(r"\b([A-Za-z_][\w\-.]*):([A-Za-z_][\w\-.]*)\b")
            # Build quick lookup from localName -> list of namespaces
            local_to_ns: Dict[str, List[str]] = {}
            if hasattr(model_xbrl, 'qnameConcepts'):
                for qn, _c in model_xbrl.qnameConcepts.items():
                    try:
                        ln = getattr(qn, 'localName', None)
                        ns = getattr(qn, 'namespaceURI', None)
                        if ln and ns:
                            local_to_ns.setdefault(ln, []).append(ns)
                    except Exception:
                        continue
            def pick_ns(local_name: str) -> str:
                nss = local_to_ns.get(local_name, [])
                if not nss:
                    return ''
                # Prefer EBA MET namespaces
                for ns in nss:
                    if 'eba' in ns and 'met' in ns:
                        return ns
                return nss[0]
            for e in entries:
                msg = str(e.get('message', '') or '')
                if 'conceptLn' in e and e['conceptLn']:
                    continue
                m = qname_pat.search(msg)
                if not m:
                    continue
                _prefix, local = m.group(1), m.group(2)
                ns = pick_ns(local)
                if local and ns:
                    e['conceptLn'] = local
                    e['conceptNs'] = ns
        except Exception:
            pass

    def _enrich_entries_with_vcode_coords(self, entries: List[Dict[str, Any]]) -> None:
        """
        Parse v-code style cell references such as:
          {C_43.00.c,0250,0020,}
        and attach table_id, rowCode, colCode to message entries.

        - TableId pattern: C_\d{2}\.\d{2}(?:\.[a-z])?
        - row/col codes: 2-5 digits; normalize by stripping leading zeros
        - If multiple references are present, use the first occurrence
        """
        try:
            import re
            # capture {TableId,rowCode,colCode, optional...}
            pat = re.compile(r"\{\s*(C_\d{2}\.\d{2}(?:\.[a-z])?)\s*,\s*(\d{2,5})\s*,\s*(\d{2,5})\s*,?[^}]*\}")
            for e in entries:
                msg = str(e.get('message', '') or '')
                m = pat.search(msg)
                if not m:
                    continue
                table_id, r, c = m.group(1), m.group(2), m.group(3)
                # Normalize codes as strings without leading zeros, but keep original if stripping empties
                def norm(s: str) -> str:
                    s2 = s.lstrip('0')
                    return s2 if s2 != '' else s
                e['table_id'] = table_id
                e['rowCode'] = norm(r)
                e['colCode'] = norm(c)
                # Extract canonical rule_id from text (strip 'message:')
                rid = None
                m2 = re.search(r"message:(v[0-9]+_[a-z]_[0-9]+)", msg, re.IGNORECASE)
                if m2:
                    rid = m2.group(1)
                if rid:
                    e['rule_id'] = rid
                    # Set categories for v-codes
                    e['category'] = 'formulas'
                # Build readable_message if labels/qualifiers are present later during mapping join
        except Exception:
            pass

    def _classify_and_count_categories(self, entries: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Classify messages into categories and return counts per category.
        Categories: xbrl21, dimensions, calculation, formulas, eba_filing.
        """
        counts = {
            "xbrl21": 0,
            "dimensions": 0,
            "calculation": 0,
            "formulas": 0,
            "eba_filing": 0,
        }
        # Known code prefixes
        dim_code_prefixes = ("xbrldte:", "xbrldi:")
        calc_code_tokens = ("calc",)  # code often contains 'calc' for calculation-related plugins
        # Message token sets (lowercased)
        dim_tokens = ("hypercube", "axis", "member", "dimension", "xbrldi", "xbrl-dim")
        calc_tokens = ("calculation", "summation-item", "summation item", "weight", "sum(")
        for e in entries:
            code = str(e.get("code", "") or "").lower()
            msg = str(e.get("message", "") or "").lower()
            refs = e.get("refs", []) or []
            refs_s = " ".join([str(r) for r in refs]).lower() if refs else ""
            # Formulas / Filing Rules (EBA v-codes)
            is_formula = code.startswith("message:v") or ("/val/" in msg or "vr-" in msg) or "formula:" in code
            if is_formula:
                counts["formulas"] += 1
                counts["eba_filing"] += 1  # EBA v-codes are Filing Rules
                continue
            # Heuristics for dimensions
            if code.startswith(dim_code_prefixes) \
               or any(token in msg for token in dim_tokens) \
               or ("/def/" in refs_s and any(token in msg for token in dim_tokens)) \
               or ("xbrl-dimensions" in msg):
                counts["dimensions"] += 1
                continue
            # Heuristics for calculation
            if any(tok in code for tok in calc_code_tokens) \
               or any(token in msg for token in calc_tokens) \
               or ("/cal/" in refs_s) \
               or ("calculationlink" in msg):
                counts["calculation"] += 1
                continue
            # Default to XBRL 2.1 core
            counts["xbrl21"] += 1
        return counts
    
    def _log_dts_evidence(self, model_xbrl: Any) -> Dict[str, Any]:
        """
        Log DTS evidence including presence of met.xsd and concept counts.
        
        Args:
            model_xbrl: Loaded ModelXbrl instance
            
        Returns:
            Dictionary with DTS evidence information
        """
        try:
            evidence = {
                "met_xsd_present": False,
                "eba_met_concepts_count": 0,
                "total_concepts": 0,
                "dts_documents": [],
                "namespace_usage": {},
                "formula_docs_count": 0,
                "val_doc_urls": []
            }
            
            # Check for met.xsd using multiple sources for robustness
            try:
                # 1) modelDocument.referencedDocumentNames (may be sparse)
                if hasattr(model_xbrl, 'modelDocument') and model_xbrl.modelDocument:
                    dts = model_xbrl.modelDocument
                    if hasattr(dts, 'referencedDocumentNames'):
                        for doc_name in dts.referencedDocumentNames:
                            evidence["dts_documents"].append(str(doc_name))
                            if 'met.xsd' in str(doc_name):
                                evidence["met_xsd_present"] = True
                # 2) modelManager.urlDocs (actual loaded docs)
                if hasattr(model_xbrl, 'modelManager') and hasattr(model_xbrl.modelManager, 'urlDocs'):
                    for url in getattr(model_xbrl.modelManager, 'urlDocs', {}).keys():
                        evidence["dts_documents"].append(str(url))
                        if 'met.xsd' in str(url):
                            evidence["met_xsd_present"] = True
            except Exception as e:
                logger.debug(f"DTS evidence collection issue: {e}")
            
            # Count eba_met concepts
            if hasattr(model_xbrl, 'qnameConcepts'):
                eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
                eba_met_concepts = [
                    concept for qname, concept in model_xbrl.qnameConcepts.items()
                    if qname.namespaceURI == eba_met_ns
                ]
                evidence["eba_met_concepts_count"] = len(eba_met_concepts)
                evidence["total_concepts"] = len(model_xbrl.qnameConcepts)
                # If we have eba_met concepts, mark met.xsd as present
                if eba_met_concepts:
                    evidence["met_xsd_present"] = True
            
            # Analyze namespace usage
            if hasattr(model_xbrl, 'qnameConcepts'):
                namespace_counts = {}
                for qname, concept in model_xbrl.qnameConcepts.items():
                    if hasattr(concept, 'namespaceURI'):
                        ns = concept.namespaceURI
                        namespace_counts[ns] = namespace_counts.get(ns, 0) + 1
                evidence["namespace_usage"] = namespace_counts
                
                # Log key namespaces
                for ns, count in namespace_counts.items():
                    if 'eba' in ns or 'xbrl' in ns:
                        logger.info(f"DTS evidence: {ns} -> {count} concepts")

            # Record formula docs evidence and val doc URLs
            try:
                if hasattr(model_xbrl, 'formulaLinkbaseDocumentObjects') and model_xbrl.formulaLinkbaseDocumentObjects:
                    evidence["formula_docs_count"] = len(model_xbrl.formulaLinkbaseDocumentObjects)
                if hasattr(model_xbrl, 'modelManager') and hasattr(model_xbrl.modelManager, 'urlDocs'):
                    for url in getattr(model_xbrl.modelManager, 'urlDocs', {}).keys():
                        u = str(url)
                        if '/val/' in u or 'vr-' in u:
                            evidence["val_doc_urls"].append(u)
                # Count calc and dimension relationship sets for diagnostics
                try:
                    from arelle import XbrlConst
                    calc_rel = model_xbrl.relationshipSet(tuple(XbrlConst.summationItemSet)) if hasattr(XbrlConst, 'summationItemSet') else model_xbrl.relationshipSet(XbrlConst.summationItem)
                    dim_rel = model_xbrl.relationshipSet(XbrlConst.all)
                    calc_count = len(calc_rel.modelRelationships) if calc_rel is not None else 0
                    # Dimensions relationships are in definition linkbase with specific arcroles; count broadly if available
                    dim_count = 0
                    if dim_rel is not None and hasattr(dim_rel, 'modelRelationships'):
                        for rel in dim_rel.modelRelationships:
                            if 'dimensions' in str(getattr(rel, 'arcrole', '')).lower():
                                dim_count += 1
                    evidence["calc_relationships_count"] = calc_count
                    evidence["dimension_relationships_count"] = dim_count
                except Exception:
                    pass
            except Exception:
                pass
            
            return evidence
            
        except Exception as e:
            logger.warning(f"Failed to collect DTS evidence: {e}")
            return {"error": str(e)}
    
    def _collect_enhanced_metrics(self, model_xbrl: Any) -> Dict[str, Any]:
        """
        Collect enhanced metrics including undefinedFacts, contexts, units.
        
        Args:
            model_xbrl: Loaded ModelXbrl instance
            
        Returns:
            Dictionary with enhanced metrics
        """
        try:
            metrics = {
                "undefined_facts": 0,
                "contexts_count": 0,
                "units_count": 0,
                "facts_by_namespace": {},
                "validation_issues": []
            }
            
            # Count undefined facts
            if hasattr(model_xbrl, 'undefinedFacts'):
                metrics["undefined_facts"] = len(model_xbrl.undefinedFacts)
                logger.info(f"Enhanced metrics: {len(model_xbrl.undefinedFacts)} undefined facts")
            
            # Count contexts
            if hasattr(model_xbrl, 'contexts'):
                metrics["contexts_count"] = len(model_xbrl.contexts)
                logger.info(f"Enhanced metrics: {len(model_xbrl.contexts)} contexts")
            
            # Count units
            if hasattr(model_xbrl, 'units'):
                metrics["units_count"] = len(model_xbrl.units)
                logger.info(f"Enhanced metrics: {len(model_xbrl.units)} units")
            
            # Analyze facts by namespace
            if hasattr(model_xbrl, 'facts'):
                facts_by_ns = {}
                for fact in model_xbrl.facts:
                    if hasattr(fact, 'qname') and hasattr(fact.qname, 'namespaceURI'):
                        ns = fact.qname.namespaceURI
                        facts_by_ns[ns] = facts_by_ns.get(ns, 0) + 1
                metrics["facts_by_namespace"] = facts_by_ns
                
                # Log key namespace usage
                for ns, count in facts_by_ns.items():
                    if 'eba' in ns or count > 10:  # Log EBA namespaces or high-count namespaces
                        logger.info(f"Enhanced metrics: {ns} -> {count} facts")
            
            # Collect validation issues
            if hasattr(model_xbrl, 'errors') and model_xbrl.errors:
                for error in model_xbrl.errors:
                    metrics["validation_issues"].append({
                        "type": "error",
                        "message": str(error),
                        "code": getattr(error, 'messageCode', 'unknown')
                    })
            
            if hasattr(model_xbrl, 'warnings') and model_xbrl.warnings:
                for warning in model_xbrl.warnings:
                    metrics["validation_issues"].append({
                        "type": "warning", 
                        "message": str(warning),
                        "code": getattr(warning, 'messageCode', 'unknown')
                    })
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Failed to collect enhanced metrics: {e}")
            return {"error": str(e)}

    def _write_validation_logs(self, model_xbrl: Any, results: Dict[str, Any]) -> None:
        """
        Write categorized validation logs for the run: XBRL 2.1 structural errors and formula (v-code) messages.
        Creates JSON and text summaries under backend/logs.
        """
        try:
            from pathlib import Path
            import uuid
            logs_dir = Path(__file__).resolve().parents[2] / "logs"
            logs_dir.mkdir(exist_ok=True)
            run_id = results.get("run_id") or uuid.uuid4().hex[:8]

            errors = results.get("errors", [])
            warnings = results.get("warnings", [])
            metrics = results.get("metrics", {}) or {}
            top_codes = metrics.get("top_error_codes", [])

            # Split messages into categories
            def is_formula(entry: Dict[str, Any]) -> bool:
                code = str(entry.get("code", ""))
                msg = str(entry.get("message", ""))
                return code.startswith("message:v") or "vr-" in msg or "/val/" in msg

            xbrl21_errors = [e for e in errors if not is_formula(e)]
            xbrl21_warnings = [w for w in warnings if not is_formula(w)]
            formula_errors = [e for e in errors if is_formula(e)]
            formula_warnings = [w for w in warnings if is_formula(w)]

            # Write JSON files
            files = {
                f"validation_xbrl21_{run_id}.json": {
                    "errors": xbrl21_errors,
                    "warnings": xbrl21_warnings,
                },
                f"validation_formulas_{run_id}.json": {
                    "errors": formula_errors,
                    "warnings": formula_warnings,
                    "top_error_codes": top_codes,
                },
            }
            for name, payload in files.items():
                (logs_dir / name).write_text(json.dumps(payload, indent=2), encoding="utf-8")

            # Write brief text summaries
            def summarize(entries: List[Dict[str, Any]]) -> str:
                return "\n".join(f"[{e.get('code','unknown')}] {e.get('message','')}" for e in entries[:200])

            (logs_dir / f"validation_xbrl21_{run_id}.txt").write_text(
                f"XBRL 2.1\nErrors: {len(xbrl21_errors)}\nWarnings: {len(xbrl21_warnings)}\n\n" + summarize(xbrl21_errors),
                encoding="utf-8",
            )
            (logs_dir / f"validation_formulas_{run_id}.txt").write_text(
                f"Formulas (v-codes)\nErrors: {len(formula_errors)}\nWarnings: {len(formula_warnings)}\nTop codes: {top_codes}\n\n" + summarize(formula_errors),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"Could not write validation logs: {e}")
