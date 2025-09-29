"""
Message catalog loader for resolving placeholder keys like "message:v4460_m_0"
into human-readable, localized text using the EBA Severity package.

Operates fully offline by reading the already-configured Severity package zip.
"""

from __future__ import annotations

import re
import zipfile
from typing import Dict, Optional
from xml.etree import ElementTree as ET


class MessageCatalog:
    def __init__(self, lang: str = "en") -> None:
        self.lang = (lang or "en").lower()
        self._messages: Dict[str, str] = {}

    @staticmethod
    def _text(el: Optional[ET.Element]) -> str:
        if el is None:
            return ""
        try:
            return "".join(el.itertext()).strip()
        except Exception:
            return (el.text or "").strip()

    def load_from_severity_zip(self, severity_zip_path: str) -> None:
        try:
            with zipfile.ZipFile(severity_zip_path) as zf:
                for name in zf.namelist():
                    # Heuristic: only parse XMLs that may contain messages/catalog entries
                    if not name.lower().endswith('.xml'):
                        continue
                    # Scan more broadly: severity package often stores messages under val/vr*.xml
                    low = name.lower()
                    if not ("message" in low or "catalog" in low or "/val/" in low or low.endswith('.xml')):
                        continue
                    try:
                        with zf.open(name) as fh:
                            tree = ET.parse(fh)  # nosec - reading trusted offline package
                            root = tree.getroot()
                            # Collect any element with an id attribute; prefer lang match
                            for el in root.findall('.//*[@id]'):
                                mid = el.get('id') or ''
                                if not mid:
                                    continue
                                lang_attr = (el.get('{http://www.w3.org/XML/1998/namespace}lang') or '').lower()
                                if lang_attr and (lang_attr not in (self.lang, self.lang.split('-')[0])):
                                    continue
                                txt = self._text(el)
                                if txt:
                                    # first one wins to avoid overriding specific language variants
                                    self._messages.setdefault(mid, txt)
                    except Exception:
                        # Best-effort parsing; skip malformed files silently
                        continue
        except Exception:
            # If the zip cannot be opened, leave the catalog empty
            pass

    def bulk_load_from_zip_globs(self, zip_globs: list[str]) -> int:
        """Load all applicable zips matching the provided globs. Returns count of ids loaded."""
        import glob
        before = len(self._messages)
        for pattern in zip_globs or []:
            try:
                for zp in glob.glob(pattern, recursive=True):
                    self.load_from_severity_zip(zp)
            except Exception:
                continue
        return len(self._messages) - before

    def load_from_unpacked_roots(self, roots: list[str]) -> int:
        """Scan unpacked directories for XML files containing message resources.

        Returns count of ids loaded.
        """
        import os
        before = len(self._messages)
        for root in roots or []:
            try:
                for dirpath, _dirnames, filenames in os.walk(root):
                    for fn in filenames:
                        if not fn.lower().endswith('.xml'):
                            continue
                        full = os.path.join(dirpath, fn)
                        try:
                            tree = ET.parse(full)
                            r = tree.getroot()
                            for el in r.findall('.//*[@id]'):
                                mid = el.get('id') or ''
                                if not mid:
                                    continue
                                lang_attr = (el.get('{http://www.w3.org/XML/1998/namespace}lang') or '').lower()
                                if lang_attr and (lang_attr not in (self.lang, self.lang.split('-')[0])):
                                    continue
                                txt = self._text(el)
                                if txt:
                                    self._messages.setdefault(mid, txt)
                        except Exception:
                            continue
            except Exception:
                continue
        return len(self._messages) - before

    def ids_loaded(self) -> int:
        return len(self._messages)

    def resolve(self, key: str, params: Optional[dict] = None) -> Optional[str]:
        if not key:
            return None
        m = re.match(r'^message:(?P<id>[A-Za-z0-9_\-.]+)$', key)
        if not m:
            return None
        msg_id = m.group('id')
        tmpl = self._messages.get(msg_id)
        if not tmpl:
            return None
        try:
            # Basic Python format with provided params (if any)
            return tmpl.format(**(params or {}))
        except Exception:
            return tmpl


