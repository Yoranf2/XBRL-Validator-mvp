"""
Service utilities for rendering EBA tables (Table Linkbase) to HTML using vendored Arelle.

This module provides a minimal, offline-safe entrypoint to generate an HTML
tableset (index + per-table pages) populated with facts from a loaded instance.

Notes:
- Imports of Arelle components are performed inside functions to avoid import-time
  failures if the vendored submodule path is not yet configured at process start.
- This utility does not mutate user uploads or the DTS; it only renders views.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Union, List, Dict
import json


logger = logging.getLogger(__name__)


def render_eba_tableset(
    model_xbrl: Any,
    out_dir: Union[str, Path],
    lang: str = "en",
) -> str:
    """
    Render the EBA tableset for the given loaded ModelXbrl into the specified directory.

    Produces an index (index.html), a forms frame, and per-table HTML files using
    Arelle's built-in EBA tables plugin and rendered grid view. Facts from the provided
    model are placed into the appropriate cells.

    Args:
        model_xbrl: Loaded Arelle ModelXbrl for the instance/DTS to render.
        out_dir: Output directory path where HTML files will be written.
        lang: Label language preference (default "en").

    Returns:
        Absolute path to the generated index.html file.

    Raises:
        ValueError: If model_xbrl is missing or appears uninitialized.
        Exception: Propagates unexpected rendering errors from Arelle.
    """
    if model_xbrl is None or not hasattr(model_xbrl, "modelManager"):
        raise ValueError("render_eba_tableset requires a loaded ModelXbrl with a modelManager")

    output_dir = Path(out_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    index_file = output_dir / "index.html"

    try:
        # Import inside function to avoid module import at process start before sys.path is set.
        from arelle import XbrlConst  # type: ignore
        from arelle.rendering import RenderingEvaluator  # type: ignore
        from arelle.ViewFileRenderedGrid import viewRenderedGrid  # type: ignore
        from arelle.ModelRenderingObject import DefnMdlTable  # type: ignore
        from lxml import etree  # type: ignore
        from arelle.ViewFileRenderedGrid import ViewRenderedGrid  # type: ignore
        from arelle.rendering.RenderingLayout import layoutTable  # type: ignore

        # Ensure custom transforms are loaded (required for XPath functions used by rendering)
        try:
            mm = getattr(model_xbrl, 'modelManager', None)
            if mm and hasattr(mm, 'loadCustomTransforms'):
                mm.loadCustomTransforms()
                logger.info("Loaded custom transforms for rendering")
        except Exception:
            logger.exception("Failed to load custom transforms; proceeding may fail")

        # Compile and initialize table rendering for the model.
        logger.info("Rendering init: starting RenderingEvaluator.init(...) with facts=%d", len(getattr(model_xbrl, 'factsInInstance', []) or []))
        RenderingEvaluator.init(model_xbrl)
        logger.info("Rendering init: completed; modelRenderingTables=%d", len(getattr(model_xbrl, 'modelRenderingTables', []) or []))

        # Collect tables via euGroupTable relationships; fallback to modelRenderingTables
        groupTableRels = model_xbrl.relationshipSet(XbrlConst.euGroupTable)
        model_tables = []  # list of DefnMdlTable in traversal order

        def _collect_tables(model_obj):
            for rel in groupTableRels.fromModelObject(model_obj):
                to_obj = rel.toModelObject
                if isinstance(to_obj, DefnMdlTable):
                    model_tables.append(to_obj)
                # continue traversal (tables may lead to more tables)
                _collect_tables(to_obj)

        # roots are concepts; walk from each to collect tables
        try:
            roots = list(groupTableRels.rootConcepts)
        except Exception:
            roots = []
        for root_concept in roots:
            _collect_tables(root_concept)

        if not model_tables:
            # Fallback: use compiled tables directly
            for t in getattr(model_xbrl, 'modelRenderingTables', []) or []:
                if isinstance(t, DefnMdlTable):
                    model_tables.append(t)

        # Minimal index page and per-table rendering
        index_doc = etree.Element("html")
        head = etree.SubElement(index_doc, "head")
        title = etree.SubElement(head, "title"); title.text = "EBA Tableset"
        style = etree.SubElement(head, "style"); style.text = "body{font-family:Arial,sans-serif;margin:16px;} ul{padding-left:16px;} li{margin:6px 0;} a{text-decoration:none;color:#243e5e;} a:hover{text-decoration:underline;}"
        body = etree.SubElement(index_doc, "body")
        h1 = etree.SubElement(body, "h1"); h1.text = "EBA Tableset"
        p = etree.SubElement(body, "p"); p.text = "Select a table to view:"
        ul = etree.SubElement(body, "ul")

        num_files = 0
        tables_manifest: List[Dict[str, Any]] = []
        def _write_mapping(table_name: str, table_id: str, table_label: str) -> None:
            """Build a sidecar mapping JSON for the given table and write alongside HTML."""
            try:
                # Build layout model for this specific table
                dummy_out = output_dir / "_void.html"
                view = ViewRenderedGrid(model_xbrl, str(dummy_out), lang, cssExtras="")
                layoutTable(view, table_name)
                lyt = view.lytMdlTblMdl
                cells: List[Dict[str, Any]] = []
                row_labels: List[str] = []
                col_labels: List[str] = []
                row_codes: List[str] = []
                col_codes: List[str] = []
                # Display codes as printed in table headers (e.g., 0010, 0030, 0230)
                display_row_codes: List[str] = []
                display_col_codes: List[str] = []
                def _extract_code_from_obj(obj: Any, fallback_text: str) -> str:
                    try:
                        import re
                        # Try attributes likely to carry identifiers
                        for attr in ("xlinkLabel", "id", "code", "name"):
                            if hasattr(obj, attr):
                                val = str(getattr(obj, attr) or "")
                                m = re.search(r"(\d{2,5})", val)
                                if m:
                                    s2 = m.group(1).lstrip('0')
                                    return s2 if s2 else m.group(1)
                        # Fallback to numeric token in label text
                        m2 = re.search(r"(\d{2,5})", fallback_text or "")
                        if m2:
                            s3 = m2.group(1).lstrip('0')
                            return s3 if s3 else m2.group(1)
                    except Exception:
                        pass
                    return ""
                # Traverse similar to ViewRenderedGrid.view(...)
                for lytMdlTableSet in getattr(lyt, 'lytMdlTableSets', []) or []:
                    for lytMdlTable in getattr(lytMdlTableSet, 'lytMdlTables', []) or []:
                        # Build Y-axis row labels
                        try:
                            lytMdlYHdrs = lytMdlTable.lytMdlAxisHeaders("y")
                            numYrows = lytMdlTable.numBodyCells("y") or 0
                            yRowHdrs = [[] for _ in range(numYrows)]
                            row_codes = [""] * numYrows
                            for lytMdlYGrp in (getattr(lytMdlYHdrs, 'lytMdlGroups', []) or []):
                                for lytMdlYHdr in lytMdlYGrp.lytMdlHeaders:
                                    if all(lytMdlCell.isOpenAspectEntrySurrogate for lytMdlCell in lytMdlYHdr.lytMdlCells):
                                        continue
                                    yRow = 0
                                    for lytMdlYCell in lytMdlYHdr.lytMdlCells:
                                        if getattr(lytMdlYCell, 'isOpenAspectEntrySurrogate', False):
                                            continue
                                        # take first label variant
                                        label_text = lytMdlYCell.labelXmlText(0, "\u00a0") if hasattr(lytMdlYCell, 'labelXmlText') else ''
                                        for _ in range(getattr(lytMdlYCell, 'span', 1) or 1):
                                            if yRow < len(yRowHdrs):
                                                yRowHdrs[yRow].append(label_text)
                                                # Attempt to derive a stable row code from structural node/label
                                                if not row_codes[yRow]:
                                                    row_codes[yRow] = _extract_code_from_obj(lytMdlYCell, label_text)
                                            yRow += 1
                            row_labels = [" / ".join([t for t in parts if t]) for parts in yRowHdrs]
                        except Exception:
                            row_labels = []
                            row_codes = []
                        # Build X-axis column labels (best-effort, top headers only)
                        try:
                            lytMdlXHdrs = lytMdlTable.lytMdlAxisHeaders("x")
                            # Determine number of columns from first body row
                            z_body = lytMdlTable.lytMdlBodyChildren[0]
                            first_y = z_body.lytMdlBodyChildren[0]
                            num_cols = len(first_y.lytMdlBodyChildren)
                            col_labels = [""] * num_cols
                            col_codes = [""] * num_cols
                            col_ptr = 0
                            for lytMdlGroup in (getattr(lytMdlXHdrs, 'lytMdlGroups', []) or []):
                                for lytMdlHeader in lytMdlGroup.lytMdlHeaders:
                                    if all(lytMdlCell.isOpenAspectEntrySurrogate for lytMdlCell in lytMdlHeader.lytMdlCells):
                                        continue
                                for lytMdlCell in lytMdlHeader.lytMdlCells:
                                        if getattr(lytMdlCell, 'isOpenAspectEntrySurrogate', False):
                                            continue
                                        label_text = lytMdlCell.labelXmlText(0, "\u00a0") if hasattr(lytMdlCell, 'labelXmlText') else ''
                                        span = getattr(lytMdlCell, 'span', 1) or 1
                                        for _ in range(span):
                                            if col_ptr < num_cols:
                                                col_labels[col_ptr] = (col_labels[col_ptr] + (' / ' if col_labels[col_ptr] else '') + label_text)
                                                if not col_codes[col_ptr]:
                                                    col_codes[col_ptr] = _extract_code_from_obj(lytMdlCell, label_text)
                                                col_ptr += 1
                            # Ensure length matches
                            if len(col_labels) != num_cols:
                                col_labels = (col_labels + [""] * num_cols)[:num_cols]
                            if len(col_codes) != num_cols:
                                col_codes = (col_codes + [""] * num_cols)[:num_cols]
                        except Exception:
                            col_labels = []
                            col_codes = []
                        z_body = lytMdlTable.lytMdlBodyChildren[0]
                        # Attempt to parse display row/column codes from the rendered HTML file
                        try:
                            html_path = output_dir / f"{table_id}.html"
                            if html_path.exists():
                                doc = etree.HTML(html_path.read_text(encoding="utf-8"))
                                # Collect display column codes: prefer a bottom-most numeric header row; fallback to header cells with pure digits per column
                                x_numeric_rows = []
                                for tr in doc.xpath('//tr'):
                                    vals = []
                                    for th in tr.xpath('.//th[contains(@class, "xAxisHdr")]'):
                                        try:
                                            txt = th.xpath('string(.)')
                                        except Exception:
                                            txt = (th.text or '')
                                        txt = (txt or '').replace('\u00a0',' ').strip()
                                        # Keep only terminal tokens if header contains composite text like "Exposure Value: SA Exposures 0010"
                                        toks = [t for t in txt.split() if t]
                                        last = toks[-1] if toks else txt
                                        vals.append(last)
                                    if vals and all(v.isdigit() for v in vals):
                                        x_numeric_rows.append(vals)
                                if x_numeric_rows:
                                    display_col_codes = x_numeric_rows[-1]
                                # If still empty, try to derive per-column code by scanning each column header group and taking the last numeric token seen vertically
                                if not display_col_codes and num_cols:
                                    col_acc = [""] * num_cols
                                    try:
                                        # Build a 2D grid of header text by column position
                                        hdr_rows = []
                                        for tr in doc.xpath('//tr'):
                                            row_vals = []
                                            for th in tr.xpath('.//th[contains(@class, "xAxisHdr")]'):
                                                try:
                                                    tx = th.xpath('string(.)')
                                                except Exception:
                                                    tx = (th.text or '')
                                                tx = (tx or '').replace('\u00a0',' ').strip()
                                                row_vals.append(tx)
                                            if row_vals:
                                                hdr_rows.append(row_vals)
                                        # Take last numeric token per column index
                                        for ci in range(num_cols):
                                            for row in reversed(hdr_rows):
                                                if ci < len(row):
                                                    import re
                                                    m = re.search(r"(\d{3,4})", row[ci])
                                                    if m:
                                                        col_acc[ci] = m.group(1)
                                                        break
                                    except Exception:
                                        pass
                                    if any(col_acc):
                                        display_col_codes = col_acc
                                # Collect display row codes by expanding rowspans of numeric yAxisHdr cells
                                display_row_codes = []
                                remaining = 0
                                current_code = ""
                                # count of body rows expected (y)
                                num_y_rows = lytMdlTable.numBodyCells("y") or 0
                                for tr in doc.xpath('//tr'):
                                    # read any new numeric yAxisHdr code with optional rowspan
                                    ths = tr.xpath('.//th[contains(@class, "yAxisHdr")]')
                                    # Expect label then numeric; pick the last numeric-only th
                                    new_code = None
                                    new_span = 1
                                    for th in ths:
                                        try:
                                            txt = th.xpath('string(.)')
                                        except Exception:
                                            txt = (th.text or '')
                                        txt = (txt or '').replace('\u00a0',' ').strip()
                                        if txt.isdigit():
                                            new_code = txt
                                            try:
                                                new_span = int(th.get('rowspan') or '1')
                                            except Exception:
                                                new_span = 1
                                    if new_code:
                                        current_code = new_code
                                        remaining = new_span
                                    # If this row appears to contain body cells, append code
                                    tds = tr.xpath('.//td[contains(@class, "cell")]')
                                    if tds:
                                        code_to_use = current_code or ""
                                        display_row_codes.append(code_to_use)
                                        if remaining > 0:
                                            remaining -= 1
                                    if num_y_rows and len(display_row_codes) >= num_y_rows:
                                        break
                                # Normalize lengths
                                if num_cols and display_col_codes and len(display_col_codes) != num_cols:
                                    display_col_codes = (display_col_codes + [""]*num_cols)[:num_cols]
                                if (lytMdlTable.numBodyCells("y") or 0) and display_row_codes and len(display_row_codes) != (lytMdlTable.numBodyCells("y") or 0):
                                    need = (lytMdlTable.numBodyCells("y") or 0)
                                    display_row_codes = (display_row_codes + [""]*need)[:need]
                        except Exception:
                            # Swallow parsing errors; display codes are best-effort
                            pass
                        z_tbl_index = 0
                        for y_body in z_body.lytMdlBodyChildren:
                            y_row_num = 0
                            # helper to derive codes from labels
                            def derive_row_code(idx:int) -> str:
                                import re
                                if idx < len(row_codes) and row_codes[idx]:
                                    return row_codes[idx]
                                if idx < len(row_labels):
                                    m = re.search(r"(\d{2,5})", row_labels[idx])
                                    if m:
                                        return m.group(1).lstrip('0') or m.group(1)
                                return str(idx+1)
                            def derive_col_code(idx:int) -> str:
                                import re
                                if idx < len(col_codes) and col_codes[idx]:
                                    return col_codes[idx]
                                if idx < len(col_labels):
                                    m = re.search(r"(\d{2,5})", col_labels[idx])
                                    if m:
                                        return m.group(1).lstrip('0') or m.group(1)
                                return str(idx+1)
                            for x_body in y_body.lytMdlBodyChildren:
                                col_idx = 0
                                for lytMdlCell in x_body.lytMdlBodyChildren:
                                    if getattr(lytMdlCell, 'isOpenAspectEntrySurrogate', False):
                                        continue
                                    facts_info: List[Dict[str, Any]] = []
                                    # Aggregate qualifiers (dimension members) across facts in this cell (best-effort)
                                    qualifiers: Dict[str, Dict[str, str]] = {}
                                    for f, v, justify in getattr(lytMdlCell, 'facts', []) or []:
                                        try:
                                            q = getattr(f, 'qname', None)
                                            concept_ns = getattr(q, 'namespaceURI', None) if q else None
                                            concept_ln = getattr(q, 'localName', None) if q else None
                                            facts_info.append({
                                                "conceptNamespace": concept_ns,
                                                "conceptLocalName": concept_ln,
                                                "contextRef": getattr(f, 'contextID', None),
                                                "unitRef": getattr(f, 'unitID', None),
                                                "value": getattr(f, 'value', None)
                                            })
                                            # Collect explicit dimension members for readable qualifiers
                                            ctx = getattr(f, 'context', None)
                                            if ctx is None and hasattr(model_xbrl, 'contexts') and getattr(f, 'contextID', None):
                                                try:
                                                    ctx = model_xbrl.contexts.get(f.contextID)
                                                except Exception:
                                                    ctx = None
                                            if ctx is not None:
                                                try:
                                                    qnameDims = getattr(ctx, 'qnameDims', {}) or {}
                                                    for dimQn, dimVal in qnameDims.items():
                                                        dim_local = getattr(dimQn, 'localName', str(dimQn))
                                                        # member QName for explicit dims
                                                        memQn = getattr(dimVal, 'memberQname', None) or getattr(dimVal, 'member', None)
                                                        mem_local = None
                                                        mem_label = None
                                                        if memQn is not None:
                                                            mem_local = getattr(memQn, 'localName', str(memQn))
                                                            # Resolve label if possible
                                                            try:
                                                                mem_concept = getattr(model_xbrl, 'qnameConcepts', {}).get(memQn)
                                                                if mem_concept is not None and hasattr(mem_concept, 'label'):
                                                                    mem_label = mem_concept.label(lang=lang, strip=True)
                                                            except Exception:
                                                                mem_label = None
                                                        if dim_local and (mem_label or mem_local):
                                                            qualifiers.setdefault(dim_local, {
                                                                "dimension": dim_local,
                                                                "member": mem_label or mem_local or ""
                                                            })
                                                except Exception:
                                                    pass
                                        except Exception:
                                            continue
                                    # Fallback labels for empty headers
                                    row_label = (row_labels[y_row_num] if y_row_num < len(row_labels) else "") or f"row {y_row_num+1}"
                                    col_label = (col_labels[col_idx] if col_idx < len(col_labels) else "") or f"col {col_idx+1}"
                                    cells.append({
                                        "rowIndex": y_row_num,
                                        "colIndex": col_idx,
                                        "rowLabel": row_label,
                                        "colLabel": col_label,
                                        "rowCode": derive_row_code(y_row_num),
                                        "colCode": derive_col_code(col_idx),
                                        "rowDisplayCode": (display_row_codes[y_row_num] if y_row_num < len(display_row_codes) else ""),
                                        "colDisplayCode": (display_col_codes[col_idx] if col_idx < len(display_col_codes) else ""),
                                        "facts": facts_info,
                                        "qualifiers": list(qualifiers.values()) if qualifiers else []
                                    })
                                    col_idx += 1
                                y_row_num += 1
                            z_tbl_index += 1
                mapping = {
                    "tableId": table_id,
                    "tableName": table_name,
                    "tableLabel": table_label,
                    "cells": cells
                }
                with open(output_dir / f"{table_id}.mapping.json", "w", encoding="utf-8") as fh:
                    json.dump(mapping, fh, ensure_ascii=False)
            except Exception:
                logger.exception("Failed to build mapping for table %s (name=%s)", table_id, table_name)

        def _inject_highlighter(html_file: Path, table_id: str) -> None:
            try:
                script = (
                    "\n<!-- Highlighter -->\n"
                    "<style> .cell-highlight{outline:3px solid #d00;background:#fff4f4}"
                    "#hlrPanel{position:fixed;right:10px;bottom:10px;background:#fff;border:1px solid #ccc;padding:8px;font:12px Arial,sans-serif;z-index:9999;max-width:480px}"
                    "#hlrPanel input{width:300px} #hlrPanel code{word-break:break-all} </style>\n"
                    "<div id=hlrPanel>"
                    "<div><strong>Highlight</strong> (?conceptNs, ?conceptLn, ?contextRef)</div>"
                    "<div style=margin-top:6px><code id=hlrStatus></code></div>"
                    "<div id=hlrError style=margin-top:6px;color:#900></div>"
                    "</div>\n"
                    "<script>(function(){\n"
                    "function q(k){const u=new URL(window.location);return u.searchParams.get(k)}\n"
                    f"const mappingUrl='{table_id}.mapping.json';\n"
                    "fetch(mappingUrl).then(r=>r.json()).then(m=>{\n"
                    "  const ns=q('conceptNs'); const ln=q('conceptLn'); const cx=q('contextRef');\n"
                    "  const rc=(q('rowCode')||'').replace(/^0+/,''), cc=(q('colCode')||'').replace(/^0+/, '');\n"
                    "  const err=q('errorText');\n"
                    "  const cells=m.cells||[];\n"
                    "  let matchIdx=[];\n"
                    "  for(let i=0;i<cells.length;i++){const c=cells[i];const facts=c.facts||[];\n"
                    "    if(rc||cc){\n"
                    "      const rOk = rc? (String(c.rowCode||'')===rc) : true;\n"
                    "      const cOk = cc? (String(c.colCode||'')===cc) : true;\n"
                    "      if(rOk && cOk){ matchIdx.push(i); continue; }\n"
                    "    }\n"
                    "    for(const f of facts){\n"
                    "      if(ns && f.conceptNamespace!==ns) continue;\n"
                    "      if(ln && f.conceptLocalName!==ln) continue;\n"
                    "      if(cx && f.contextRef!==cx) continue;\n"
                    "      matchIdx.push(i); break;\n"
                    "    }\n"
                    "  }\n"
                    "  const tds=Array.from(document.querySelectorAll('td.cell'));\n"
                    "  let hits=0;\n"
                    "  for(const i of matchIdx){ if(i<tds.length){ tds[i].classList.add('cell-highlight'); hits++; } }\n"
                    "  const st=document.getElementById('hlrStatus');\n"
                    "  if(st){\n"
                    "    const crit = rc||cc ? {rowCode:rc,colCode:cc} : {ns,ln,cx};\n"
                    "    st.textContent = 'criteria: ' + JSON.stringify(crit) + ' | highlighted cells: ' + hits + (hits>10? ' (+more)':'');\n"
                    "  }\n"
                    "  const he=document.getElementById('hlrError'); if(he && err){ he.textContent = err; }\n"
                    "}).catch(()=>{});\n"
                    "})();</script>\n"
                )
                html = html_file.read_text(encoding="utf-8")
                if "</body>" in html:
                    html = html.replace("</body>", script + "</body>")
                elif "</html>" in html:
                    html = html.replace("</html>", script + "</html>")
                else:
                    html += script
                html_file.write_text(html, encoding="utf-8")
            except Exception:
                logger.exception("Failed to inject highlighter into %s", str(html_file))

        for model_table in model_tables:
            table_id = getattr(model_table, "id", None) or getattr(model_table, "xlinkLabel", None) or f"table_{num_files+1}"
            # Derive table name expected by viewRenderedGrid (strip known prefixes)
            table_name = table_id
            for prefix in ("eba_t", "srb_t"):
                if table_name.startswith(prefix):
                    table_name = table_name[len(prefix):]
                    break

            tbl_file = output_dir / f"{table_id}.html"
            try:
                viewRenderedGrid(model_xbrl, str(tbl_file), lang=lang, cssExtras="", table=table_name)
                # Build sidecar mapping JSON
                # Attempt readable table label
                tbl_label = None
                try:
                    tbl_label = model_table.genLabel(lang=lang, strip=True)  # type: ignore[attr-defined]
                except Exception:
                    tbl_label = None
                _write_mapping(table_name, table_id, tbl_label or table_id)
                # Inject highlighter script and panel
                _inject_highlighter(tbl_file, table_id)
                num_files += 1
                li = etree.SubElement(ul, "li")
                a = etree.SubElement(li, "a", href=f"{table_id}.html")
                # Prefer generated label text; fallback to id
                label = None
                try:
                    label = model_table.genLabel(lang=lang, strip=True)  # type: ignore[attr-defined]
                except Exception:
                    label = None
                a.text = label or table_id
                tables_manifest.append({"tableId": table_id, "tableLabel": (label or table_id)})
            except Exception:
                logger.exception("Failed rendering table %s (name=%s)", table_id, table_name)
                # Skip broken table, continue
                continue

        # Write index.html
        with open(index_file, "wb") as fh:
            fh.write(etree.tostring(index_doc, encoding="utf-8", pretty_print=True, doctype='<!DOCTYPE html>'))

        # Write tables.json manifest for UI tabs
        try:
            (output_dir / 'tables.json').write_text(json.dumps(tables_manifest, ensure_ascii=False), encoding='utf-8')
        except Exception:
            logger.warning("Failed to write tables.json manifest")

        logger.info(
            "Rendered EBA tableset: %d tables, index=%s, dir=%s", num_files, str(index_file), str(output_dir)
        )
        return str(index_file)

    except Exception:
        logger.exception("Failed to render EBA tableset to %s", str(output_dir))
        raise


def render_single_table(
    model_xbrl: Any,
    out_file: Union[str, Path],
    table_id_or_name: str,
    lang: str = "en",
) -> str:
    """
    Render a single table to an HTML file using the rendered grid view.

    Args:
        model_xbrl: Loaded Arelle ModelXbrl
        out_file: Output HTML file path
        table_id_or_name: Table ID (e.g., 'eba_t...') or plain table name
        lang: Label language (default 'en')

    Returns:
        Absolute path to the generated HTML file.
    """
    if model_xbrl is None or not hasattr(model_xbrl, "modelManager"):
        raise ValueError("render_single_table requires a loaded ModelXbrl with a modelManager")

    out_path = Path(out_file).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    from arelle.rendering import RenderingEvaluator  # type: ignore
    from arelle.ViewFileRenderedGrid import viewRenderedGrid  # type: ignore

    # Ensure custom transforms are loaded for rendering
    mm = getattr(model_xbrl, 'modelManager', None)
    if mm and hasattr(mm, 'loadCustomTransforms'):
        mm.loadCustomTransforms()

    # Initialize rendering
    RenderingEvaluator.init(model_xbrl)

    # Derive name used by viewRenderedGrid
    tbl_name = table_id_or_name
    for prefix in ("eba_t", "srb_t"):
        if tbl_name.startswith(prefix):
            tbl_name = tbl_name[len(prefix):]
            break

    viewRenderedGrid(model_xbrl, str(out_path), lang=lang, cssExtras="", table=tbl_name)
    return str(out_path)


