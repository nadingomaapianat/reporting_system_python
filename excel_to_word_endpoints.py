# Top-level imports
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import FileResponse
from typing import Optional
import os
import json
from datetime import datetime
from io import BytesIO
import logging

from openpyxl import load_workbook
from openpyxl.styles.numbers import is_date_format
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

router = APIRouter()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _read_excel_cell_values(excel_bytes: bytes, sheet_name: Optional[str]) -> tuple:
    """Load workbook and return (workbook, sheet)."""
    try:
        wb = load_workbook(filename=BytesIO(excel_bytes), data_only=True)
        ws = wb[sheet_name] if sheet_name and sheet_name in wb.sheetnames else wb.active
        return wb, ws
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed reading Excel file: {exc}")


def _get_cell_value(ws, address: str) -> str:
    try:
        return str(ws[address].value if ws[address].value is not None else "")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid cell address '{address}': {exc}")


def _build_table_from_range(document: Document, ws, table_range: str):
    """Insert a table into the document from an Excel range like 'A1:D10'."""
    try:
        start, end = table_range.split(":", 1)
        start_col = ''.join([c for c in start if c.isalpha()])
        start_row = int(''.join([c for c in start if c.isdigit()]))
        end_col = ''.join([c for c in end if c.isalpha()])
        end_row = int(''.join([c for c in end if c.isdigit()]))

        # Convert column letters to indices (A=1)
        def col_to_idx(col_letters: str) -> int:
            idx = 0
            for ch in col_letters.upper():
                idx = idx * 26 + (ord(ch) - ord('A') + 1)
            return idx

        start_col_idx = col_to_idx(start_col)
        end_col_idx = col_to_idx(end_col)

        num_rows = end_row - start_row + 1
        num_cols = end_col_idx - start_col_idx + 1

        table = document.add_table(rows=num_rows, cols=num_cols)
        _apply_table_style_safely(table)
        table.autofit = True
        for r in range(num_rows):
            for c in range(num_cols):
                xl_cell = ws.cell(row=start_row + r, column=start_col_idx + c)
                docx_cell = table.cell(r, c)
                _write_cell_with_style(docx_cell, xl_cell)
        
        return table
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid table_range '{table_range}': {exc}")


def _replace_text_placeholders(doc: Document, replacements: dict) -> None:
    """Replace placeholders like {{NAME}} across paragraphs and table cells."""
    if not replacements:
        return

    def replace_in_run_text(text: str) -> str:
        for key, value in replacements.items():
            # Accept both bare and braced keys: "NAME" or "{{NAME}}"
            placeholder = key if (key.startswith("{{") and key.endswith("}}")) else f"{{{{{key}}}}}"
            text = text.replace(placeholder, value)
        return text

    for paragraph in doc.paragraphs:
        for run in paragraph.runs:
            run.text = replace_in_run_text(run.text)

    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    for run in p.runs:
                        run.text = replace_in_run_text(run.text)


def _normalize_placeholder_text(text: str) -> str:
    """Normalize text by removing spaces and zero-width/RTL marks for robust matching."""
    if text is None:
        return ""
    zero_width = [
        "\u200e",  # LRM
        "\u200f",  # RLM
        "\u202a",  # LRE
        "\u202b",  # RLE
        "\u202c",  # PDF
        "\u202d",  # LRO
        "\u202e",  # RLO
    ]
    for z in zero_width:
        text = text.replace(z, "")
    return "".join(text.split())


def _find_paragraph_with_token_anywhere(doc: Document, token: str):
    # Robust search in paragraphs and inside table cells using normalized text
    desired = _normalize_placeholder_text(token)
    for paragraph in doc.paragraphs:
        if desired in _normalize_placeholder_text(paragraph.text):
            return paragraph
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    if desired in _normalize_placeholder_text(p.text):
                        return p
    return None


def _delete_paragraph(paragraph) -> None:
    """Remove a paragraph from the document."""
    p = paragraph._element
    parent = p.getparent()
    if parent is not None:
        parent.remove(p)


def _clear_token_in_document(doc: Document, token: str) -> int:
    """Remove all occurrences of token across paragraphs and table cells. Returns count removed.
    Works even when token is split across multiple runs by clearing entire paragraphs that contain it.
    """
    count = 0
    desired = _normalize_placeholder_text(token)
    # Normal paragraphs
    for paragraph in list(doc.paragraphs):
        if desired in _normalize_placeholder_text(paragraph.text):
            for run in list(paragraph.runs):
                run.text = ""
            count += 1
    # Paragraphs in table cells
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in list(cell.paragraphs):
                    if desired in _normalize_placeholder_text(p.text):
                        for run in list(p.runs):
                            run.text = ""
                        count += 1
    return count



@router.post("/api/reports/excel-to-word")
async def excel_to_word(
    request: Request,
    excel: UploadFile = File(...),
    template: UploadFile = File(...),
    mappings: str = Form(...),  # required JSON for text/cell values
):
    """
    Flexible Excel → Word generator
    Supports:
    - {{TAG}} placeholders replaced by Excel cell values or direct text
    - Table or text insertion from Excel ranges
    - RTL (right-to-left) table alignment when requested
    """

    try:
        excel_bytes = await excel.read()
        template_bytes = await template.read()

        wb = load_workbook(filename=BytesIO(excel_bytes), data_only=True)
        doc = Document(BytesIO(template_bytes))

        # Read base text mappings
        try:
            mapping_dict = json.loads(mappings)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON mappings: {e}")

        # Merge table mappings sent as repeated form fields
        form = await request.form()
        sheet_default = form.get('sheet')
        table_tags = form.getlist('table_tag') if hasattr(form, 'getlist') else []
        table_ranges = form.getlist('table_range') if hasattr(form, 'getlist') else []
        table_sheets = form.getlist('table_sheet') if hasattr(form, 'getlist') else []
    
        for i, ttag in enumerate(table_tags):
            trange = table_ranges[i] if i < len(table_ranges) else None
            tsheet = table_sheets[i] if i < len(table_sheets) else None
            if trange:
                existing = mapping_dict.get(ttag)
                base_type = "table"
                base_align = "left"
                if isinstance(existing, dict):
                    base_type = existing.get("type", base_type)
                    base_align = existing.get("align", base_align)
                payload = {
                    "sheet": tsheet or sheet_default,
                    "range": trange,
                    "type": base_type,
                    "align": base_align,
                }
                if isinstance(existing, dict):
                    existing.update({k: v for k, v in payload.items() if k not in existing})
                    mapping_dict[ttag] = existing
                else:
                    mapping_dict[ttag] = payload

        logger.info(f"Loaded mappings: {mapping_dict}")

        for tag, value in mapping_dict.items():
            # -------------------------------
            # CASE 1: Direct value or single cell reference
            # -------------------------------
            if isinstance(value, str):
                # Use selected sheet if provided; fallback to active
                ws = None
                if sheet_default and sheet_default in wb.sheetnames:
                    ws = wb[sheet_default]
                else:
                    ws = wb.active

                if any(c.isalpha() for c in value) and any(c.isdigit() for c in value):
                    try:
                        cell_val = ws[value].value
                        text_value = str(cell_val or "")
                    except Exception:
                        text_value = f"<Invalid cell {value}>"
                else:
                    text_value = value

                _replace_text_placeholders(doc, {tag: text_value})
                continue

            # -------------------------------
            # CASE 2: Object-based mapping (table or range)
            # -------------------------------
            elif isinstance(value, dict):
                sheet_name = value.get("sheet")
                ws = wb[sheet_name] if sheet_name and sheet_name in wb.sheetnames else (
                    wb[sheet_default] if sheet_default and sheet_default in wb.sheetnames else wb.active
                )
                range_str = value.get("range")
                insert_type = value.get("type", "table")
                align = value.get("align", "left")
                style_override = value.get("style", {}) or {}
                direction = value.get("direction", "auto")
                trim_empty = bool(value.get("trimEmpty", True))
                drop_empty_any = bool(value.get("dropEmptyCells", True))

                if not range_str:
                    continue

                logger.info(f"Processing {tag} as {insert_type} from {range_str}")

                # NEW: support 'A:G' and similar
                start_row, end_row, start_col_idx, end_col_idx = _parse_range_flexible(range_str, ws)

                rtl = False
                if direction == "rtl":
                    rtl = True
                elif direction == "auto":
                    rtl = _range_has_arabic(ws, start_row, end_row, start_col_idx, end_col_idx)

                paragraph = _find_paragraph_with_token(doc, tag)
                if not paragraph:
                    doc.add_paragraph(f"⚠️ Tag {tag} not found in template.")
                    continue

                for run in paragraph.runs:
                    run.text = run.text.replace(tag, "").strip()

                if insert_type == "row_tables":
                    _insert_row_tables(
                        doc, ws,
                        start_row, end_row,
                        start_col_idx, end_col_idx,
                        paragraph, rtl, align, style_override,
                        trim_empty=trim_empty,
                        drop_empty_any=drop_empty_any
                    )
                    logger.info(f"Inserted row-by-row tables for {tag}")
                elif insert_type == "table":
                    num_rows = end_row - start_row + 1
                    num_cols = end_col_idx - start_col_idx + 1

                    table = doc.add_table(rows=num_rows, cols=num_cols)
                    _apply_table_style_safely(table)
                    table.autofit = True

                    # If rtl, first Excel column goes to rightmost Word cell
                    for r in range(num_rows):
                        for c in range(num_cols):
                            target_col = (num_cols - 1 - c) if rtl else c
                            xl = ws.cell(row=start_row + r, column=start_col_idx + c)
                            _write_cell_with_style(table.cell(r, target_col), xl, {**style_override, "forceRTL": rtl})

                    # Align and RTL table
                    if align.lower() == "right" or rtl:
                        table.alignment = WD_TABLE_ALIGNMENT.RIGHT
                    elif align.lower() == "center":
                        table.alignment = WD_TABLE_ALIGNMENT.CENTER
                    else:
                        table.alignment = WD_TABLE_ALIGNMENT.LEFT
                    _set_table_rtl(table, rtl)

                    paragraph._p.addnext(table._tbl)
                    logger.info(f"Inserted table for {tag}")

                elif insert_type == "text":
                    lines = []
                    for r in range(start_row, end_row + 1):
                        cols = list(range(start_col_idx, end_col_idx + 1))
                        if rtl:
                            cols = cols[::-1]
                        row_vals = []
                        for c_idx in cols:
                            val = ws.cell(row=r, column=c_idx).value
                            row_vals.append(str(val or ""))
                        lines.append("    ".join(row_vals))  # spaced columns
                    new_para = paragraph.insert_paragraph_before("\n".join(lines))
                    new_para.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT if (rtl or align.lower() == "right") else (
                        WD_ALIGN_PARAGRAPH.CENTER if align.lower() == "center" else WD_ALIGN_PARAGRAPH.LEFT
                    )
                    if rtl:
                        _set_paragraph_rtl(new_para, True)

                    logger.info(f"Inserted text for {tag}")

        # Save output
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"excel_to_word_{timestamp}.docx"
        filepath = os.path.join(export_dir, filename)
        doc.save(filepath)

        return FileResponse(filepath, filename=filename)

    except Exception as e:
        logger.error(f"Excel to Word conversion failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generating Word file: {e}")

# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------

def _apply_table_style_safely(table):
    """Apply a table style safely, falling back to default if style doesn't exist."""
    try:
        table.style = 'Table Grid'
    except (ValueError, KeyError):
        # If Table Grid doesn't exist, try other common styles
        try:
            table.style = 'Light Grid'
        except (ValueError, KeyError):
            try:
                table.style = 'Table Normal'
            except (ValueError, KeyError):
                # If no styles work, just use default (no style)
                pass

def _build_table_from_worksheet(doc, ws, min_row, max_row, min_col, max_col):
    """Create a Word table from Excel sheet cells."""
    table = doc.add_table(rows=max_row - min_row + 1, cols=max_col - min_col + 1)
    _apply_table_style_safely(table)
    
    for r in range(min_row, max_row + 1):
        for c in range(min_col, max_col + 1):
            val = ws.cell(row=r, column=c).value
            table.cell(r - min_row, c - min_col).text = str(val or "")
    return table


def _find_paragraph_with_token(doc, token):
    """Find a paragraph that contains a given token like {{TABLE}}."""
    for para in doc.paragraphs:
        if token in para.text:
            return para
    return None


@router.post("/api/reports/excel-to-word/preview")
async def excel_to_word_preview(
    excel: UploadFile = File(...),
    sheet: Optional[str] = Form(None)
):
    try:
        excel_bytes = await excel.read()
        wb = load_workbook(filename=BytesIO(excel_bytes), data_only=True)
        sheet_names = wb.sheetnames
        ws = wb[sheet] if sheet and sheet in sheet_names else wb.active

        # Compute used size (rows/cols having data)
        max_row_used = 0
        max_col_used = 0
        for r_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
            last_col_with_data = 0
            row_has_data = False
            for c_idx, val in enumerate(row, start=1):
                if val not in (None, ""):
                    row_has_data = True
                    last_col_with_data = c_idx
            if row_has_data:
                max_row_used = r_idx
                if last_col_with_data > max_col_used:
                    max_col_used = last_col_with_data

        return {
            "sheetNames": sheet_names,
            "sheetPreview": {
                "size": {"rows": max_row_used, "cols": max_col_used}
            }
        }
    except Exception as exc:
        logger.error(f"Excel preview analysis failed: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail="Failed to analyze Excel file")
@router.post("/api/reports/excel-to-word/debug")
async def excel_to_word_debug(
    template: UploadFile = File(...),
    table_tag: Optional[str] = Form("{{TABLE}}"),
):
    """Debug endpoint to analyze Word template and show all text content."""
    try:
        template_bytes = await template.read()
        doc = Document(BytesIO(template_bytes))
        
        debug_info = {
            "table_tag": table_tag,
            "total_paragraphs": len(doc.paragraphs),
            "paragraphs": [],
            "total_tables": len(doc.tables),
            "table_cells": [],
            "found_table_placeholder": False,
            "found_table_placeholder_normalized": False,
        }
        
        # Analyze all paragraphs
        for i, paragraph in enumerate(doc.paragraphs):
            para_text = paragraph.text
            normalized_text = _normalize_placeholder_text(para_text)
            
            para_info = {
                "index": i,
                "text": para_text,
                "normalized": normalized_text,
                "length": len(para_text),
                "runs": len(paragraph.runs),
                "contains_table": table_tag in para_text,
                "contains_table_normalized": table_tag in normalized_text,
            }
            
            # Show individual runs if there are multiple
            if len(paragraph.runs) > 1:
                para_info["runs_detail"] = []
                for j, run in enumerate(paragraph.runs):
                    para_info["runs_detail"].append({
                        "index": j,
                        "text": run.text,
                        "length": len(run.text)
                    })
            
            debug_info["paragraphs"].append(para_info)
            
            if table_tag in para_text:
                debug_info["found_table_placeholder"] = True
            if table_tag in normalized_text:
                debug_info["found_table_placeholder_normalized"] = True
        
        # Analyze table cells
        for table_idx, table in enumerate(doc.tables):
            for row_idx, row in enumerate(table.rows):
                for cell_idx, cell in enumerate(row.cells):
                    for para_idx, para in enumerate(cell.paragraphs):
                        para_text = para.text
                        normalized_text = _normalize_placeholder_text(para_text)
                        
                        cell_info = {
                            "table": table_idx,
                            "row": row_idx,
                            "cell": cell_idx,
                            "paragraph": para_idx,
                            "text": para_text,
                            "normalized": normalized_text,
                            "contains_table": table_tag in para_text,
                            "contains_table_normalized": table_tag in normalized_text,
                        }
                        
                        debug_info["table_cells"].append(cell_info)
                        
                        if table_tag in para_text:
                            debug_info["found_table_placeholder"] = True
                        if table_tag in normalized_text:
                            debug_info["found_table_placeholder_normalized"] = True
        
        return debug_info
        
    except Exception as exc:
        logger.error(f"Debug analysis failed: {exc}", exc_info=True)
        error_detail = {
            "category": "debug_error",
            "message": str(exc),
            "suggestion": "Check if Word template is valid and not corrupted.",
            "timestamp": datetime.now().isoformat()
        }
        raise HTTPException(status_code=500, detail=json.dumps(error_detail))


@router.post("/api/reports/excel-to-word/simple")
async def excel_to_word_simple(
    excel: UploadFile = File(...),
    sheet: Optional[str] = Form(None),
    table_range: Optional[str] = Form(None),
    table_sheet: Optional[str] = Form(None),
):
    """
    Simple Excel to Word conversion - creates a Word document with Excel data as a table.
    No template needed, just Excel data converted to Word table.
    """
    try:
        excel_bytes = await excel.read()
        
        # Load Excel
        wb = load_workbook(filename=BytesIO(excel_bytes), data_only=True)
        ws_mappings = wb[sheet] if sheet and sheet in wb.sheetnames else wb.active
        ws_table = wb[table_sheet] if table_sheet and table_sheet in wb.sheetnames else ws_mappings
        
        # Create new Word document
        doc = Document()
        
        # Add title
        title = doc.add_heading(f'Excel Data from {ws_table.title}', 0)
        title.alignment = 1  # Center alignment
        
        # Add info paragraph
        info = doc.add_paragraph()
        info.add_run(f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        info.add_run(f'Source Sheet: {ws_table.title}\n')
        if table_range:
            info.add_run(f'Data Range: {table_range}\n')
        info.alignment = 1  # Center alignment
        
        # Add spacing
        doc.add_paragraph()
        
        if table_range:
            # Use specific range
            try:
                start, end = table_range.split(":", 1)
                start_col = ''.join([c for c in start if c.isalpha()])
                start_row = int(''.join([c for c in start if c.isdigit()]))
                end_col = ''.join([c for c in end if c.isalpha()])
                end_row = int(''.join([c for c in end if c.isdigit()]))

                def col_to_idx(col_letters: str) -> int:
                    idx = 0
                    for ch in col_letters.upper():
                        idx = idx * 26 + (ord(ch) - ord('A') + 1)
                    return idx

                start_col_idx = col_to_idx(start_col)
                end_col_idx = col_to_idx(end_col)

                num_rows = end_row - start_row + 1
                num_cols = end_col_idx - start_col_idx + 1

                # Create table
                table = doc.add_table(rows=num_rows, cols=num_cols)
                _apply_table_style_safely(table)
                
                # Fill table with data
                for r in range(num_rows):
                    for c in range(num_cols):
                        value = ws_table.cell(row=start_row + r, column=start_col_idx + c).value
                        table.cell(r, c).text = "" if value is None else str(value)
                        
                logger.info(f"Created table with {num_rows} rows and {num_cols} columns from range {table_range}")
                
            except Exception as e:
                logger.error(f"Failed to create table from range {table_range}: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid table range: {e}")
        else:
            # Use entire sheet (first 50 rows x 20 columns)
            max_rows = min(ws_table.max_row or 0, 50)
            max_cols = min(ws_table.max_column or 0, 20)
            
            if max_rows > 0 and max_cols > 0:
                # Create table
                table = doc.add_table(rows=max_rows, cols=max_cols)
                _apply_table_style_safely(table)
                
                # Fill table with data
                for r in range(max_rows):
                    for c in range(max_cols):
                        value = ws_table.cell(row=r + 1, column=c + 1).value
                        table.cell(r, c).text = "" if value is None else str(value)
                        
                logger.info(f"Created table with {max_rows} rows and {max_cols} columns from entire sheet")
            else:
                raise HTTPException(status_code=400, detail="No data found in Excel sheet")

        # Save file to exports
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"excel_to_word_simple_{timestamp}.docx"
        filepath = os.path.join(export_dir, filename)
        doc.save(filepath)

        logger.info(f"Simple Word document saved: {filepath}")

        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"X-Export-Src": filepath},
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Simple Excel to Word generation failed: {exc}", exc_info=True)
        error_detail = {
            "category": "simple_generation_error",
            "message": str(exc),
            "suggestion": "Check Excel file format and ensure it contains data.",
            "timestamp": datetime.now().isoformat()
        }
        raise HTTPException(status_code=500, detail=json.dumps(error_detail))


@router.post("/api/reports/excel-to-word/no-replace")
async def excel_to_word_no_replace(
    template: UploadFile = File(...)
):
    try:
        template_bytes = await template.read()

        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"excel_to_word_raw_{timestamp}.docx"
        filepath = os.path.join(export_dir, filename)

        with open(filepath, "wb") as f:
            f.write(template_bytes)

        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"X-Export-Src": filepath},
        )
    except Exception as exc:
        logger.error(f"Pass-through Word return failed: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to return template")
        raise HTTPException(status_code=500, detail=f"Error generating Word file: {e}")

# Helper functions
def _contains_arabic(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        code = ord(ch)
        if (0x0600 <= code <= 0x06FF) or (0x0750 <= code <= 0x077F) or (0x08A0 <= code <= 0x08FF):
            return True
    return False

def _set_paragraph_rtl(paragraph, rtl: bool = True):
    # Enable bidirectional rendering for Arabic
    p = paragraph._element
    pPr = p.get_or_add_pPr()
    bidi = pPr.find(qn('w:bidi'))
    if bidi is None:
        bidi = OxmlElement('w:bidi')
        pPr.append(bidi)
    bidi.set(qn('w:val'), '1' if rtl else '0')

def _set_table_rtl(table, rtl: bool = True):
    try:
        tbl = table._tbl
        tblPr = tbl.tblPr
        if tblPr is None:
            tblPr = OxmlElement('w:tblPr')
            tbl._element.insert(0, tblPr)
        bidi = tblPr.find(qn('w:bidi'))
        if bidi is None:
            bidi = OxmlElement('w:bidi')
            tblPr.append(bidi)
        bidi.set(qn('w:val'), '1' if rtl else '0')
    except Exception:
        # Non-critical if not supported
        pass

# ---- NEW: Flexible range parsing helpers ----
def _col_to_idx(col_letters: str) -> int:
    """Convert Excel column letters to 1-based index (A=1)."""
    idx = 0
    for ch in (col_letters or "").upper():
        if 'A' <= ch <= 'Z':
            idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

def _detect_used_row_span(ws, start_col_idx: int, end_col_idx: int, start_row_min: int = 1) -> tuple[int, int]:
    """
    Scan the worksheet to find the first and last non-empty row across the given column range.
    Returns (start_row, end_row). Falls back to (start_row_min, ws.max_row or start_row_min) if no data.
    """
    max_row = ws.max_row or start_row_min
    first = None
    last = None

    for r in range(start_row_min, max_row + 1):
        row_has_data = False
        for c in range(start_col_idx, end_col_idx + 1):
            v = ws.cell(row=r, column=c).value
            if v is not None and str(v).strip() != "":
                row_has_data = True
                break
        if row_has_data:
            if first is None:
                first = r
            last = r

    if first is None or last is None:
        # No data found: return a safe span
        return start_row_min, max_row
    return first, last

def _parse_range_flexible(range_str: str, ws):
    """
    Parse ranges like:
      - 'A:G'        (columns only, detect used rows)
      - 'A5:G'       (start row fixed, detect end row)
      - 'A:G60'      (detect start row, end row fixed)
      - 'A1:G60'     (fully specified)
      - 'C7'         (single cell)
    Returns: (start_row, end_row, start_col_idx, end_col_idx)
    """
    s = (range_str or "").replace(" ", "")
    if not s:
        raise HTTPException(status_code=400, detail="Empty range string")

    def split_part(part: str) -> tuple[str, int | None]:
        letters = "".join(ch for ch in part if ch.isalpha())
        digits = "".join(ch for ch in part if ch.isdigit())
        return letters, (int(digits) if digits else None)

    if ":" in s:
        left, right = s.split(":", 1)
        left_col_letters, left_row = split_part(left)
        right_col_letters, right_row = split_part(right)

        if not left_col_letters or not right_col_letters:
            raise HTTPException(status_code=400, detail=f"Invalid range '{s}': missing column letters")

        start_col_idx = _col_to_idx(left_col_letters)
        end_col_idx = _col_to_idx(right_col_letters)
        if start_col_idx < 1 or end_col_idx < 1:
            raise HTTPException(status_code=400, detail=f"Invalid range '{s}': bad column letters")

        # Ensure start_col_idx <= end_col_idx
        if start_col_idx > end_col_idx:
            start_col_idx, end_col_idx = end_col_idx, start_col_idx

        # Determine start_row and end_row
        if left_row is not None and right_row is not None:
            start_row = max(1, left_row)
            end_row = max(start_row, right_row)
        elif left_row is not None and right_row is None:
            # 'A5:G' → detect end_row from left_row to bottom
            start_row = max(1, left_row)
            _, end_row = _detect_used_row_span(ws, start_col_idx, end_col_idx, start_row_min=start_row)
        elif left_row is None and right_row is not None:
            # 'A:G60' → detect start_row from top, end_row fixed
            start_row, _ = _detect_used_row_span(ws, start_col_idx, end_col_idx, start_row_min=1)
            end_row = max(start_row, right_row)
        else:
            # 'A:G' → detect both
            start_row, end_row = _detect_used_row_span(ws, start_col_idx, end_col_idx, start_row_min=1)

        return start_row, end_row, start_col_idx, end_col_idx
    else:
        # Single address like 'C7' or 'AA12'
        col_letters, row = split_part(s)
        if not col_letters or row is None:
            raise HTTPException(status_code=400, detail=f"Invalid range/address '{s}'")
        col_idx = _col_to_idx(col_letters)
        start_row = end_row = max(1, row)
        return start_row, end_row, col_idx, col_idx

def _range_has_arabic(ws, start_row, end_row, start_col_idx, end_col_idx) -> bool:
    for r in range(start_row, end_row + 1):
        for c in range(start_col_idx, end_col_idx + 1):
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and _contains_arabic(v):
                return True
    return False

def _format_excel_value(xl_cell) -> str:
    val = xl_cell.value
    if val is None:
        return ""
    fmt = xl_cell.number_format or ""

    # Date/time formats
    try:
        if is_date_format(fmt):
            # Return like 30/Jun/2025 to match Excel-style preview
            if hasattr(val, 'strftime'):
                return val.strftime('%d/%b/%Y')
    except Exception:
        pass

    # Percent
    if isinstance(val, (int, float)) and '%' in fmt:
        try:
            # Excel typically stores 0.12 for 12%
            return f"{val:.2%}"
        except Exception:
            return str(val)

    # Thousands separators and decimals
    if isinstance(val, (int, float)):
        try:
            if '.00' in fmt or '.##' in fmt or '.0' in fmt or '.##' in fmt:
                # 2 decimal places if format implies decimals
                return format(val, ',.2f')
            if ',' in fmt:
                return format(val, ',.0f')
            return str(val)
        except Exception:
            return str(val)

    return str(val)

def _apply_cell_shading(docx_cell, xl_cell, style: Optional[dict]):
    # Shading OFF by default; only apply if explicitly requested
    if not style or not style.get('applyShading'):
        return
    try:
        fill = xl_cell.fill
        fill_type = getattr(fill, 'fill_type', None) or getattr(fill, 'patternType', None)
        if str(fill_type or '').lower() != 'solid':
            return
        # Prefer fgColor when available
        color = getattr(fill, 'fgColor', None) or getattr(fill, 'start_color', None)
        rgb = getattr(color, 'rgb', None)
        if not rgb or rgb in ('00000000', 'FFFFFFFF'):
            return

        hexrgb = rgb[-6:]  # strip alpha if ARGB
        tc = docx_cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd = OxmlElement('w:shd')
        shd.set(qn('w:val'), 'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), hexrgb)
        tcPr.append(shd)
    except Exception:
        # Non-critical
        pass

def _write_cell_with_style(docx_cell, xl_cell, style: Optional[dict] = None):
    text = _format_excel_value(xl_cell)
    docx_cell.text = ""
    para = docx_cell.paragraphs[0] if docx_cell.paragraphs else docx_cell.add_paragraph()
    run = para.add_run(text)

    # Alignment
    align = (xl_cell.alignment.horizontal or '').lower()
    if align == 'right':
        para.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    elif align == 'center':
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    else:
        para.alignment = WD_ALIGN_PARAGRAPH.LEFT

    # RTL hint or auto for Arabic
    force_rtl = bool(style and style.get('forceRTL'))
    if force_rtl or _contains_arabic(text):
        _set_paragraph_rtl(para, True)
        if para.alignment == WD_ALIGN_PARAGRAPH.LEFT:
            para.alignment = WD_ALIGN_PARAGRAPH.RIGHT

    # Font overrides
    font = xl_cell.font
    try:
        if font:
            if font.name:
                run.font.name = font.name
            if font.size:
                run.font.size = Pt(font.size)
            if font.bold is not None:
                run.font.bold = font.bold
            if font.italic is not None:
                run.font.italic = font.italic
            if font.color and getattr(font.color, 'rgb', None):
                run.font.color.rgb = RGBColor.from_string(font.color.rgb[-6:])
        # Explicit overrides from mapping
        if style:
            if style.get('fontName'):
                run.font.name = style['fontName']
            if style.get('fontSize'):
                try:
                    run.font.size = Pt(float(style['fontSize']))
                except Exception:
                    pass
            if style.get('bold') is not None:
                run.font.bold = bool(style['bold'])
            if style.get('italic') is not None:
                run.font.italic = bool(style['italic'])
            if style.get('color'):
                try:
                    run.font.color.rgb = RGBColor.from_string(style['color'].replace('#', ''))
                except Exception:
                    pass
    except Exception:
        pass

    # Background shading only if asked
    _apply_cell_shading(docx_cell, xl_cell, style)


# Helper functions
def _insert_row_tables(doc, ws, start_row, end_row, start_col_idx, end_col_idx, anchor_paragraph, rtl, align, style_override, trim_empty=True, drop_empty_any=True):
    """
    Insert a sequence of 1-row tables, one per Excel row.
    Empty leading/trailing cells are trimmed per row to avoid ultra-narrow tables.
    """
    # Use ~6 inches as total table width
    page_width_in = 6.0
    current_anchor = anchor_paragraph._p

    for r in range(start_row, end_row + 1):
        # Collect row cells
        row_cells = [ws.cell(row=r, column=c) for c in range(start_col_idx, end_col_idx + 1)]

        # Visible span
        if drop_empty_any:
            visible = [cell for cell in row_cells if cell.value is not None and str(cell.value).strip() != ""]
            if not visible:
                continue
        else:
            left = 0
            right = len(row_cells) - 1
            if trim_empty:
                while left <= right and (row_cells[left].value is None or str(row_cells[left].value).strip() == ""):
                    left += 1
                while right >= left and (row_cells[right].value is None or str(row_cells[right].value).strip() == ""):
                    right -= 1
            if right < left:
                continue
            visible = row_cells[left:right + 1]

        num_cols = len(visible)

        # Create 1-row table and set widths
        table = doc.add_table(rows=1, cols=num_cols)
        _apply_table_style_safely(table)
        try:
            table.autofit = False
            col_w = max(page_width_in / max(num_cols, 1), 0.6)
            for col in table.columns:
                col.width = Inches(col_w)
        except Exception:
            table.autofit = True

        # Alignment + RTL
        if rtl or str(align).lower() == "right":
            table.alignment = WD_TABLE_ALIGNMENT.RIGHT
        elif str(align).lower() == "center":
            table.alignment = WD_TABLE_ALIGNMENT.CENTER
        else:
            table.alignment = WD_TABLE_ALIGNMENT.LEFT
        _set_table_rtl(table, rtl)

        # Fill cells: RTL places first Excel value at rightmost cell
        for i, xl_cell in enumerate(visible):
            target_col = (num_cols - 1 - i) if rtl else i
            _write_cell_with_style(table.cell(0, target_col), xl_cell, {**(style_override or {}), "forceRTL": rtl})

        # Insert table after anchor, chain for next insert
        current_anchor.addnext(table._tbl)
        current_anchor = table._tbl

        # Optional small spacer for readability
        spacer = anchor_paragraph.insert_paragraph_before("")
        spacer.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT if (rtl or str(align).lower() == "right") else (
            WD_ALIGN_PARAGRAPH.CENTER if str(align).lower() == "center" else WD_ALIGN_PARAGRAPH.LEFT
        )