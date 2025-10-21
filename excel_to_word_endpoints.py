from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from typing import Optional
import os
import json
from datetime import datetime
from io import BytesIO
import logging

from openpyxl import load_workbook
from docx import Document
from docx.shared import Pt


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
        for r in range(num_rows):
            for c in range(num_cols):
                value = ws.cell(row=start_row + r, column=start_col_idx + c).value
                table.cell(r, c).text = "" if value is None else str(value)
        
        return table
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid table_range '{table_range}': {exc}")


def _replace_text_placeholders(doc: Document, replacements: dict) -> None:
    """Replace placeholders like {{NAME}} across paragraphs and table cells."""
    if not replacements:
        return

    def replace_in_run_text(text: str) -> str:
        for key, value in replacements.items():
            placeholder = f"{{{{{key}}}}}"
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


def _find_paragraph_with_token(doc: Document, token: str):
    """Find a paragraph whose normalized text contains token (normalized).
    Returns the paragraph or None.
    """
    desired = _normalize_placeholder_text(token)
    for paragraph in doc.paragraphs:
        normalized_text = _normalize_placeholder_text(paragraph.text)
        if desired in normalized_text:
            logger.info(f"Found paragraph with token '{token}': '{paragraph.text}' -> normalized: '{normalized_text}'")
            return paragraph
    # Also search paragraphs inside table cells just in case
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    normalized_text = _normalize_placeholder_text(p.text)
                    if desired in normalized_text:
                        logger.info(f"Found paragraph in table cell with token '{token}': '{p.text}' -> normalized: '{normalized_text}'")
                        return p
    logger.warning(f"Token '{token}' not found in any paragraph")
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
    excel: UploadFile = File(...),
    template: UploadFile = File(...),
    mappings: str = Form(...),  # required JSON
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

        try:
            mapping_dict = json.loads(mappings)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON mappings: {e}")

        logger.info(f"Loaded mappings: {mapping_dict}")

        for tag, value in mapping_dict.items():
            # -------------------------------
            # CASE 1: Direct value or single cell reference
            # -------------------------------
            if isinstance(value, str):
                if any(c.isalpha() for c in value) and any(c.isdigit() for c in value):
                    # Looks like a cell reference
                    ws = wb.active
                    try:
                        cell_val = ws[value].value
                        text_value = str(cell_val or "")
                    except Exception as e:
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
                ws = wb[sheet_name] if sheet_name and sheet_name in wb.sheetnames else wb.active
                range_str = value.get("range")
                insert_type = value.get("type", "table")  # "table" or "text"
                align = value.get("align", "left")

                if not range_str:
                    continue

                logger.info(f"Processing {tag} as {insert_type} from {range_str}")

                start, end = range_str.split(":")
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

                paragraph = _find_paragraph_with_token(doc, tag)
                if not paragraph:
                    doc.add_paragraph(f"⚠️ Tag {tag} not found in template.")
                    continue

                # Remove the tag text
                for run in paragraph.runs:
                    run.text = run.text.replace(tag, "").strip()

                # Create content based on type
                if insert_type == "table":
                    table = doc.add_table(rows=end_row - start_row + 1, cols=end_col_idx - start_col_idx + 1)
                    _apply_table_style_safely(table)

                    for r in range(end_row - start_row + 1):
                        for c in range(end_col_idx - start_col_idx + 1):
                            val = ws.cell(row=start_row + r, column=start_col_idx + c).value
                            table.cell(r, c).text = str(val or "")

                    # Align table to the right
                    if align.lower() == "right":
                        table.alignment = 2  # 0=left,1=center,2=right

                    paragraph._p.addnext(table._tbl)
                    logger.info(f"Inserted table for {tag}")

                elif insert_type == "text":
                    # Combine the range cells into lines of text
                    lines = []
                    for r in range(start_row, end_row + 1):
                        row_vals = []
                        for c in range(start_col_idx, end_col_idx + 1):
                            val = ws.cell(row=r, column=c).value
                            row_vals.append(str(val or ""))
                        lines.append(" | ".join(row_vals))

                    new_para = paragraph.insert_paragraph_before("\n".join(lines))
                    if align.lower() == "right":
                        new_para.paragraph_format.alignment = 2

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
async def excel_to_word(
    excel: UploadFile = File(None),
    template: UploadFile = File(...),
    sheet: str = Form(None),
    table_range: str = Form(None),
    mappings: str = Form(None)
):
    # ---- Load Word Template ----
    template_bytes = await template.read()
    doc = Document(BytesIO(template_bytes))

    # ---- Default placeholders ----
    replacements = {
        "{{date}}": datetime.now().strftime("%Y-%m-%d"),
        "{{name}}": "Default Name"
    }

    # ---- Handle Excel if provided ----
    if excel is not None:
        excel_bytes = await excel.read()
        df = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet if sheet else None)

        # If range is given, crop the dataframe
        if table_range:
            start_cell, end_cell = table_range.split(":")
            start_col = ord(start_cell[0].upper()) - 65
            end_col = ord(end_cell[0].upper()) - 65
            start_row = int(start_cell[1:]) - 1
            end_row = int(end_cell[1:])
            df = df.iloc[start_row:end_row, start_col:end_col + 1]

        # Convert table to Word
        table_html = df.to_html(index=False)
        replacements["{{table}}"] = table_html
    else:
        # No Excel sent → fallback text
        replacements["{{table}}"] = "No data table provided"

    # ---- Replace placeholders in the Word file ----
    for p in doc.paragraphs:
        for key, value in replacements.items():
            if key in p.text:
                inline = p.runs
                for i in range(len(inline)):
                    if key in inline[i].text:
                        inline[i].text = inline[i].text.replace(key, str(value))

    # ---- Replace inside tables ----
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    for key, value in replacements.items():
                        if key in p.text:
                            inline = p.runs
                            for i in range(len(inline)):
                                if key in inline[i].text:
                                    inline[i].text = inline[i].text.replace(key, str(value))

    # ---- Save and return the file ----
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".docx")
    doc.save(temp_file.name)

    return FileResponse(
        temp_file.name,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="filled_template.docx"
    )

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