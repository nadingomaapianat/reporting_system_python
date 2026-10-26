"""
Size limits for uploaded files (same rule as the ICR template parser).

read_upload() reads at most UPLOAD_MAX_FILE_MB (default 50) and answers 413 above that. Office
files (docx/xlsx/pptx) are zip archives: one whose entries would unpack to more than
UPLOAD_MAX_UNZIPPED_MB (default 200) is refused before any parser opens it (zip bomb).
"""
import os
import zipfile
from io import BytesIO

from fastapi import HTTPException, UploadFile

_MB = 1024 * 1024


def _mb_setting(name: str, default_mb: float) -> int:
    try:
        mb = float(os.getenv(name, ""))
    except ValueError:
        mb = 0
    return int((mb if mb > 0 else default_mb) * _MB)


EXCEL = (".xlsx", ".xlsm", ".xls")
WORD = (".docx",)
XML = (".xml", ".xbrl", ".xsd")
PDF = (".pdf",)
CHECK_SCAN = (".pdf", ".png", ".jpg", ".jpeg")


async def read_upload(upload: UploadFile, allowed: tuple = None) -> bytes:
    """allowed: accepted file extensions (e.g. EXCEL); anything else is refused with 400 (WAPT06)."""
    if allowed is not None:
        name = (upload.filename or "").lower()
        if not name.endswith(tuple(allowed)):
            raise HTTPException(status_code=400, detail="Unsupported file type. Allowed: " + ", ".join(allowed))
    max_bytes = _mb_setting("UPLOAD_MAX_FILE_MB", 50)
    content = await upload.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail="File too large")
    if content[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(BytesIO(content)) as zf:
                unzipped = sum(info.file_size for info in zf.infolist())
        except zipfile.BadZipFile:
            unzipped = 0  # not a valid zip: left to the parser's own error handling
        if unzipped > _mb_setting("UPLOAD_MAX_UNZIPPED_MB", 200):
            raise HTTPException(status_code=413, detail="File too large")
    return content
