"""
Period parsing from filename and header, and discrepancy note formatting.
Used for ETL metadata and future assistant context.
"""
import re
from typing import Optional

# Month name to number for canonical (year, month) representation
_MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


def _month_name_to_int(name: str) -> Optional[int]:
    s = name.strip().lower()
    for i, m in enumerate(_MONTH_NAMES, start=1):
        if m.startswith(s) or s.startswith(m):
            return i
    return None


def parse_period_from_filename(filename: Optional[str]) -> Optional[tuple[int, int]]:
    """
    Parse (year, month) from filename if possible.
    Looks for patterns like "November 2025", "2025-11", "Nov 2025", etc.
    Returns (year, month) or None.
    """
    if not filename:
        return None
    # Remove extension
    base = filename.rsplit(".", 1)[0] if "." in filename else filename
    # Try "MonthName YYYY" or "MonthName YY" (allow separator before month, e.g. Report_August 2024)
    for i, m in enumerate(_MONTH_NAMES, start=1):
        pat = re.compile(rf"(?:^|[\s_\-]){m}\s*(\d{{4}}|\d{{2}})\b", re.I)
        match = pat.search(base)
        if match:
            year = int(match.group(1))
            if year < 100:
                year += 2000 if year < 50 else 1900
            return (year, i)
    # Try short month
    short = re.compile(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*[.\s-]*(\d{4}|\d{2})\b",
        re.I,
    )
    match = short.search(base)
    if match:
        year = int(match.group(2))
        if year < 100:
            year += 2000 if year < 50 else 1900
        mon = _month_name_to_int(match.group(1))
        if mon:
            return (year, mon)
    # Try YYYY-MM or YYYY-MM-DD (no leading \b so data_2025-11 matches)
    ym = re.search(r"(20\d{2})[-_](\d{1,2})\b", base)
    if ym:
        y, m = int(ym.group(1)), int(ym.group(2))
        if 1 <= m <= 12:
            return (y, m)
    return None


def parse_period_from_header_cells(cells: list[tuple[int, int, str]]) -> Optional[tuple[int, int]]:
    """
    Search a list of (row, col, value) header cells for a period.
    Values like "November 2025", "September 2025", "Month: November", "Year: 2025".
    Returns (year, month) or None.
    """
    for _r, _c, val in cells:
        if not val or not isinstance(val, str):
            continue
        s = val.strip()
        # "November 2025" style
        for m in _MONTH_NAMES:
            pat = re.compile(rf"^{m}\s+(\d{{4}})$", re.I)
            match = pat.search(s)
            if match:
                return (int(match.group(1)), _month_name_to_int(m) or 0)
        # "2025-11" or "2025/11"
        ym = re.search(r"(20\d{2})[-/](\d{1,2})", s)
        if ym:
            y, m = int(ym.group(1)), int(ym.group(2))
            if 1 <= m <= 12:
                return (y, m)
    return None


def format_period(year: int, month: int) -> str:
    """Human-readable period string."""
    if 1 <= month <= 12:
        return f"{_MONTH_NAMES[month - 1].capitalize()} {year}"
    return f"{year}-{month}"


def build_discrepancy_notes(
    table_period: Optional[tuple[int, int]],
    period_from_filename: Optional[tuple[int, int]],
    period_from_header: Optional[tuple[int, int]],
) -> list[str]:
    """
    Build human-readable notes when filename/header/table periods differ.
    Table period is authoritative; we only note when others disagree.
    """
    notes = []
    if table_period is None:
        return notes
    t_str = format_period(table_period[0], table_period[1])
    if period_from_filename is not None and period_from_filename != table_period:
        f_str = format_period(period_from_filename[0], period_from_filename[1])
        notes.append(f"Filename suggests {f_str}; table period is {t_str}.")
    if period_from_header is not None and period_from_header != table_period:
        h_str = format_period(period_from_header[0], period_from_header[1])
        notes.append(f"Header suggests {h_str}; table period is {t_str}.")
    if period_from_filename is not None and period_from_header is not None and period_from_filename != period_from_header:
        f_str = format_period(period_from_filename[0], period_from_filename[1])
        h_str = format_period(period_from_header[0], period_from_header[1])
        notes.append(f"Filename suggests {f_str}; header suggests {h_str}.")
    return notes
