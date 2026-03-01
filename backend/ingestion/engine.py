"""
ETL engine for Salesforce captive summary spreadsheets.
Supports combined (RLIP+RAP), RLIP-only, and RAP-only formats;
table discovery under header blocks; period from table/filename/header with discrepancy notes;
merge of separate RLIP and RAP files with same-period checksum.
"""
import io
import math
from typing import Optional

import polars as pl

from ingestion.columns import header_matches_canonical, resolve_column_mapping
from ingestion.period import (
    build_discrepancy_notes,
    format_period,
    parse_period_from_filename,
    parse_period_from_header_cells,
)

# --- Constants ---
CAPTIVE_COL = "Captive Name: Captive Name"
CLIENT_COL = "Captive Name: Client"

TARGET_COLUMNS = [
    "Additional Rent", "Additional Rent Charge",
    "Retained At Property", "Retained at Property",
    "Gross Written Premium", "Taxes", "Credit Card Fees",
    "Administrative Fees", "Net Premium to Captive",
    "Claims Reserves", "Operating Expenses", "Proxy Tax",
    "Other Expenses", "Net Income", "Total Available Units",
    "Enrolled Units", "POPIC Fee RLIP FOF", "POPIC Fee RAP FOF",
    "POPIC Fee RLIP", "POPIC Fee RAP",
    "POPIC Fee From Parent RLIP", "POPIC Fee From Parent RAP",
    "Penetration %",
]

RLIP_COLUMNS = [c for c in TARGET_COLUMNS if "RLIP" in c]
RAP_COLUMNS = [c for c in TARGET_COLUMNS if "RAP" in c]

# Same value per (Captive, Client); aggregate by taking one value (e.g. first), not sum. Enrolled Units and Penetration % are additive (sum RLIP + RAP).
NON_ADDITIVE_COLUMNS = ["Total Available Units"]

# Table columns used for canonical period (first match wins)
MONTH_COLUMN_CANDIDATES = ["Month", "Report Month", "Captive Name: Month"]
YEAR_COLUMN_CANDIDATES = ["Year", "Report Year", "Captive Name: Year"]

# Header region size to scan for period (rows above table)
HEADER_SCAN_ROWS = 20
HEADER_SCAN_COLS = 10


def _normalize_column_name(col: str) -> str:
    return col.replace(" ↑", "").strip() if isinstance(col, str) else str(col)


def _is_nuisance_header(name: str) -> bool:
    """True if this header is a placeholder/nuisance we should omit (e.g. empty, single letter 'x')."""
    if not name:
        return True
    s = name.strip()
    if not s:
        return True
    # Single-letter or common placeholders Polars/Excel use for unnamed columns
    if len(s) == 1 and s.isalpha():
        return True
    low = s.lower()
    if low in ("unnamed", "column", "null", "none"):
        return True
    return False


def _find_table_start_row(df_raw: pl.DataFrame) -> int:
    """
    Find 0-based row index where the table starts (row containing CAPTIVE_COL).
    Uses flexible matching: case-insensitive, symbols stripped, minor typos allowed.
    Assumes df_raw was read with has_header=False (columns like column_1, column_2, ...).
    """
    for i in range(min(df_raw.height, 200)):
        row = df_raw.row(i, named=False)
        for cell in row:
            if cell is None:
                continue
            if header_matches_canonical(CAPTIVE_COL, str(cell)):
                return i
    return 0


def _raw_header_cells(df_raw: pl.DataFrame, header_row: int) -> list[tuple[int, int, str]]:
    """Collect (row, col, value) for rows above the table header for period parsing."""
    cells = []
    for r in range(min(header_row, df_raw.height)):
        row = df_raw.row(r, named=False)
        for c, cell in enumerate(row):
            if cell is not None and str(cell).strip():
                cells.append((r, c, str(cell).strip()))
    return cells


def _load_excel_table(contents: bytes) -> tuple[pl.DataFrame, list[tuple[int, int, str]], dict]:
    """
    Load the data table from Excel, supporting optional header block above the table.
    Omits nuisance columns (empty, single-letter placeholders like 'x') and duplicate
    header names to avoid DuplicateError; records them in returned metadata.
    Returns (dataframe with table data, header region cells, load_metadata with header_issues).
    """
    header_issues: list[str] = []

    # Read without header to find table start
    df_raw = pl.read_excel(
        source=io.BytesIO(contents),
        has_header=False,
        infer_schema_length=10000,
    )
    header_row = _find_table_start_row(df_raw)
    header_cells = _raw_header_cells(df_raw, header_row)

    if header_row == 0:
        # Table at top: read again with default header
        df = pl.read_excel(source=io.BytesIO(contents), infer_schema_length=10000)
        # Still detect and drop nuisance/duplicate columns
        col_names = list(df.columns)
        kept_indices: list[int] = []
        kept_names: list[str] = []
        seen: set[str] = set()
        for i, name in enumerate(col_names):
            n = _normalize_column_name(str(name))
            if _is_nuisance_header(n):
                header_issues.append(f"Omitted nuisance column at index {i}: {repr(name)}")
                continue
            if n in seen:
                header_issues.append(f"Omitted duplicate column at index {i}: {repr(n)}")
                continue
            seen.add(n)
            kept_indices.append(i)
            kept_names.append(n)
        if kept_indices != list(range(len(col_names))):
            df = df.select([df.columns[i] for i in kept_indices])
            df = df.rename({df.columns[j]: kept_names[j] for j in range(len(kept_indices))})
    else:
        # Use header row as column names, rest as data
        names_row = df_raw.row(header_row, named=False)
        col_names = [_normalize_column_name(str(c)) for c in names_row]
        # Decide which columns to keep: skip nuisance and duplicate names
        kept_indices = []
        kept_names = []
        seen = set()
        for i, n in enumerate(col_names):
            if _is_nuisance_header(n):
                header_issues.append(f"Omitted nuisance column at index {i}: {repr(n) or '(empty)'}")
                continue
            if n in seen:
                header_issues.append(f"Omitted duplicate column at index {i}: {repr(n)}")
                continue
            seen.add(n)
            kept_indices.append(i)
            kept_names.append(n)

        df_data = df_raw.slice(header_row + 1)
        if df_data.height == 0:
            df = pl.DataFrame(schema={c: pl.Utf8 for c in kept_names})
        else:
            df = pl.DataFrame(
                [df_data.row(i, named=False) for i in range(df_data.height)],
                orient="row",
            )
            n_cols = len(df.columns)
            # Only use columns we're keeping; restrict to available indices
            kept_in_bounds = [i for i in kept_indices if i < n_cols]
            kept_names_bounds = [kept_names[j] for j in range(len(kept_indices)) if kept_indices[j] < n_cols]
            df = df.select([df.columns[i] for i in kept_in_bounds])
            df = df.rename({df.columns[j]: kept_names_bounds[j] for j in range(len(kept_in_bounds))})

    # Clean column names (in case of extra spaces)
    new_cols = {col: _normalize_column_name(col) for col in df.columns}
    df = df.rename(new_cols)

    # Map actual headers to canonical names (case-insensitive, symbols, minor typos)
    all_canonical = (
        [CAPTIVE_COL, CLIENT_COL]
        + TARGET_COLUMNS
        + MONTH_COLUMN_CANDIDATES
        + YEAR_COLUMN_CANDIDATES
    )
    mapping = resolve_column_mapping(list(df.columns), all_canonical)
    if mapping:
        df = df.rename(mapping)

    # RLIP/RAP summary exports sometimes have an empty/null header for the Client column (Polars reads as "None")
    if CLIENT_COL not in df.columns and "None" in df.columns:
        df = df.rename({"None": CLIENT_COL})

    # Client column is optional; if missing, add empty so grouping by Captive+Client still works
    if CLIENT_COL not in df.columns:
        df = df.with_columns(pl.lit("").alias(CLIENT_COL))

    if CAPTIVE_COL not in df.columns:
        raise ValueError(f"Missing required columns. Found: {df.columns}")

    # Require at least 35% of TARGET_COLUMNS so we only accept Salesforce Captive Summary files.
    # RAP-only and RLIP-only exports have a subset of columns (~12); full combined has more.
    # Referral/Commission maps only 1–2 targets, so 35% (ceil 9 of 24) keeps them invalid.
    target_present = [c for c in TARGET_COLUMNS if c in df.columns]
    min_required = math.ceil(0.35 * len(TARGET_COLUMNS))
    if len(target_present) < min_required:
        raise ValueError("Upload a valid Salesforce Captive Report file.")

    load_metadata = {"header_issues": header_issues} if header_issues else {}
    return df, header_cells, load_metadata


def _clean_and_aggregate(df: pl.DataFrame) -> pl.DataFrame:
    """Forward-fill captive, filter Subtotals, clean numerics, group by Captive+Client, sum."""
    df = df.with_columns(pl.col(CAPTIVE_COL).fill_null(strategy="forward"))
    df = df.filter(
        pl.col(CAPTIVE_COL).is_not_null() & (pl.col(CAPTIVE_COL) != "Subtotal")
    )
    df = df.with_columns(pl.col(CLIENT_COL).cast(pl.String).fill_null(""))

    existing_sum_cols = [c for c in TARGET_COLUMNS if c in df.columns]
    sum_cols = [c for c in existing_sum_cols if c not in NON_ADDITIVE_COLUMNS]
    take_first_cols = [c for c in existing_sum_cols if c in NON_ADDITIVE_COLUMNS]
    clean_exprs = []
    for col_name in existing_sum_cols:
        if df[col_name].dtype == pl.String:
            clean_exprs.append(
                pl.col(col_name)
                .str.replace(r"\((.*)\)", "-$1")
                .str.replace_all(r"[$,]", "")
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .alias(col_name)
            )
        else:
            clean_exprs.append(pl.col(col_name).fill_null(0.0).alias(col_name))

    if clean_exprs:
        df = df.with_columns(clean_exprs)

    agg_exprs = [pl.col(c).sum() for c in sum_cols] + [pl.col(c).first() for c in take_first_cols]
    grouped = df.group_by([CAPTIVE_COL, CLIENT_COL]).agg(agg_exprs)
    return grouped.sort(CAPTIVE_COL)


def _get_canonical_period_from_table_pre_agg(df: pl.DataFrame) -> Optional[tuple[int, int]]:
    """
    Derive (year, month) from Month/Year columns. Call on cleaned df before group_by.
    Uses first non-null row.
    """
    month_col = next((c for c in MONTH_COLUMN_CANDIDATES if c in df.columns), None)
    year_col = next((c for c in YEAR_COLUMN_CANDIDATES if c in df.columns), None)
    if not month_col or not year_col:
        return None
    for i in range(df.height):
        try:
            y_val = df[year_col][i]
            m_val = df[month_col][i]
            if y_val is None or m_val is None:
                continue
            year = int(float(y_val)) if isinstance(y_val, (int, float)) else int(str(y_val).strip())
            m = m_val
            if isinstance(m, (int, float)):
                month = int(m)
            else:
                month = _month_name_to_number(str(m).strip())
            if 1 <= month <= 12 and 1900 <= year <= 2100:
                return (year, month)
        except (ValueError, TypeError):
            continue
    return None


def _month_name_to_number(s: str) -> int:
    """Return 1-12 for month name or number string."""
    from ingestion.period import _MONTH_NAMES
    s = s.strip().lower()
    for i, m in enumerate(_MONTH_NAMES, start=1):
        if m.startswith(s) or s.startswith(m) or s == str(i):
            return i
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _detect_file_type(df: pl.DataFrame) -> str:
    """
    Detect combined | RLIP-only | RAP-only by checking which numeric columns have values.
    """
    rlip_cols = [c for c in RLIP_COLUMNS if c in df.columns]
    rap_cols = [c for c in RAP_COLUMNS if c in df.columns]
    if not rlip_cols and not rap_cols:
        return "combined"
    rlip_has = False
    rap_has = False
    for c in rlip_cols:
        if df[c].dtype in (pl.Int64, pl.Float64):
            if df[c].sum() != 0 or df[c].null_count() < len(df):
                rlip_has = True
                break
    for c in rap_cols:
        if df[c].dtype in (pl.Int64, pl.Float64):
            if df[c].sum() != 0 or df[c].null_count() < len(df):
                rap_has = True
                break
    if rlip_has and rap_has:
        return "combined"
    if rlip_has:
        return "RLIP-only"
    if rap_has:
        return "RAP-only"
    return "combined"


def ingest_salesforce(
    contents: bytes,
    filename: Optional[str] = None,
) -> dict:
    """
    Ingest one Salesforce captive summary file (combined, RLIP-only, or RAP-only).
    Returns dict with keys: data (list[dict]), ingestion_metadata (dict with
    canonical_period, period_from_filename, period_from_header, discrepancy_notes,
    file_type, filenames).
    """
    df, header_cells, load_metadata = _load_excel_table(contents)
    period_from_filename = parse_period_from_filename(filename)
    period_from_header = parse_period_from_header_cells(header_cells)

    # Clean and get period from table (before group_by) if Month/Year exist
    df_clean = df.with_columns(pl.col(CAPTIVE_COL).fill_null(strategy="forward"))
    df_clean = df_clean.filter(
        pl.col(CAPTIVE_COL).is_not_null() & (pl.col(CAPTIVE_COL) != "Subtotal")
    )
    df_clean = df_clean.with_columns(pl.col(CLIENT_COL).cast(pl.String).fill_null(""))
    existing_sum_cols = [c for c in TARGET_COLUMNS if c in df_clean.columns]
    sum_cols = [c for c in existing_sum_cols if c not in NON_ADDITIVE_COLUMNS]
    take_first_cols = [c for c in existing_sum_cols if c in NON_ADDITIVE_COLUMNS]
    clean_exprs = []
    for col_name in existing_sum_cols:
        if df_clean[col_name].dtype == pl.String:
            clean_exprs.append(
                pl.col(col_name)
                .str.replace(r"\((.*)\)", "-$1")
                .str.replace_all(r"[$,]", "")
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .alias(col_name)
            )
        else:
            clean_exprs.append(pl.col(col_name).fill_null(0.0).alias(col_name))
    if clean_exprs:
        df_clean = df_clean.with_columns(clean_exprs)

    canonical_period = _get_canonical_period_from_table_pre_agg(df_clean)
    agg_exprs = [pl.col(c).sum() for c in sum_cols] + [pl.col(c).first() for c in take_first_cols]
    grouped = df_clean.group_by([CAPTIVE_COL, CLIENT_COL]).agg(agg_exprs)
    grouped = grouped.sort(CAPTIVE_COL)
    file_type = _detect_file_type(grouped)

    discrepancy_notes = build_discrepancy_notes(
        canonical_period, period_from_filename, period_from_header
    )

    canonical_period_str = format_period(canonical_period[0], canonical_period[1]) if canonical_period else None
    period_from_filename_str = format_period(period_from_filename[0], period_from_filename[1]) if period_from_filename else None
    period_from_header_str = format_period(period_from_header[0], period_from_header[1]) if period_from_header else None

    ingestion_metadata = {
        "canonical_period": canonical_period_str,
        "period_from_filename": period_from_filename_str,
        "period_from_header": period_from_header_str,
        "discrepancy_notes": discrepancy_notes,
        "file_type": file_type,
        "filenames": [filename] if filename else [],
    }
    if load_metadata.get("header_issues"):
        ingestion_metadata["header_issues"] = load_metadata["header_issues"]
    return {
        "data": grouped.to_dicts(),
        "ingestion_metadata": ingestion_metadata,
    }


def merge_rlip_rap(
    contents_rlip: bytes,
    contents_rap: bytes,
    filename_rlip: Optional[str] = None,
    filename_rap: Optional[str] = None,
) -> dict:
    """
    Process separate RLIP-only and RAP-only files and merge on (Captive, Client).
    Fails if table-derived periods differ. Returns same shape as single-file ingest
    plus merged metadata.
    """
    result_rlip = ingest_salesforce(contents_rlip, filename_rlip)
    result_rap = ingest_salesforce(contents_rap, filename_rap)

    meta_rlip = result_rlip["ingestion_metadata"]
    meta_rap = result_rap["ingestion_metadata"]
    period_rlip = meta_rlip.get("canonical_period")
    period_rap = meta_rap.get("canonical_period")

    if period_rlip != period_rap:
        raise ValueError(
            f"Cannot merge: RLIP file period is {period_rlip}, RAP file period is {period_rap}. "
            "Table month/year must match."
        )

    df_rlip = pl.DataFrame(result_rlip["data"])
    df_rap = pl.DataFrame(result_rap["data"])

    # Full outer join on Captive + Client; right duplicate columns get _right suffix
    merged = df_rlip.join(
        df_rap,
        on=[CAPTIVE_COL, CLIENT_COL],
        how="full",
    )
    # Build merged columns: RLIP cols from left, RAP cols from right, others sum both
    rlip_cols = [c for c in RLIP_COLUMNS if c in df_rlip.columns]
    rap_cols = [c for c in RAP_COLUMNS if c in df_rlip.columns]
    other_num_cols = [c for c in TARGET_COLUMNS if c in df_rlip.columns and c not in rlip_cols and c not in rap_cols]

    coalesced = [pl.col(CAPTIVE_COL), pl.col(CLIENT_COL)]
    for c in rlip_cols:
        coalesced.append(pl.col(c).fill_null(0.0).alias(c))
    for c in rap_cols:
        right_c = f"{c}_right"
        if right_c in merged.columns:
            coalesced.append(pl.col(right_c).fill_null(0.0).alias(c))
        else:
            coalesced.append(pl.col(c).fill_null(0.0).alias(c))
    for c in other_num_cols:
        right_c = f"{c}_right"
        if right_c in merged.columns:
            if c in NON_ADDITIVE_COLUMNS:
                coalesced.append(pl.coalesce([pl.col(c), pl.col(right_c)]).fill_null(0.0).alias(c))
            else:
                coalesced.append((pl.col(c).fill_null(0.0) + pl.col(right_c).fill_null(0.0)).alias(c))
        else:
            coalesced.append(pl.col(c).fill_null(0.0).alias(c))
    merged = merged.select(coalesced).sort(CAPTIVE_COL)

    combined_notes = list(meta_rlip.get("discrepancy_notes", [])) + list(meta_rap.get("discrepancy_notes", []))

    return {
        "data": merged.to_dicts(),
        "ingestion_metadata": {
            "canonical_period": period_rlip,
            "period_from_filename": meta_rlip.get("period_from_filename") or meta_rap.get("period_from_filename"),
            "period_from_header": meta_rlip.get("period_from_header") or meta_rap.get("period_from_header"),
            "discrepancy_notes": combined_notes,
            "file_type": "merged",
            "filenames": [filename_rlip, filename_rap] if filename_rlip or filename_rap else [],
            "merged_from": ["RLIP-only", "RAP-only"],
        },
    }


def consolidate_excel_data(contents: bytes, filename: Optional[str] = None) -> list[dict]:
    """
    Reads an Excel file, cleans it, and consolidates rows by summing numeric fields
    for each Client/Captive combo. Backward-compatible: returns list[dict] only.
    """
    result = ingest_salesforce(contents, filename)
    return result["data"]
