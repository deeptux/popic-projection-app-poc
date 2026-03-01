"""
ETL engine for Referral Fee spreadsheets.
Supports table discovery under header blocks; grouping by (Vendor, Captive Name, Client);
subtotal/count/total row filtering; forward-fill of key columns; aggregation of referral
fee amounts by month and POPIC fee.
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

# --- Canonical key columns (grouping) ---
VENDOR_COL = "Vendor"
CAPTIVE_COL = "Captive Name"
CLIENT_COL = "Client Name (in POPIC)"

# Table/year columns
YEAR_COL = "Year"

# Referral percent column (treated as non-additive / first)
REFERRAL_PERCENT_COL = "Referral Fee %"

# Columns to sum when grouping: calendar months, POPIC fee, P&L month columns
REFERRAL_MONTH_COLUMNS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

REFERRAL_FEE_COLUMNS = [
    "POPIC Fee",
]

REFERRAL_PNL_MONTH_COLUMNS = [
    "P&L Name: January",
    "P&L Name: February",
    "P&L Name: March",
    "P&L Name: April",
    "P&L Name: May",
    "P&L Name: June",
    "P&L Name: July",
    "P&L Name: August",
    "P&L Name: September",
    "P&L Name: October",
    "P&L Name: November",
    "P&L Name: December",
]

REFERRAL_SUM_COLUMNS = (
    REFERRAL_MONTH_COLUMNS + REFERRAL_FEE_COLUMNS + REFERRAL_PNL_MONTH_COLUMNS
)

# Non-additive columns (take first per group)
REFERRAL_FIRST_COLUMNS = [YEAR_COL, REFERRAL_PERCENT_COL]

# All referral-specific columns used to validate file type (require 35% present)
REFERRAL_TARGET_COLUMNS = REFERRAL_SUM_COLUMNS + REFERRAL_FIRST_COLUMNS

# Cleaned output column order: key cols, then Referral Fee %, Year, POPIC Fee, then months, then P&L months
CLEANED_OUTPUT_COLUMN_ORDER = (
    [VENDOR_COL, CAPTIVE_COL, CLIENT_COL]
    + [REFERRAL_PERCENT_COL, YEAR_COL]
    + REFERRAL_FEE_COLUMNS
    + REFERRAL_MONTH_COLUMNS
    + REFERRAL_PNL_MONTH_COLUMNS
)

# Header region
REFERRAL_HEADER_SCAN_ROWS = 30


def _normalize_referral_header(col: str) -> str:
    """Strip unicode arrow (↑) and extra spaces from column names."""
    if col is None or not isinstance(col, str):
        return ""
    return col.replace("\u2191", "").replace(" ↑", "").strip()


def _find_referral_table_start_row(df_raw: pl.DataFrame) -> int:
    """
    Find 0-based row index where the table starts.
    Prefer a row that has Vendor (or Referrer) and at least one of Captive Name / Client Name.
    Fall back to row with both Captive Name and Client Name. Skip rows that look like
    a title row (e.g. first cell is "Referral Fee %" and no key columns).
    """
    for i in range(min(df_raw.height, REFERRAL_HEADER_SCAN_ROWS)):
        row = df_raw.row(i, named=False)
        has_vendor = False
        has_captive = False
        has_client = False
        first_cell = ""
        for c, cell in enumerate(row):
            if cell is not None:
                s = str(cell).strip()
                if c == 0:
                    first_cell = s
                if not s:
                    continue
                if header_matches_canonical(VENDOR_COL, s):
                    has_vendor = True
                if header_matches_canonical(CAPTIVE_COL, s):
                    has_captive = True
                if header_matches_canonical(CLIENT_COL, s):
                    has_client = True
        # Require at least Vendor + one of Captive/Client, or both Captive and Client
        if (has_vendor and (has_captive or has_client)) or (has_captive and has_client):
            return i
        # Skip obvious non-header: first cell is "Referral Fee %" and no key columns
        if first_cell and "referral" in first_cell.lower() and "fee" in first_cell.lower():
            continue
    return 0


def _raw_header_cells(df_raw: pl.DataFrame, header_row: int) -> list[tuple[int, int, str]]:
    """Collect (row, col, value) for rows above the table header for period parsing."""
    cells: list[tuple[int, int, str]] = []
    for r in range(min(header_row, df_raw.height)):
        row = df_raw.row(r, named=False)
        for c, cell in enumerate(row):
            if cell is not None and str(cell).strip():
                cells.append((r, c, str(cell).strip()))
    return cells


def _load_referral_excel(contents: bytes) -> tuple[pl.DataFrame, list[tuple[int, int, str]]]:
    """
    Load the referral table from Excel, supporting an optional header block above the table.
    Returns (dataframe with table data, header region cells for period parsing).
    """
    df_raw = pl.read_excel(
        source=io.BytesIO(contents),
        has_header=False,
        infer_schema_length=10000,
    )
    header_row = _find_referral_table_start_row(df_raw)
    header_cells = _raw_header_cells(df_raw, header_row)

    names_row = df_raw.row(header_row, named=False)
    col_names = [_normalize_referral_header(str(c)) for c in names_row]

    # Ensure unique column names so Polars rename() does not raise DuplicateError (e.g. duplicate/empty headers in Excel).
    _seen: dict[str, int] = {}
    _uniquified: list[str] = []
    for name in col_names:
        base = (name or "").strip() or "column"
        _seen[base] = _seen.get(base, 0) + 1
        _uniquified.append(base if _seen[base] == 1 else f"{base}_{_seen[base]}")
    col_names = _uniquified

    df_data = df_raw.slice(header_row + 1)
    if df_data.height == 0:
        df = pl.DataFrame(schema={c: pl.Utf8 for c in col_names})
    else:
        df = pl.DataFrame(
            [df_data.row(i, named=False) for i in range(df_data.height)],
            orient="row",
        )
        n = min(len(df.columns), len(col_names))
        df = df.rename({df.columns[i]: col_names[i] for i in range(n)})

    new_cols = {col: _normalize_referral_header(col) for col in df.columns}
    df = df.rename(new_cols)

    # Canonical names we care about
    all_canonical = [
        VENDOR_COL,
        CAPTIVE_COL,
        CLIENT_COL,
        YEAR_COL,
        REFERRAL_PERCENT_COL,
        *REFERRAL_SUM_COLUMNS,
    ]
    mapping = resolve_column_mapping(list(df.columns), all_canonical)
    if mapping:
        df = df.rename(mapping)

    # Vendor and Captive Name are required; Client is optional (some reports omit it)
    missing_required = [c for c in (VENDOR_COL, CAPTIVE_COL) if c not in df.columns]
    if missing_required:
        raise ValueError("Upload a valid Referral Report file.")

    # Require at least 35% of REFERRAL_TARGET_COLUMNS, or at least 3 target columns for shorter layouts
    # (e.g. Year + POPIC Fee + Referral Fee % with no month columns).
    target_present = [c for c in REFERRAL_TARGET_COLUMNS if c in df.columns]
    min_required = min(3, math.ceil(0.35 * len(REFERRAL_TARGET_COLUMNS)))
    if len(target_present) < min_required:
        raise ValueError("Upload a valid Referral Report file.")

    # Add Client column if missing so grouping and output order stay consistent
    if CLIENT_COL not in df.columns:
        df = df.with_columns(pl.lit("").alias(CLIENT_COL))

    return df, header_cells


def _clean_and_aggregate_referral(df: pl.DataFrame) -> pl.DataFrame:
    """Forward-fill keys, filter summary rows, clean numerics, group by (Vendor, Captive, Client)."""
    df = df.with_columns(pl.col(VENDOR_COL).cast(pl.String).fill_null(strategy="forward"))
    df = df.with_columns(pl.col(CAPTIVE_COL).cast(pl.String).fill_null(strategy="forward"))
    df = df.with_columns(pl.col(CLIENT_COL).cast(pl.String).fill_null(""))

    # Filter out Subtotal/Count/Total rows in Vendor
    df = df.filter(
        pl.col(VENDOR_COL).is_not_null()
        & (pl.col(VENDOR_COL).str.to_uppercase() != "SUBTOTAL")
        & (pl.col(VENDOR_COL).str.to_uppercase() != "COUNT")
        & (pl.col(VENDOR_COL).str.to_uppercase() != "TOTAL")
    )

    existing_sum = [c for c in REFERRAL_SUM_COLUMNS if c in df.columns]
    existing_first = [c for c in REFERRAL_FIRST_COLUMNS if c in df.columns]

    # Clean numeric sum columns: strip $, commas, parentheses for negatives, cast to float
    for col_name in existing_sum:
        if col_name not in df.columns:
            continue
        if df[col_name].dtype == pl.String:
            df = df.with_columns(
                pl.col(col_name)
                    .str.replace_all(r"\((.*)\)", "-$1")
                    .str.replace_all(r"[$,]", "")
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .alias(col_name)
            )
        else:
            df = df.with_columns(pl.col(col_name).fill_null(0.0).alias(col_name))

    agg_exprs = [pl.col(c).sum() for c in existing_sum] + [
        pl.col(c).first() for c in existing_first
    ]
    group_cols = [VENDOR_COL, CAPTIVE_COL, CLIENT_COL]
    grouped = df.group_by(group_cols).agg(agg_exprs)
    output_cols = [c for c in CLEANED_OUTPUT_COLUMN_ORDER if c in grouped.columns]
    return grouped.select(output_cols).sort([VENDOR_COL, CAPTIVE_COL])


def _get_referral_period_from_table(df: pl.DataFrame) -> Optional[tuple[int, int]]:
    """Derive (year, month) from Year column if present. Uses first non-null row."""
    if YEAR_COL not in df.columns:
        return None
    for i in range(df.height):
        try:
            y_val = df[YEAR_COL][i]
            if y_val is None:
                continue
            year = int(float(y_val)) if isinstance(y_val, (int, float)) else int(
                str(y_val).strip()
            )
            if 1900 <= year <= 2100:
                # Month is not a single column; treat as January of the year for metadata.
                return (year, 1)
        except (ValueError, TypeError):
            continue
    return None


def ingest_referral(
    contents: bytes,
    filename: Optional[str] = None,
) -> dict:
    """Ingest one Referral Fee report file and return cleaned table + metadata."""
    df, header_cells = _load_referral_excel(contents)
    period_from_filename = parse_period_from_filename(filename)
    period_from_header = parse_period_from_header_cells(header_cells)

    grouped = _clean_and_aggregate_referral(df)
    canonical_period = _get_referral_period_from_table(df)

    discrepancy_notes = build_discrepancy_notes(
        canonical_period, period_from_filename, period_from_header
    )

    canonical_period_str = (
        format_period(canonical_period[0], canonical_period[1]) if canonical_period else None
    )
    period_from_filename_str = (
        format_period(period_from_filename[0], period_from_filename[1])
        if period_from_filename
        else None
    )
    period_from_header_str = (
        format_period(period_from_header[0], period_from_header[1]) if period_from_header else None
    )

    data = grouped.to_dicts()
    columns = grouped.columns

    return {
        "data": data,
        "columns": columns,
        "ingestion_metadata": {
            "canonical_period": canonical_period_str,
            "period_from_filename": period_from_filename_str,
            "period_from_header": period_from_header_str,
            "discrepancy_notes": discrepancy_notes,
            "filenames": [filename] if filename else [],
        },
    }

