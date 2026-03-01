"""
ETL engine for Commission Report spreadsheets.
Supports table discovery under header blocks; grouping by (Salesperson, Captive Name, Client);
subtotal/count/total row filtering; forward-fill of key columns; RLIP/RAP via Income Type (summed together per key).
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
SALESPERSON_COL = "Salesperson"
CAPTIVE_COL = "Captive Name"
CLIENT_COL = "Client Name (in POPIC)"

# Columns to sum when grouping (month P&L, month Commission, Total)
COMMISSION_MONTH_PNL = [
    "January P&L", "February P&L", "March P&L", "April P&L", "May P&L", "June P&L",
    "July P&L", "August P&L", "September P&L", "October P&L", "November P&L", "December P&L",
]
COMMISSION_MONTH_COMMISSION = [
    "January Commission", "February Commission", "March Commission", "April Commission",
    "May Commission", "June Commission", "July Commission", "August Commission",
    "September Commission", "October Commission", "November Commission", "December Commission",
]
COMMISSION_SUM_COLUMNS = COMMISSION_MONTH_PNL + COMMISSION_MONTH_COMMISSION + ["Total"]

# Take first value per group (non-additive)
COMMISSION_FIRST_COLUMNS = ["Year", "Income Type", "Commission Rate", "Account Name"]

# All commission-specific columns used to validate file type (require 35% present)
COMMISSION_TARGET_COLUMNS = COMMISSION_SUM_COLUMNS + COMMISSION_FIRST_COLUMNS

# Cleaned Data output column order: key cols, then Income Type/Commission Rate/Account Name, then Commissions (Jan–Dec, Total), then P&L (Jan–Dec)
CLEANED_OUTPUT_COLUMN_ORDER = (
    [SALESPERSON_COL, CAPTIVE_COL, CLIENT_COL]
    + ["Income Type", "Commission Rate", "Account Name", "Year"]
    + COMMISSION_MONTH_COMMISSION
    + ["Total"]
    + COMMISSION_MONTH_PNL
)

# Header region
COMMISSION_HEADER_SCAN_ROWS = 30


def _normalize_commission_header(col: str) -> str:
    """Strip unicode arrow (↑) and extra spaces from column names."""
    if col is None or not isinstance(col, str):
        return ""
    return col.replace("\u2191", "").replace(" ↑", "").strip()


def _find_commission_table_start_row(df_raw: pl.DataFrame) -> int:
    """
    Find 0-based row index where the table starts (row containing Captive Name and Salesperson/Client).
    Uses flexible matching. Assumes df_raw was read with has_header=False.
    """
    for i in range(min(df_raw.height, COMMISSION_HEADER_SCAN_ROWS)):
        row = df_raw.row(i, named=False)
        has_captive = False
        has_salesperson_or_client = False
        for cell in row:
            if cell is None:
                continue
            s = str(cell).strip()
            if header_matches_canonical(CAPTIVE_COL, s):
                has_captive = True
            if header_matches_canonical(SALESPERSON_COL, s) or header_matches_canonical(CLIENT_COL, s):
                has_salesperson_or_client = True
            if has_captive and has_salesperson_or_client:
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


def _load_commission_excel(contents: bytes) -> tuple[pl.DataFrame, list[tuple[int, int, str]]]:
    """
    Load the commission table from Excel, supporting optional header block above the table.
    Returns (dataframe with table data, header region cells for period parsing).
    """
    df_raw = pl.read_excel(
        source=io.BytesIO(contents),
        has_header=False,
        infer_schema_length=10000,
    )
    header_row = _find_commission_table_start_row(df_raw)
    header_cells = _raw_header_cells(df_raw, header_row)

    names_row = df_raw.row(header_row, named=False)
    col_names = [_normalize_commission_header(str(c)) for c in names_row]

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

    new_cols = {col: _normalize_commission_header(col) for col in df.columns}
    df = df.rename(new_cols)

    all_canonical = (
        [SALESPERSON_COL, CAPTIVE_COL, CLIENT_COL]
        + COMMISSION_SUM_COLUMNS
        + COMMISSION_FIRST_COLUMNS
    )
    mapping = resolve_column_mapping(list(df.columns), all_canonical)
    if mapping:
        df = df.rename(mapping)

    if SALESPERSON_COL not in df.columns or CAPTIVE_COL not in df.columns or CLIENT_COL not in df.columns:
        raise ValueError(
            "Upload a valid Commission Report file."
        )

    # Require at least 35% of COMMISSION_TARGET_COLUMNS so we only accept Commission Report files
    target_present = [c for c in COMMISSION_TARGET_COLUMNS if c in df.columns]
    min_required = math.ceil(0.35 * len(COMMISSION_TARGET_COLUMNS))
    if len(target_present) < min_required:
        raise ValueError("Upload a valid Commission Report file.")

    return df, header_cells


def _clean_and_aggregate_commission(df: pl.DataFrame) -> pl.DataFrame:
    """Forward-fill key columns, filter Subtotal/Count rows, clean numerics, group by (Salesperson, Captive, Client), sum/first."""
    # Forward-fill so blank cells inherit from above
    df = df.with_columns(pl.col(SALESPERSON_COL).cast(pl.String).fill_null(strategy="forward"))
    df = df.with_columns(pl.col(CAPTIVE_COL).cast(pl.String).fill_null(strategy="forward"))
    df = df.with_columns(pl.col(CLIENT_COL).cast(pl.String).fill_null(""))

    # Filter out Subtotal, Count, and Total rows (summary markers in first key column)
    df = df.filter(
        pl.col(SALESPERSON_COL).is_not_null()
        & (pl.col(SALESPERSON_COL).str.to_uppercase() != "SUBTOTAL")
        & (pl.col(SALESPERSON_COL).str.to_uppercase() != "COUNT")
        & (pl.col(SALESPERSON_COL).str.to_uppercase() != "TOTAL")
    )

    existing_sum = [c for c in COMMISSION_SUM_COLUMNS if c in df.columns]
    existing_first = [c for c in COMMISSION_FIRST_COLUMNS if c in df.columns]
    sum_cols = existing_sum
    first_cols = existing_first

    # Clean numeric columns: strip $, commas, parentheses for negatives, cast to float
    for col_name in sum_cols:
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

    agg_exprs = [pl.col(c).sum() for c in sum_cols] + [pl.col(c).first() for c in first_cols]
    group_cols = [SALESPERSON_COL, CAPTIVE_COL, CLIENT_COL]
    grouped = df.group_by(group_cols).agg(agg_exprs)
    # Reorder columns for Cleaned Data: key cols, then Income Type / Commission Rate / Account Name / Year, then Commissions (Jan–Dec, Total), then P&L (Jan–Dec)
    output_cols = [c for c in CLEANED_OUTPUT_COLUMN_ORDER if c in grouped.columns]
    return grouped.select(output_cols).sort([SALESPERSON_COL, CAPTIVE_COL])


def _get_commission_period_from_table(df: pl.DataFrame) -> Optional[tuple[int, int]]:
    """Derive (year, month) from Year column if present. Uses first non-null row."""
    if "Year" not in df.columns:
        return None
    for i in range(df.height):
        try:
            y_val = df["Year"][i]
            if y_val is None:
                continue
            year = int(float(y_val)) if isinstance(y_val, (int, float)) else int(str(y_val).strip())
            if 1900 <= year <= 2100:
                return (year, 1)
        except (ValueError, TypeError):
            continue
    return None


def ingest_commission(
    contents: bytes,
    filename: Optional[str] = None,
) -> dict:
    """
    Ingest one Commission Report file.
    Returns dict with keys: data (list[dict]), columns (list[str]), ingestion_metadata (dict with
    canonical_period, period_from_filename, period_from_header, discrepancy_notes, filenames).
    """
    df, header_cells = _load_commission_excel(contents)
    period_from_filename = parse_period_from_filename(filename)
    period_from_header = parse_period_from_header_cells(header_cells)

    grouped = _clean_and_aggregate_commission(df)
    canonical_period = _get_commission_period_from_table(df)

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
