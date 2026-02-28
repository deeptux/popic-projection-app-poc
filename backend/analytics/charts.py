"""
Chart-ready aggregations for cleaned ETL data.
Entity = Captive Name only, or (Captive Name, Client) when Client column is present.
Accepts canonical column names; optional aliases (e.g. ADTL RENT -> Additional Rent) for robustness.
"""
from __future__ import annotations

from typing import Any

# Canonical names (from ingestion engine)
CAPTIVE_COL = "Captive Name: Captive Name"
CLIENT_COL = "Captive Name: Client"
ADDITIONAL_RENT = "Additional Rent"
TOTAL_AVAILABLE_UNITS = "Total Available Units"
POPIC_FEE_RLIP = "POPIC Fee RLIP"
POPIC_FEE_RAP = "POPIC Fee RAP"

# Optional aliases for robustness (e.g. display/export variants). ADTL RENT = abbreviated "Additional Rent".
COLUMN_ALIASES: dict[str, str] = {
    "ADTL RENT": ADDITIONAL_RENT,
    "Additional Rent Charge": ADDITIONAL_RENT,
}


def _resolve_column(columns: list[str], canonical: str, aliases: dict[str, str] | None = None) -> str | None:
    """Return the first column that matches canonical or any alias, or None."""
    aliases = aliases or COLUMN_ALIASES
    candidates = [canonical] + [a for a, c in aliases.items() if c == canonical]
    for col in columns:
        if col in candidates:
            return col
    return None


def _entity_key(row: dict[str, Any], captive_col: str, client_col: str | None) -> str:
    """Single string key for entity: captive only or 'Captive | Client'."""
    captive = row.get(captive_col)
    cap = "" if captive is None else str(captive).strip()
    if client_col and client_col in row:
        client = row.get(client_col)
        cli = "" if client is None else str(client).strip()
        return f"{cap} | {cli}" if cli else cap
    return cap


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _aggregate_by_entity(
    data: list[dict[str, Any]],
    columns: list[str],
    value_col: str,
    take_first: bool = False,
) -> tuple[list[tuple[str, float]], str | None]:
    """
    Group by entity (captive or captive+client), aggregate value_col.
    take_first: use first value per entity (for non-additive like Total Available Units).
    Returns (list of (label, value) sorted by value desc), error message if missing col.
    """
    captive_col = CAPTIVE_COL if CAPTIVE_COL in columns else None
    if not captive_col:
        for alias, can in COLUMN_ALIASES.items():
            if can == CAPTIVE_COL and alias in columns:
                captive_col = alias
                break
        if not captive_col:
            return [], f"Missing required column: {CAPTIVE_COL}"
    client_col = _resolve_column(columns, CLIENT_COL) if CLIENT_COL != captive_col else None
    if not client_col and CLIENT_COL in columns:
        client_col = CLIENT_COL

    value_col_resolved = _resolve_column(columns, value_col)
    if not value_col_resolved:
        return [], f"Missing required column: {value_col}"

    agg: dict[str, list[float]] = {}
    for row in data:
        key = _entity_key(row, captive_col, client_col)
        if not key:
            continue
        v = _to_float(row.get(value_col_resolved))
        if v is None:
            continue
        if key not in agg:
            agg[key] = []
        agg[key].append(v)

    if take_first:
        result = [(k, (vs[0] if vs else 0.0)) for k, vs in agg.items()]
    else:
        result = [(k, sum(vs)) for k, vs in agg.items()]

    result.sort(key=lambda x: x[1], reverse=True)
    return result, None


def top_additional_rent_line(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Top 7 entities by Additional Rent (sum per entity). Returns labels and values for line chart."""
    pairs, err = _aggregate_by_entity(data, columns, ADDITIONAL_RENT, take_first=False)
    if err:
        return {"error": err}
    top = pairs[:7]
    return {"labels": [p[0] for p in top], "values": [p[1] for p in top]}


def top_total_available_units_bar(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Top 5 entities by Total Available Units (one value per entity). Returns labels and values for bar chart."""
    pairs, err = _aggregate_by_entity(data, columns, TOTAL_AVAILABLE_UNITS, take_first=True)
    if err:
        return {"error": err}
    top = pairs[:5]
    return {"labels": [p[0] for p in top], "values": [p[1] for p in top]}


def _pie_top_n_others(
    data: list[dict[str, Any]],
    columns: list[str],
    value_col: str,
    n: int,
) -> dict[str, Any]:
    """Top n entities by value sum + Others slice. Returns slices with label, value, percent."""
    pairs, err = _aggregate_by_entity(data, columns, value_col, take_first=False)
    if err:
        return {"error": err}
    total = sum(p[1] for p in pairs)
    if total == 0:
        return {"slices": []}
    top = pairs[:n]
    rest_sum = sum(p[1] for p in pairs[n:])
    slices = []
    for label, val in top:
        slices.append({"label": label, "value": val, "percent": round(100.0 * val / total, 2)})
    if rest_sum > 0:
        slices.append({"label": "Others", "value": rest_sum, "percent": round(100.0 * rest_sum / total, 2)})
    return {"slices": slices}


def pie_popic_fee_rlip(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Top 4 entities by POPIC Fee RLIP sum + Others."""
    value_col = POPIC_FEE_RLIP
    if not _resolve_column(columns, value_col):
        return {"error": f"Missing required column: {value_col}"}
    return _pie_top_n_others(data, columns, value_col, 4)


def pie_popic_fee_rap(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Top 4 entities by POPIC Fee RAP sum + Others."""
    value_col = POPIC_FEE_RAP
    if not _resolve_column(columns, value_col):
        return {"error": f"Missing required column: {value_col}"}
    return _pie_top_n_others(data, columns, value_col, 4)


def pie_popic_fee_comparison(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Two slices: total POPIC Fee RLIP vs total POPIC Fee RAP (percentage of combined total)."""
    rlip_col = _resolve_column(columns, POPIC_FEE_RLIP)
    rap_col = _resolve_column(columns, POPIC_FEE_RAP)
    if not rlip_col:
        return {"error": f"Missing required column: {POPIC_FEE_RLIP}"}
    if not rap_col:
        return {"error": f"Missing required column: {POPIC_FEE_RAP}"}
    total_rlip = 0.0
    total_rap = 0.0
    for row in data:
        v = _to_float(row.get(rlip_col))
        if v is not None:
            total_rlip += v
        v = _to_float(row.get(rap_col))
        if v is not None:
            total_rap += v
    combined = total_rlip + total_rap
    if combined == 0:
        return {"slices": [{"label": "POPIC Fee RLIP", "value": 0, "percent": 0}, {"label": "POPIC Fee RAP", "value": 0, "percent": 0}]}
    return {
        "slices": [
            {"label": "POPIC Fee RLIP", "value": total_rlip, "percent": round(100.0 * total_rlip / combined, 2)},
            {"label": "POPIC Fee RAP", "value": total_rap, "percent": round(100.0 * total_rap / combined, 2)},
        ]
    }


# --- Commission report analytics (January–December columns) ---
COMMISSION_MONTH_LABELS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
COMMISSION_MONTH_COMMISSION_COLS = [
    "January Commission", "February Commission", "March Commission", "April Commission",
    "May Commission", "June Commission", "July Commission", "August Commission",
    "September Commission", "October Commission", "November Commission", "December Commission",
]
COMMISSION_MONTH_PNL_COLS = [
    "January P&L", "February P&L", "March P&L", "April P&L", "May P&L", "June P&L",
    "July P&L", "August P&L", "September P&L", "October P&L", "November P&L", "December P&L",
]


def _commission_monthly_totals(data: list[dict[str, Any]], columns: list[str], month_cols: list[str]) -> dict[str, Any]:
    """Sum each month column across all rows. Returns labels (month names) and values."""
    found = [c for c in month_cols if c in columns]
    if len(found) != len(month_cols):
        missing = set(month_cols) - set(columns)
        return {"error": f"Missing commission columns: {sorted(missing)}"}
    values = []
    for col in month_cols:
        total = 0.0
        for row in data:
            v = _to_float(row.get(col))
            if v is not None:
                total += v
        values.append(total)
    return {"labels": COMMISSION_MONTH_LABELS[: len(values)], "values": values}


def commission_monthly_commission_line(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Totals per month for January Commission–December Commission. For line chart (months on X)."""
    return _commission_monthly_totals(data, columns, COMMISSION_MONTH_COMMISSION_COLS)


def commission_monthly_pnl_bar(data: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Totals per month for January P&L–December P&L. For bar chart (months on X)."""
    return _commission_monthly_totals(data, columns, COMMISSION_MONTH_PNL_COLS)
