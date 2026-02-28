"""
Flexible column name matching for ETL: case-insensitive, strip symbols,
tolerant of small misspellings so headers like 'captive Name: captive name'
or 'captiev Name: cap tive name #!' are recognized as the Captive Name column.
"""
import re
from difflib import SequenceMatcher
from typing import Optional


def normalize_for_match(header: str) -> str:
    """
    Normalize a header string for comparison: lowercase, remove non-alphanumeric
    (except spaces), collapse multiple spaces, strip.
    """
    if not header or not isinstance(header, str):
        return ""
    s = header.strip().lower()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# Key tokens for required columns: if ratio is low, accept when these tokens appear (typo-tolerant).
_CAPTIVE_KEY_ANY = ("captive", "captiev", "captve", "captiv")
_CAPTIVE_KEY_ALL = ("name",)
_CLIENT_KEY_ANY = ("client", "cliant", "clent")
_SALESPERSON_KEY_ANY = ("salesperson", "sales person", "salesperson")
_VENDOR_KEY_ANY = ("vendor", "referrer", "referral")


def _key_tokens_match(canonical: str, n_act: str) -> bool:
    """Return True if normalized actual contains key tokens for the canonical column."""
    if not n_act:
        return False
    if "Captive Name: Captive Name" in canonical or canonical == "Captive Name: Captive Name":
        if not any(t in n_act for t in _CAPTIVE_KEY_ANY):
            return False
        if not any(t in n_act for t in _CAPTIVE_KEY_ALL):
            return False
        return True
    if "Client" in canonical or canonical == "Captive Name: Client":
        return any(t in n_act for t in _CLIENT_KEY_ANY)
    if canonical == "Captive Name":
        return any(t in n_act for t in _CAPTIVE_KEY_ANY) and any(t in n_act for t in _CAPTIVE_KEY_ALL)
    if canonical == "Client Name (in POPIC)":
        return any(t in n_act for t in _CLIENT_KEY_ANY) and "name" in n_act
    if canonical == "Salesperson":
        return any(t in n_act for t in _SALESPERSON_KEY_ANY)
    if canonical == "Vendor":
        return any(t in n_act for t in _VENDOR_KEY_ANY)
    return False


def header_matches_canonical(canonical: str, actual: str) -> bool:
    """
    Return True if actual header should be accepted as the canonical column.
    - Case insensitive.
    - Ignores trailing/embedded symbols and extra spaces.
    - Tolerant of small misspellings (e.g. captiev -> captive) via similarity ratio or key tokens.
    """
    if not actual or not canonical:
        return False
    n_can = normalize_for_match(canonical)
    n_act = normalize_for_match(actual)
    if n_can == n_act:
        return True
    if not n_can or not n_act:
        return False
    ratio = SequenceMatcher(None, n_can, n_act).ratio()
    if ratio >= 0.72:
        return True
    if ratio >= 0.40 and _key_tokens_match(canonical, n_act):
        return True
    return False


def resolve_column_mapping(
    actual_columns: list[str],
    canonical_list: list[str],
) -> dict[str, str]:
    """
    For each canonical name, find the first actual column that matches (flexible match).
    Returns mapping actual_column_name -> canonical_column_name so that
    df.rename(mapping) standardizes column names.
    """
    mapping: dict[str, str] = {}
    for canonical in canonical_list:
        for actual in actual_columns:
            if actual in mapping:
                continue
            if header_matches_canonical(canonical, actual):
                mapping[actual] = canonical
                break
    return mapping
