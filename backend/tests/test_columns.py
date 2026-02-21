"""Tests for flexible column name matching."""
import pytest

from ingestion.columns import (
    header_matches_canonical,
    normalize_for_match,
    resolve_column_mapping,
)
from ingestion.engine import CAPTIVE_COL, CLIENT_COL


class TestNormalizeForMatch:
    def test_lowercase_and_strip(self):
        assert normalize_for_match("  Captive Name: Captive Name  ") == "captive name captive name"

    def test_remove_symbols(self):
        assert normalize_for_match("Captive Name: Captive Name #!") == "captive name captive name"

    def test_collapse_spaces(self):
        assert normalize_for_match("captiev   Name:  cap   tive name") == "captiev name cap tive name"


class TestHeaderMatchesCanonical:
    def test_exact_after_normalize(self):
        assert header_matches_canonical(CAPTIVE_COL, "Captive Name: Captive Name") is True

    def test_case_insensitive(self):
        assert header_matches_canonical(CAPTIVE_COL, "captive Name: captive name") is True

    def test_trailing_symbols(self):
        assert header_matches_canonical(CAPTIVE_COL, "Captive Name: Captive Name #!") is True

    def test_minor_typo(self):
        assert header_matches_canonical(CAPTIVE_COL, "captiev Name: cap tive name") is True

    def test_client_column(self):
        assert header_matches_canonical(CLIENT_COL, "Captive Name: Client") is True
        assert header_matches_canonical(CLIENT_COL, "captive name: client") is True

    def test_no_match(self):
        assert header_matches_canonical(CAPTIVE_COL, "Gross Written Premium") is False
        assert header_matches_canonical(CAPTIVE_COL, "") is False


class TestResolveColumnMapping:
    def test_identity_when_exact(self):
        actual = ["Captive Name: Captive Name", "Captive Name: Client"]
        mapping = resolve_column_mapping(actual, [CAPTIVE_COL, CLIENT_COL])
        assert mapping.get("Captive Name: Captive Name") == CAPTIVE_COL
        assert mapping.get("Captive Name: Client") == CLIENT_COL

    def test_fuzzy_maps_to_canonical(self):
        actual = ["captive Name: captive name", "Captive Name: Client"]
        mapping = resolve_column_mapping(actual, [CAPTIVE_COL, CLIENT_COL])
        assert mapping.get("captive Name: captive name") == CAPTIVE_COL

    def test_typo_maps(self):
        actual = ["captiev Name: cap tive name #!", "Captive Name: Client"]
        mapping = resolve_column_mapping(actual, [CAPTIVE_COL, CLIENT_COL])
        assert mapping.get("captiev Name: cap tive name #!") == CAPTIVE_COL
