"""Tests for ETL engine: ingest and column resolution in load path."""
import io

import pytest

pytest.importorskip("fastexcel")
import polars as pl  # noqa: E402

from ingestion.engine import (  # noqa: E402
    CAPTIVE_COL,
    CLIENT_COL,
    ingest_salesforce,
    merge_rlip_rap,
)


def _minimal_excel_bytes_with_canonical_headers() -> bytes:
    """Build a minimal Excel (table at row 0) with canonical column names."""
    df = pl.DataFrame({
        CAPTIVE_COL: ["Cap A", "Cap A", "Cap B"],
        CLIENT_COL: ["C1", "C2", "C1"],
        "Gross Written Premium": [100.0, 200.0, 50.0],
        "POPIC Fee RLIP": [1.0, 2.0, 0.5],
        "POPIC Fee RAP": [0.0, 0.0, 0.0],
    })
    buf = io.BytesIO()
    df.write_excel(buf)
    return buf.getvalue()


def _minimal_excel_bytes_with_fuzzy_headers() -> bytes:
    """Build a minimal Excel with fuzzy headers (case + symbols) to test column resolution."""
    df = pl.DataFrame({
        "captive Name: captive name": ["Cap A", "Cap A"],
        "Captive Name: Client": ["C1", "C2"],
        "Gross Written Premium": [100.0, 200.0],
        "POPIC Fee RLIP": [1.0, 2.0],
        "POPIC Fee RAP": [0.0, 0.0],
    })
    buf = io.BytesIO()
    df.write_excel(buf)
    return buf.getvalue()


class TestIngestSalesforce:
    def test_ingest_returns_data_and_metadata(self):
        contents = _minimal_excel_bytes_with_canonical_headers()
        result = ingest_salesforce(contents, filename="Test_November 2025.xlsx")
        assert "data" in result
        assert "ingestion_metadata" in result
        assert isinstance(result["data"], list)
        meta = result["ingestion_metadata"]
        assert "file_type" in meta
        assert "filenames" in meta
        assert "period_from_filename" in meta

    def test_ingest_aggregates_by_captive_client(self):
        contents = _minimal_excel_bytes_with_canonical_headers()
        result = ingest_salesforce(contents)
        rows = result["data"]
        assert len(rows) == 3
        by_key = {(r[CAPTIVE_COL], r[CLIENT_COL]): r for r in rows}
        assert ("Cap A", "C1") in by_key
        assert by_key[("Cap A", "C1")]["Gross Written Premium"] == 100.0

    def test_ingest_with_fuzzy_headers_succeeds(self):
        """Headers like 'captive Name: captive name' are resolved to canonical."""
        contents = _minimal_excel_bytes_with_fuzzy_headers()
        result = ingest_salesforce(contents)
        assert result["data"]
        assert all(CAPTIVE_COL in r for r in result["data"])

    def test_missing_required_columns_raises(self):
        df = pl.DataFrame({"Other Col": [1, 2]})
        buf = io.BytesIO()
        df.write_excel(buf)
        with pytest.raises(ValueError, match="Missing required columns"):
            ingest_salesforce(buf.getvalue())


class TestMergeRlipRap:
    def test_merge_requires_same_period(self):
        # Two minimal files without Month/Year columns: both have canonical_period None, so they match
        c1 = _minimal_excel_bytes_with_canonical_headers()
        c2 = _minimal_excel_bytes_with_canonical_headers()
        result = merge_rlip_rap(c1, c2, "rlip.xlsx", "rap.xlsx")
        assert "data" in result
        assert result["ingestion_metadata"]["file_type"] == "merged"
