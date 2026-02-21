"""Tests for period parsing and discrepancy notes."""
import pytest

from ingestion.period import (
    build_discrepancy_notes,
    format_period,
    parse_period_from_filename,
    parse_period_from_header_cells,
)


class TestParsePeriodFromFilename:
    def test_november_2025(self):
        assert parse_period_from_filename("RLIP Financial November 2025.xlsx") == (2025, 11)

    def test_august_2024(self):
        assert parse_period_from_filename("Report_August 2024.xlsx") == (2024, 8)

    def test_short_month(self):
        assert parse_period_from_filename("Summary Nov 2025.csv") == (2025, 11)

    def test_yyyy_mm(self):
        assert parse_period_from_filename("data_2025-11.xlsx") == (2025, 11)

    def test_none_for_empty(self):
        assert parse_period_from_filename(None) is None
        assert parse_period_from_filename("") is None

    def test_no_date_in_name(self):
        assert parse_period_from_filename("random_file.xlsx") is None


class TestParsePeriodFromHeaderCells:
    def test_november_2025_cell(self):
        cells = [(0, 0, "November 2025")]
        assert parse_period_from_header_cells(cells) == (2025, 11)

    def test_yyyy_mm_cell(self):
        cells = [(1, 2, "Report period: 2025/09")]
        assert parse_period_from_header_cells(cells) == (2025, 9)

    def test_empty(self):
        assert parse_period_from_header_cells([]) is None


class TestFormatPeriod:
    def test_valid(self):
        assert format_period(2025, 11) == "November 2025"

    def test_invalid_month(self):
        assert format_period(2025, 13) == "2025-13"


class TestBuildDiscrepancyNotes:
    def test_no_notes_when_all_match(self):
        notes = build_discrepancy_notes((2025, 11), (2025, 11), (2025, 11))
        assert notes == []

    def test_filename_differs(self):
        notes = build_discrepancy_notes((2025, 11), (2024, 8), None)
        assert len(notes) == 1
        assert "August 2024" in notes[0] and "November 2025" in notes[0]

    def test_header_differs(self):
        notes = build_discrepancy_notes((2025, 11), None, (2025, 9))
        assert len(notes) == 1
        assert "September 2025" in notes[0]

    def test_table_period_none(self):
        notes = build_discrepancy_notes(None, (2024, 8), (2025, 9))
        assert notes == []
