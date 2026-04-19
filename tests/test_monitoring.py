"""Tests for src/monitoring/metrics.py."""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch


MOCK_MAPPINGS = [
    {
        "season": "2025-26",
        "fpl_id": 1,
        "vaastav_id": 101,
        "understat_id": 1001,
        "confidence_score": 0.95,
    },
    {
        "season": "2025-26",
        "fpl_id": 2,
        "vaastav_id": 102,
        "understat_id": 1002,
        "confidence_score": 0.50,
    },
    {
        "season": "2025-26",
        "fpl_id": 3,
        "vaastav_id": 103,
        "understat_id": None,
        "confidence_score": 0.30,
    },
    {
        "season": "2024-25",
        "fpl_id": 4,
        "vaastav_id": 201,
        "understat_id": 2001,
        "confidence_score": 0.90,
    },
]

MOCK_DUP_MAPPINGS = [
    {"season": "2025-26", "fpl_id": 1},
    {"season": "2025-26", "fpl_id": 1},
    {"season": "2025-26", "fpl_id": 2},
    {"season": "2025-26", "fpl_id": None},
]


def _import_metrics():
    """Ensure src.monitoring.metrics is importable and return it."""
    mod = importlib.import_module("src.monitoring.metrics")
    return mod


class TestGetMappingQuality:
    """Tests for get_mapping_quality()."""

    def test_returns_mapping_rates(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", return_value=MOCK_MAPPINGS), \
             patch.object(mod, "get_supabase"):
            result = mod.get_mapping_quality()

        assert result["totals"]["total"] == 4
        assert result["totals"]["fpl"] == 4
        assert result["totals"]["vaastav"] == 4
        assert result["totals"]["understat"] == 3
        assert result["totals"]["fpl_to_understat"] == 3
        assert result["totals"]["vaastav_to_understat"] == 3
        assert result["totals"]["fpl_to_understat_rate"] == 75.0
        assert result["totals"]["vaastav_to_understat_rate"] == 75.0

    def test_by_season_breakdown(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", return_value=MOCK_MAPPINGS), \
             patch.object(mod, "get_supabase"):
            result = mod.get_mapping_quality()

        assert "2025-26" in result["by_season"]
        assert "2024-25" in result["by_season"]
        s25 = result["by_season"]["2025-26"]
        assert s25["total"] == 3
        assert s25["fpl"] == 3
        assert s25["fpl_to_understat"] == 2

    def test_season_filter_passed(self):
        mod = _import_metrics()
        mock_fetch = MagicMock(return_value=MOCK_MAPPINGS)
        with patch.object(mod, "fetch_all_paginated", mock_fetch), \
             patch.object(mod, "get_supabase"):
            mod.get_mapping_quality(season="2025-26")

        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs.get("filters") == {"season": "2025-26"}

    def test_empty_mappings(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", return_value=[]), \
             patch.object(mod, "get_supabase"):
            result = mod.get_mapping_quality()

        assert result == {"total": 0}


class TestCollectAllMetrics:
    """Tests for collect_all_metrics()."""

    def test_returns_all_sections(self):
        mod = _import_metrics()
        with patch.object(mod, "get_duplicate_counts", return_value={"silver_player_mapping": 0}), \
             patch.object(mod, "get_mapping_quality", return_value={"totals": {}}), \
             patch.object(mod, "get_table_counts", return_value={"bronze_fpl_players": 100}), \
             patch.object(mod, "get_supabase"):
            result = mod.collect_all_metrics()

        assert "collected_at" in result
        assert "season" in result
        assert "table_counts" in result
        assert "mapping_quality" in result
        assert "duplicates" in result
        assert result["season"] is None

    def test_season_passed_through(self):
        mod = _import_metrics()
        with patch.object(mod, "get_duplicate_counts", return_value={}), \
             patch.object(mod, "get_mapping_quality", return_value={}), \
             patch.object(mod, "get_table_counts", return_value={}), \
             patch.object(mod, "get_supabase"):
            result = mod.collect_all_metrics(season="2025-26")

        assert result["season"] == "2025-26"


class TestGetDuplicateCounts:
    """Tests for get_duplicate_counts()."""

    def test_detects_duplicates(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", return_value=MOCK_DUP_MAPPINGS), \
             patch.object(mod, "get_supabase"):
            result = mod.get_duplicate_counts()

        assert result["silver_player_mapping"] == 1

    def test_no_duplicates_when_empty(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", return_value=[]), \
             patch.object(mod, "get_supabase"):
            result = mod.get_duplicate_counts()

        assert result["silver_player_mapping"] == 0

    def test_handles_exception(self):
        mod = _import_metrics()
        with patch.object(mod, "fetch_all_paginated", side_effect=Exception("table missing")), \
             patch.object(mod, "get_supabase"):
            result = mod.get_duplicate_counts()

        assert result["silver_player_mapping"] == -1
