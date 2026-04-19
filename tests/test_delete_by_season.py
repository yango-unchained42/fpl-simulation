"""Tests for delete-by-season behavior in daily_bronze_update.

Verifies the SQL injection prevention and season validation logic.
"""

from __future__ import annotations

import re
import sys
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

# Mock dotenv at module level so the import of scripts.daily_bronze_update works
mock_dotenv = MagicMock()
sys.modules.setdefault("dotenv", mock_dotenv)


class TestSeasonValidation:
    """Tests for season format validation (SQL injection prevention)."""

    VALID_SEASONS = [
        "2024-25",
        "2023-24",
        "2020-21",
        "2099-00",
    ]

    INVALID_SEASONS = [
        "2024",           # missing hyphen and second part
        "24-25",          # short year
        "2024-2",         # single digit suffix
        "2024-2526",      # too many digits
        "DROP TABLE",     # SQL injection attempt
        "2024-25; DROP TABLE users;--",  # SQL injection
        "2024' OR '1'='1",  # SQL injection
        "",               # empty
        "abcd-ef",        # non-numeric
        "2024--25",       # double hyphen
        None,             # not a string
    ]

    @pytest.mark.parametrize("season", VALID_SEASONS)
    def test_valid_seasons(self, season):
        """Valid season formats should match the regex pattern."""
        assert re.match(r"^\d{4}-\d{2}$", str(season)) is not None

    @pytest.mark.parametrize("season", INVALID_SEASONS)
    def test_invalid_seasons(self, season):
        """Invalid season formats (including SQL injections) should be rejected."""
        assert re.match(r"^\d{4}-\d{2}$", str(season)) is None


class TestUploadTableDeleteBySeason:
    """Tests for the upload_table function's delete-by-season logic."""

    def _make_mock_supabase(self):
        """Create a mock supabase client that tracks delete calls."""
        from unittest.mock import MagicMock, call

        mock_client = MagicMock()

        # Chain: .table().delete().eq().execute()
        delete_chain = MagicMock()
        mock_client.table.return_value.delete.return_value = delete_chain
        delete_chain.eq.return_value = delete_chain
        delete_chain.execute.return_value = MagicMock()

        return mock_client, delete_chain

    def test_delete_uses_season_column_not_truncate(self):
        """DELETE should filter by season, not TRUNCATE the entire table."""
        import subprocess
        from unittest.mock import MagicMock, patch

        from scripts.daily_bronze_update import upload_table

        mock_client, delete_chain = self._make_mock_supabase()

        # Make get_table_columns return some valid columns
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"season": "2024-25", "col_a": 1}]
        )

        df = pl.DataFrame({"season": ["2024-25", "2024-25"], "col_a": [1, 2]})

        with patch("scripts.daily_bronze_update.get_table_columns", return_value={"season", "col_a"}):
            upload_table(mock_client, "bronze_fpl_players", df)

        # Verify delete was called with .eq("season", "2024-25")
        mock_client.table("bronze_fpl_players").delete.assert_called_once()
        delete_chain.eq.assert_called_with("season", "2024-25")

    def test_cli_fallback_rejects_invalid_season(self):
        """CLI fallback should reject invalid season formats."""
        import subprocess
        from unittest.mock import MagicMock, patch

        from scripts.daily_bronze_update import upload_table

        mock_client, delete_chain = self._make_mock_supabase()

        # Make the REST API delete fail so we hit the CLI fallback
        mock_client.table.return_value.delete.return_value.execute.side_effect = Exception("API error")
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"season": "2024-25"}]
        )

        # Create a DataFrame with a suspicious "season" value
        df = pl.DataFrame({"season": ["2024-25; DROP TABLE users;--"], "col_a": [1]})

        with patch("scripts.daily_bronze_update.get_table_columns", return_value={"season", "col_a"}):
            with patch("subprocess.run") as mock_run:
                upload_table(mock_client, "bronze_fpl_players", df)

        # subprocess.run should NOT have been called because the season is invalid
        mock_run.assert_not_called()

    def test_cli_fallback_accepts_valid_season(self):
        """CLI fallback should execute for valid season formats."""
        import os
        import subprocess
        from unittest.mock import MagicMock, patch

        from scripts.daily_bronze_update import upload_table

        mock_client, delete_chain = self._make_mock_supabase()

        # Make the REST API delete fail so we hit the CLI fallback
        mock_client.table.return_value.delete.return_value.execute.side_effect = Exception("API error")
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"season": "2024-25"}]
        )

        df = pl.DataFrame({"season": ["2024-25"], "col_a": [1]})

        with patch("scripts.daily_bronze_update.get_table_columns", return_value={"season", "col_a"}):
            with patch("subprocess.run") as mock_run:
                with patch.dict(os.environ, {"SUPABASE_ACCESS_TOKEN": "fake-token"}):
                    upload_table(mock_client, "bronze_fpl_players", df)

        # subprocess.run should have been called with the SQL delete
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert "DELETE FROM bronze_fpl_players WHERE season = '2024-25'" in cmd[-1]

    def test_empty_dataframe_skips_delete(self):
        """Empty DataFrames should not trigger any delete."""
        from unittest.mock import MagicMock, patch

        from scripts.daily_bronze_update import upload_table

        mock_client = MagicMock()
        df = pl.DataFrame()

        with patch("scripts.daily_bronze_update.get_table_columns", return_value=set()):
            result = upload_table(mock_client, "bronze_fpl_players", df)

        assert result == 0
        # Should not call delete at all on empty dataframe
        mock_client.table.return_value.delete.assert_not_called()
