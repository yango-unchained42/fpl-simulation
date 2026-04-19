"""Tests for src/utils/safe_upsert.py module."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.utils.safe_upsert import (
    clean_records_for_upload,
    deduplicate_by_key,
    safe_upsert,
    truncate_table,
)


# ── deduplicate_by_key ──────────────────────────────────────────────────────


class TestDeduplicateByKey:
    def test_basic_dedup_keeps_last(self):
        records = [
            {"season": "2024-25", "player_id": 1, "name": "A"},
            {"season": "2024-25", "player_id": 1, "name": "B"},
            {"season": "2024-25", "player_id": 2, "name": "C"},
        ]
        result = deduplicate_by_key(records, ["season", "player_id"])
        assert len(result) == 2
        # Player 1 should keep the last record
        p1 = [r for r in result if r["player_id"] == 1][0]
        assert p1["name"] == "B"

    def test_dedup_with_score_column(self):
        records = [
            {"id": 1, "score": 0.5, "val": "low"},
            {"id": 1, "score": 0.9, "val": "high"},
            {"id": 1, "score": 0.3, "val": "lowest"},
        ]
        result = deduplicate_by_key(records, ["id"], score_column="score")
        assert len(result) == 1
        assert result[0]["val"] == "high"

    def test_dedup_skips_none_keys(self):
        records = [
            {"season": "2024-25", "player_id": None, "name": "A"},
            {"season": None, "player_id": 1, "name": "B"},
            {"season": "2024-25", "player_id": 2, "name": "C"},
        ]
        result = deduplicate_by_key(records, ["season", "player_id"])
        assert len(result) == 1
        assert result[0]["player_id"] == 2

    def test_dedup_empty_list(self):
        result = deduplicate_by_key([], ["season", "player_id"])
        assert result == []

    def test_dedup_no_duplicates(self):
        records = [
            {"season": "2024-25", "id": 1},
            {"season": "2024-25", "id": 2},
            {"season": "2023-24", "id": 1},
        ]
        result = deduplicate_by_key(records, ["season", "id"])
        assert len(result) == 3

    def test_dedup_score_with_none_values(self):
        records = [
            {"id": 1, "score": None, "val": "none_score"},
            {"id": 1, "score": 0.8, "val": "has_score"},
        ]
        result = deduplicate_by_key(records, ["id"], score_column="score")
        assert len(result) == 1
        assert result[0]["val"] == "has_score"


# ── clean_records_for_upload ────────────────────────────────────────────────


class TestCleanRecordsForUpload:
    def test_removes_default_columns(self):
        records = [
            {"name": "Test", "created_at": "2024-01-01", "updated_at": "2024-01-02", "id": 123, "value": 10},
        ]
        result = clean_records_for_upload(records)
        assert len(result) == 1
        assert "created_at" not in result[0]
        assert "updated_at" not in result[0]
        assert "id" not in result[0]
        assert result[0]["name"] == "Test"
        assert result[0]["value"] == 10

    def test_custom_exclude_columns(self):
        records = [{"a": 1, "b": 2, "c": 3}]
        result = clean_records_for_upload(records, exclude_columns=["b"])
        assert result[0] == {"a": 1, "c": 3}

    def test_removes_none_values(self):
        records = [{"name": "Test", "value": None, "score": 10}]
        result = clean_records_for_upload(records, exclude_columns=[])
        assert "value" not in result[0]
        assert result[0]["score"] == 10

    def test_empty_list(self):
        result = clean_records_for_upload([])
        assert result == []

    def test_preserves_all_when_no_exclusions(self):
        records = [{"a": 1, "b": 2}]
        result = clean_records_for_upload(records, exclude_columns=[])
        assert result[0] == {"a": 1, "b": 2}


# ── truncate_table ──────────────────────────────────────────────────────────


class TestTruncateTable:
    def test_truncate_with_data(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.data = [{"season": "2024-25", "id": 1, "name": "Test"}]
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result

        # Mock the delete chain
        mock_delete = MagicMock()
        mock_not = MagicMock()
        mock_not.is_.return_value = mock_delete
        mock_delete_chain = MagicMock()
        mock_delete_chain.not_ = mock_not
        mock_client.table.return_value.delete.return_value = mock_delete_chain

        truncate_table(mock_client, "test_table")

        mock_client.table.assert_any_call("test_table")
        mock_delete_chain.not_.is_.assert_called_once()

    def test_truncate_empty_table(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.data = []
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result

        truncate_table(mock_client, "empty_table")

        # Should not attempt delete when table is empty
        mock_client.table.return_value.delete.assert_not_called()

    @patch("src.utils.safe_upsert.os.getenv")
    @patch("src.utils.safe_upsert.subprocess.run")
    def test_truncate_fallback_to_cli(self, mock_run, mock_getenv):
        mock_client = MagicMock()
        mock_probe_result = MagicMock()
        mock_probe_result.data = [{"col": "val"}]
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_probe_result
        # Make delete raise an error
        mock_client.table.return_value.delete.side_effect = Exception("Table not found")
        mock_getenv.return_value = "fake-token"

        truncate_table(mock_client, "test_table")

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert "TRUNCATE test_table CASCADE;" in call_args[0][0]


# ── safe_upsert ─────────────────────────────────────────────────────────────


class TestSafeUpsert:
    def test_empty_records_returns_zero(self):
        mock_client = MagicMock()
        result = safe_upsert(mock_client, "test_table", [], ["season", "id"])
        assert result == 0

    def test_basic_upsert(self):
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        records = [
            {"season": "2024-25", "id": 1, "value": 10},
            {"season": "2024-25", "id": 2, "value": 20},
        ]
        result = safe_upsert(
            mock_client, "test_table", records, ["season", "id"]
        )

        assert result == 2
        mock_client.table.return_value.upsert.assert_called_once()

    def test_deduplicates_before_upsert(self):
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        records = [
            {"season": "2024-25", "id": 1, "score": 0.5},
            {"season": "2024-25", "id": 1, "score": 0.9},
            {"season": "2024-25", "id": 2, "score": 0.7},
        ]
        result = safe_upsert(
            mock_client,
            "test_table",
            records,
            ["season", "id"],
            score_column="score",
        )

        assert result == 2
        # Verify the upserted batch has deduplicated records
        upserted = mock_client.table.return_value.upsert.call_args[0][0]
        assert len(upserted) == 2

    @patch("src.utils.safe_upsert.BATCH_SIZE", 2)
    def test_batching(self):
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        records = [
            {"season": "2024-25", "id": i, "value": i} for i in range(5)
        ]
        result = safe_upsert(
            mock_client, "test_table", records, ["season", "id"]
        )

        assert result == 5
        assert mock_client.table.return_value.upsert.call_count == 3  # ceil(5/2)

    @patch("src.utils.safe_upsert.BATCH_SIZE", 2)
    def test_batch_failure_logged_and_continues(self, caplog):
        mock_client = MagicMock()
        call_count = 0

        def upsert_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Network error")
            mock_result = MagicMock()
            mock_result.execute.return_value = MagicMock()
            return mock_result

        mock_client.table.return_value.upsert.side_effect = upsert_side_effect

        records = [
            {"season": "2024-25", "id": 1, "value": 1},
            {"season": "2024-25", "id": 2, "value": 2},
            {"season": "2024-25", "id": 3, "value": 3},
            {"season": "2024-25", "id": 4, "value": 4},
        ]
        with caplog.at_level(logging.ERROR):
            result = safe_upsert(
                mock_client, "test_table", records, ["season", "id"]
            )

        # First batch (2 records) fails, second batch (2 records) succeeds
        assert result == 2
        assert "failed" in caplog.text.lower()

    def test_skip_existing(self):
        mock_client = MagicMock()

        # Mock fetch_all_paginated to return existing keys
        with patch("src.utils.safe_upsert.fetch_all_paginated") as mock_fetch:
            mock_fetch.return_value = [
                {"season": "2024-25", "id": 1},
            ]
            mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock()

            records = [
                {"season": "2024-25", "id": 1, "value": 10},  # existing
                {"season": "2024-25", "id": 2, "value": 20},  # new
            ]
            result = safe_upsert(
                mock_client,
                "test_table",
                records,
                ["season", "id"],
                season="2024-25",
                skip_existing=True,
            )

            assert result == 1
            upserted = mock_client.table.return_value.upsert.call_args[0][0]
            assert len(upserted) == 1
            assert upserted[0]["id"] == 2
