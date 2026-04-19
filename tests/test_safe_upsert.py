"""Tests for src/utils/safe_upsert.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.utils.safe_upsert import (
    clean_records_for_upload,
    deduplicate_by_key,
    load_existing_keys,
    safe_upsert,
)


class TestDeduplicateByKey:
    """Tests for deduplicate_by_key function."""

    def test_empty_records(self) -> None:
        result = deduplicate_by_key([], ["season", "fpl_id"])
        assert result == []

    def test_no_duplicates(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "name": "Salah"},
            {"season": "2024-25", "fpl_id": 2, "name": "Saka"},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id"])
        assert len(result) == 2

    def test_duplicates_keep_last_without_score(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "name": "Salah", "confidence": 0.5},
            {"season": "2024-25", "fpl_id": 1, "name": "Salah", "confidence": 0.9},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id"])
        assert len(result) == 1
        assert result[0]["confidence"] == 0.9

    def test_duplicates_keep_highest_score(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "data_quality_score": 0.5},
            {"season": "2024-25", "fpl_id": 1, "data_quality_score": 0.9},
            {"season": "2024-25", "fpl_id": 1, "data_quality_score": 0.7},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id"], score_column="data_quality_score")
        assert len(result) == 1
        assert result[0]["data_quality_score"] == 0.9

    def test_skips_records_with_null_keys(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "name": "Salah"},
            {"season": None, "fpl_id": 2, "name": "Saka"},
            {"season": "2024-25", "fpl_id": None, "name": "Haaland"},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id"])
        assert len(result) == 1
        assert result[0]["fpl_id"] == 1

    def test_multiple_keys(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "vaastav_id": 10, "score": 0.8},
            {"season": "2024-25", "fpl_id": 1, "vaastav_id": 10, "score": 0.6},
            {"season": "2025-26", "fpl_id": 1, "vaastav_id": 10, "score": 0.9},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id", "vaastav_id"], score_column="score")
        assert len(result) == 2

    def test_score_column_none_treated_as_zero(self) -> None:
        records = [
            {"season": "2024-25", "fpl_id": 1, "score": None},
            {"season": "2024-25", "fpl_id": 1, "score": 0.5},
        ]
        result = deduplicate_by_key(records, ["season", "fpl_id"], score_column="score")
        assert len(result) == 1
        assert result[0]["score"] == 0.5

    def test_single_key_column(self) -> None:
        records = [
            {"id": 1, "name": "Salah"},
            {"id": 1, "name": "Mohamed Salah"},
        ]
        result = deduplicate_by_key(records, ["id"])
        assert len(result) == 1
        assert result[0]["name"] == "Mohamed Salah"


class TestCleanRecordsForUpload:
    """Tests for clean_records_for_upload function."""

    def test_removes_default_columns(self) -> None:
        records = [
            {"id": 1, "name": "Salah", "created_at": "2024-01-01", "updated_at": "2024-01-02"},
        ]
        result = clean_records_for_upload(records)
        assert len(result) == 1
        assert "id" not in result[0]
        assert "created_at" not in result[0]
        assert "updated_at" not in result[0]
        assert result[0]["name"] == "Salah"

    def test_removes_custom_columns(self) -> None:
        records = [{"id": 1, "name": "Salah", "temp_col": "x"}]
        result = clean_records_for_upload(records, exclude_columns=["temp_col"])
        assert "temp_col" not in result[0]
        assert result[0]["id"] == 1

    def test_removes_none_values(self) -> None:
        records = [
            {"name": "Salah", "team": None, "position": "MID"},
        ]
        result = clean_records_for_upload(records)
        assert "team" not in result[0]
        assert result[0]["name"] == "Salah"
        assert result[0]["position"] == "MID"

    def test_empty_records(self) -> None:
        result = clean_records_for_upload([])
        assert result == []

    def test_preserves_non_none_values(self) -> None:
        records = [
            {"name": "Salah", "team": "Liverpool", "position": "MID", "score": 0},
        ]
        result = clean_records_for_upload(records)
        assert result[0]["score"] == 0
        assert result[0]["team"] == "Liverpool"


class TestLoadExistingKeys:
    """Tests for load_existing_keys function."""

    @patch("src.utils.safe_upsert.fetch_all_paginated")
    def test_returns_key_set(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [
            {"season": "2024-25", "fpl_id": 1},
            {"season": "2024-25", "fpl_id": 2},
        ]
        client = MagicMock()
        result = load_existing_keys(client, "silver_player_mapping", ["season", "fpl_id"])
        assert result == {("2024-25", 1), ("2024-25", 2)}

    @patch("src.utils.safe_upsert.fetch_all_paginated")
    def test_skips_null_keys(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [
            {"season": "2024-25", "fpl_id": 1},
            {"season": None, "fpl_id": 2},
        ]
        client = MagicMock()
        result = load_existing_keys(client, "table", ["season", "fpl_id"])
        assert result == {("2024-25", 1)}

    @patch("src.utils.safe_upsert.fetch_all_paginated")
    def test_with_season_filter(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [{"season": "2024-25", "fpl_id": 1}]
        client = MagicMock()
        load_existing_keys(client, "table", ["season", "fpl_id"], season="2024-25")
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args
        assert call_kwargs[1]["filters"] == {"season": "2024-25"}

    @patch("src.utils.safe_upsert.fetch_all_paginated")
    def test_empty_table(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = []
        client = MagicMock()
        result = load_existing_keys(client, "table", ["season", "fpl_id"])
        assert result == set()


class TestSafeUpsert:
    """Tests for safe_upsert function."""

    def test_empty_records(self) -> None:
        client = MagicMock()
        result = safe_upsert(client, "table", [], ["season", "fpl_id"])
        assert result == 0

    @patch("src.utils.safe_upsert.BATCH_SIZE", 2)
    def test_upserts_in_batches(self) -> None:
        client = MagicMock()
        records = [
            {"season": "2024-25", "fpl_id": 1, "name": "A"},
            {"season": "2024-25", "fpl_id": 2, "name": "B"},
            {"season": "2024-25", "fpl_id": 3, "name": "C"},
        ]
        result = safe_upsert(client, "table", records, ["season", "fpl_id"])
        assert result == 3
        assert client.table.return_value.upsert.call_count == 2  # 3 records / batch_size 2 = 2 batches

    @patch("src.utils.safe_upsert.BATCH_SIZE", 10)
    def test_batch_failure_does_not_stop(self) -> None:
        client = MagicMock()
        client.table.return_value.upsert.return_value.execute.side_effect = Exception("fail")

        records = [{"season": "2024-25", "fpl_id": 1, "name": "A"}]
        result = safe_upsert(client, "table", records, ["season", "fpl_id"])
        assert result == 0  # batch failed, so 0 written

    @patch("src.utils.safe_upsert.BATCH_SIZE", 10)
    def test_deduplication_applied(self) -> None:
        client = MagicMock()
        records = [
            {"season": "2024-25", "fpl_id": 1, "data_quality_score": 0.5},
            {"season": "2024-25", "fpl_id": 1, "data_quality_score": 0.9},
        ]
        result = safe_upsert(
            client, "table", records, ["season", "fpl_id"],
            score_column="data_quality_score",
        )
        assert result == 1  # deduped to 1 record

    @patch("src.utils.safe_upsert.fetch_all_paginated")
    @patch("src.utils.safe_upsert.BATCH_SIZE", 10)
    def test_skip_existing(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [{"season": "2024-25", "fpl_id": 1}]
        client = MagicMock()
        records = [
            {"season": "2024-25", "fpl_id": 1, "name": "A"},  # exists, skip
            {"season": "2024-25", "fpl_id": 2, "name": "B"},  # new, upload
        ]
        result = safe_upsert(
            client, "table", records, ["season", "fpl_id"],
            skip_existing=True,
        )
        assert result == 1
        # Only 1 record should have been upserted
        upserted_batch = client.table.return_value.upsert.call_args[0][0]
        assert len(upserted_batch) == 1
        assert upserted_batch[0]["fpl_id"] == 2

    @patch("src.utils.safe_upsert.BATCH_SIZE", 10)
    def test_partial_batch_failure(self) -> None:
        """Test that only successful batches count toward written total."""
        client = MagicMock()
        call_count = 0

        def upsert_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            if call_count == 1:
                mock.execute.return_value = None  # success
            else:
                mock.execute.side_effect = Exception("batch 2 failed")
            return mock

        client.table.return_value.upsert.side_effect = upsert_side_effect

        with patch("src.utils.safe_upsert.BATCH_SIZE", 2):
            records = [
                {"season": "2024-25", "fpl_id": 1, "name": "A"},
                {"season": "2024-25", "fpl_id": 2, "name": "B"},
                {"season": "2024-25", "fpl_id": 3, "name": "C"},
            ]
            result = safe_upsert(client, "table", records, ["season", "fpl_id"])
            assert result == 2  # only first batch succeeded
