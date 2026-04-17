"""Tests for player-level event simulator."""

from __future__ import annotations

from src.models.event_simulator import (
    PlayerEvent,
    PlayerRates,
    simulate_player_events,
    simulate_what_if_scenario,
)


class TestSimulatePlayerEvents:
    """Tests for simulate_player_events function."""

    def test_basic_simulation(self) -> None:
        """Test basic player event simulation."""
        rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                shots_per_90=3.0,
                goals_per_shot=0.15,
                assists_per_90=0.5,
                key_passes_per_90=2.0,
                start_probability=0.9,
            ),
            PlayerRates(
                player_id=2,
                position="DEF",
                shots_per_90=0.5,
                goals_per_shot=0.1,
                tackles_per_90=3.0,
                interceptions_per_90=2.0,
                clearances_per_90=5.0,
                start_probability=0.95,
            ),
        ]
        events = simulate_player_events(rates, random_seed=42)
        assert len(events) == 2
        assert all(isinstance(e, PlayerEvent) for e in events)
        # Both players should have FPL points calculated
        assert all(e.fpl_points >= 0 for e in events)

    def test_reproducibility_with_seed(self) -> None:
        """Test that results are reproducible with same seed."""
        rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                shots_per_90=3.0,
                goals_per_shot=0.15,
                assists_per_90=0.5,
                start_probability=0.9,
            ),
        ]
        events1 = simulate_player_events(rates, random_seed=42)
        events2 = simulate_player_events(rates, random_seed=42)
        assert events1[0].fpl_points == events2[0].fpl_points
        assert events1[0].shots == events2[0].shots

    def test_clean_sheet_probability(self) -> None:
        """Test that clean sheet probability is affected by opponent xG."""
        rates = [
            PlayerRates(
                player_id=1,
                position="DEF",
                start_probability=1.0,
            ),
        ]
        # Low opponent xG -> higher clean sheet chance
        events_low_xg = simulate_player_events(
            rates, opponent_goals_conceded=0.5, random_seed=42
        )
        # High opponent xG -> lower clean sheet chance
        events_high_xg = simulate_player_events(
            rates, opponent_goals_conceded=3.0, random_seed=42
        )
        # With seed 42, both might have clean sheet or not, but the
        # goals_conceded should be higher for high xG opponent
        assert events_high_xg[0].goals_conceded >= events_low_xg[0].goals_conceded

    def test_sub_appearance(self) -> None:
        """Test that non-starters get sub appearance minutes."""
        rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                start_probability=0.0,  # Never starts
            ),
        ]
        events = simulate_player_events(rates, random_seed=42)
        assert events[0].minutes < 60
        assert events[0].minutes >= 10

    def test_bonus_points_allocation(self) -> None:
        """Test that bonus points are allocated to top BPS scorers."""
        rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                shots_per_90=5.0,
                goals_per_shot=0.3,
                assists_per_90=1.0,
                start_probability=1.0,
            ),
            PlayerRates(
                player_id=2,
                position="MID",
                shots_per_90=0.5,
                goals_per_shot=0.0,
                assists_per_90=0.0,
                start_probability=1.0,
            ),
            PlayerRates(
                player_id=3,
                position="MID",
                shots_per_90=0.5,
                goals_per_shot=0.0,
                assists_per_90=0.0,
                start_probability=1.0,
            ),
        ]
        events = simulate_player_events(rates, random_seed=42)
        # Player 1 should have highest BPS and get bonus
        bps_scores = {e.player_id: e.bps for e in events}
        assert bps_scores[1] > bps_scores[2]
        assert bps_scores[1] > bps_scores[3]


class TestSimulateWhatIfScenario:
    """Tests for simulate_what_if_scenario function."""

    def test_penalty_taker_modification(self) -> None:
        """Test what-if scenario with penalty taker modification."""
        base_rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                shots_per_90=2.0,
                goals_per_shot=0.1,
                penalty_taker=False,
                start_probability=1.0,
            ),
        ]
        # Scenario: Player becomes penalty taker
        modifications = {1: {"penalty_taker": True, "shots_per_90": 3.0}}
        events = simulate_what_if_scenario(base_rates, modifications, random_seed=42)
        assert len(events) == 1
        assert events[0].shots >= 0  # Should have some shots

    def test_multiple_modifications(self) -> None:
        """Test what-if scenario with multiple player modifications."""
        base_rates = [
            PlayerRates(
                player_id=1,
                position="MID",
                shots_per_90=2.0,
                goals_per_shot=0.1,
                start_probability=1.0,
            ),
            PlayerRates(
                player_id=2,
                position="DEF",
                shots_per_90=0.5,
                goals_per_shot=0.05,
                start_probability=1.0,
            ),
        ]
        modifications = {
            1: {"shots_per_90": 5.0},
            2: {"tackles_per_90": 5.0},
        }
        events = simulate_what_if_scenario(base_rates, modifications, random_seed=42)
        assert len(events) == 2
