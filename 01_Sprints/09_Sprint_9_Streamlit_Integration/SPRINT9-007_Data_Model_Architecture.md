# Ticket: SPRINT9-007 - Data Model Architecture Decision & Implementation

## Description
Determine and implement the optimal data model for the Streamlit application. We need to decide between keeping the flat `features.parquet` table (generated in Sprint 3) or refactoring into a Star Schema (Fact/Dim tables) for better query performance and modularity.

## Technical Requirements
- **Performance Evaluation:**
  - Test loading times and query speeds of the flat `features.parquet` table in Streamlit.
  - Assess memory usage with 300k+ rows and 150+ columns.
- **Architecture Decision:**
  - **Option A (Flat Table):** Keep `features.parquet` if performance is acceptable. Simpler implementation.
  - **Option B (Star Schema):** Refactor into `dim_player`, `dim_team`, `fact_player_gw`, `fact_player_h2h` if performance is poor or app becomes complex.
- **Implementation (if Star Schema chosen):**
  - Create dimension tables (`dim_player`, `dim_team`, `dim_fixture`).
  - Create fact tables (`fact_player_gw`, `fact_player_h2h`, `fact_team_match`).
  - Implement efficient joins for Streamlit pages.
  - Ensure caching strategy works with the new model.

## Acceptance Criteria
- [ ] Performance benchmarks completed for flat table vs. Star Schema.
- [ ] Architecture decision documented.
- [ ] Data model implemented (either flat or Star Schema).
- [ ] Streamlit pages load within <2 seconds.
- [ ] Caching strategy updated for the chosen model.

## Blockers
- **Blocked by:** Performance testing results from Sprint 3 & 6.
- **Decision needed:** Team consensus on architecture based on benchmarks.

## Agent
build

## Status
🚫 Blocked

## Progress Log

## Review Failures
[None yet]

## Comments
- **Note:** Defer this decision until Sprint 3 features are generated and Sprint 6 models are trained.
- **Hypothesis:** Polars + Parquet on a flat table will likely be fast enough for <500k rows, avoiding the complexity of a Star Schema.
