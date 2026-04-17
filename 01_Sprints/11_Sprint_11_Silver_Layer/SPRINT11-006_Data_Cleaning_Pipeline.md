# Ticket: SPRINT11-006 - Data Cleaning Pipeline

## Description
Implement data cleaning and validation for Silver layer tables to ensure data quality before ML processing.

## Technical Requirements
- Validate data integrity:
  - Check for duplicate player-gameweek-season records
  - Verify foreign key relationships exist
  - Flag missing required fields
  - Validate data types match schema

- Handle outliers:
  - Suspicious goal totals (e.g., >5 goals in one game)
  - Impossible stats (e.g., assists > goals)
  - Negative values where not allowed

- Data quality scoring:
  - Calculate completeness score per record
  - Flag records with missing critical fields
  - Track data quality over time

- Position validation:
  - Verify positions match player types (GKP has saves, etc.)
  - Flag invalid position-stat combinations

## Acceptance Criteria
- [ ] Duplicate detection implemented
- [ ] Outlier detection implemented
- [ ] Data quality scoring implemented
- [ ] Position validation implemented
- [ ] Error/warning logging for data issues

## Definition of Done
- [ ] Code in `src/silver/data_quality.py`
- [ ] Unit tests >80%
- [ ] Integration tests passing
- [ ] All checks passing
- [ ] Run on all Silver tables and report quality metrics

## Agent
build

## Status
Pending

## Progress Log
