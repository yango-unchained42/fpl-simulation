# Ticket: SPRINT10-002 - Smart Retraining Pipeline Setup

## Description
Set up manual smart retraining pipeline for model updates.

## Technical Requirements
- Create retraining script (trigger full retrain via CLI or Streamlit button)
- Implement model versioning strategy (save models with timestamps in models/ directory)
- Create model validation before deployment (performance threshold: MAE < 1.5 points for points predictor)
- Implement A/B testing framework for model comparison (compare new vs current model on holdout set)
- Create retraining schedule documentation (manual trigger after each gameweek ends)
- Test retraining pipeline
- Use MLflow for local model tracking during retraining
- Save trained models as .joblib files for deployment
- Update model metadata in database (if using Supabase)

## Acceptance Criteria
- [ ] Retraining script created (trigger full retrain)
- [ ] Model versioning strategy implemented
- [ ] Model validation before deployment (performance threshold)
- [ ] A/B testing framework for model comparison
- [ ] Retraining schedule documentation (manual trigger after gameweek ends)
- [ ] Retraining pipeline tested
- [ ] Documentation updated
- [ ] MLflow integration for local tracking
- [ ] .joblib model saving tested
- [ ] Model metadata update tested

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests written and passing (>80% coverage for this component)
- [ ] Type hints added (100% for public APIs)
- [ ] Code reviewed by reviewer
- [ ] Documentation updated
- [ ] Integrated into main application

## Agent
build

## Status
📋 Backlog

## Progress Log

## Comments
[Agents can add questions, blockers, or notes here]
