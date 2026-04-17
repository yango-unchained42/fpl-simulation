# Ticket: SPRINT6-002 - LightGBM Model Implementation

## Description

Implement LightGBM model architecture and training pipeline.

## Technical Requirements

- Create LightGBM model class with proper initialization and methods
- Implement custom loss function if needed for FPL distribution characteristics
- Build training pipeline with proper data loading and preprocessing
- Implement early stopping to prevent overfitting
- Add model serialization/deserialization capabilities
- Create prediction pipeline for both training and inference
- Use MLflow for local development tracking only
- Save model as .joblib for deployment

## Acceptance Criteria

- [ ] LightGBM model class implemented with all required methods
- [ ] Custom loss function implemented if needed for FPL distribution
- [ ] Training pipeline with proper data loading and preprocessing
- [ ] Early stopping implemented and tested
- [ ] Model serialization (save) and deserialization (load) working
- [ ] Prediction pipeline implemented for both training and inference
- [ ] MLflow used for local development tracking only
- [ ] Model saved as .joblib for deployment
- [ ] Model documentation updated with usage examples

## Definition of Done

- [ ] LightGBM model class implemented
- [ ] Custom loss function (if needed for FPL distribution)
- [ ] Training pipeline with proper data loading
- [ ] Early stopping implementation
- [ ] Model serialization/deserialization
- [ ] Prediction pipeline (training → prediction)
- [ ] MLflow used for local development only
- [ ] Model saved as .joblib for deployment
- [ ] Documentation updated

## Agent
build

## Status
Done

## Progress Log

### 2026-04-06 — Implementation started
Picking up SPRINT6-002. Will implement LightGBM model class with early stopping, serialization, MLflow logging, and prediction pipeline.

### 2026-04-06 — Implementation complete
Created `src/models/lightgbm_model.py` with:
- `train_player_model()` — LightGBM training with early stopping (50 rounds), validation set support, custom hyperparameters
- `predict_points()` — prediction with clipping to >= 0 (FPL points can't be negative)
- `save_model()` / `load_model()` — joblib serialization + feature names persistence
- `get_feature_importance()` — top-N feature importance extraction (gain-based)
- `_log_training_to_mlflow()` — logs params, hyperparams, and feature importance to MLflow

Created `tests/test_lightgbm_model.py` with 9 tests:
- `TestTrainPlayerModel` (3): basic training, training with validation, custom params
- `TestPredictPoints` (2): basic prediction, negative clipping
- `TestSaveLoadModel` (2): save/load roundtrip, missing model error
- `TestFeatureImportance` (1): top-N features, sorted by importance
- `TestLogsToMlflow` (1): MLflow logging during training

Coverage: 93% on `lightgbm_model.py`. All 9 tests passing.

### 2026-04-06 14:00:00 — Review fixes applied
- Removed unused `import polars as pl` from `lightgbm_model.py`
- Renamed all arguments to snake_case: `X_train` → `x_train`, `X_val` → `x_val`, `X` → `x`
- Fixed return type annotation in `predict_points()` with proper `np.asarray()` and `.astype(np.float64)`
- All 41 Sprint 6 tests passing, ruff clean, mypy clean

### 2026-04-06 15:00:00 — Final Re-review
**Tests:** 369/369 passing ✓
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓
**All acceptance criteria met** ✓

### 2026-04-06 15:00:00 Quality review passed. All checks green. Ticket closed.

## Review Failures

### 2026-04-06 14:00:00 — Quality Review
**1. Unused import `polars`** — ✅ Fixed: removed `import polars as pl`
**2. Argument naming convention violations (N803)** — ✅ Fixed: renamed to `x_train`, `x_val`, `x`
**3. MyPy type errors** — ✅ Fixed: proper type annotation with `np.asarray()` and `.astype(np.float64)`

## Comments
[Agents can add questions, blockers, or notes here]
