# Sprint 7: XI Predictor Model

## Overview

**Sprint Goal:** Develop model to predict starting XI probability for each player.

**Duration:** 1 week

**Focus Areas:**
- Binary classification model for starting probability
- Feature engineering for XI prediction
- Model training and evaluation
- Probability calibration

---

## Tickets

### SPRINT7-001: XI Prediction Feature Engineering
**Status:** Awaiting Development  
**Priority:** High  
**Labels:** `feature-engineering` `xi-prediction`

**Description:**
Prepare features and target variable for XI prediction model.

**Definition of Done:**
- [ ] Target variable created (binary: 1 = started, 0 = did not start)
- [ ] Features adapted for XI prediction (similar to performance model)
- [ ] Position-specific features considered
- [ ] Recent form features emphasized (form players more likely to start)
- [ ] Training/validation/test split created
- [ ] Class imbalance addressed (if needed)
- [ ] Dataset saved to Supabase
- [ ] Feature store updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT7-002: XI Classification Model Implementation
**Status:** Awaiting Development  
**Priority:** High  
**Labels:** `model` `classification` `xi-prediction`

**Description:**
Implement LightGBM classifier for XI probability prediction.

**Definition of Done:**
- [ ] LightGBM classifier implemented (binary classification)
- [ ] Probability calibration implemented (Platt scaling or isotonic)
- [ ] Training pipeline with class weights (if imbalanced)
- [ ] Early stopping implemented
- [ ] Model serialization/deserialization
- [ ] Prediction pipeline for probability output
- [ ] Model logged to MLflow (local dev only)
- [ ] Model registered in MLflow Model Registry
- [ ] Model serialized as .joblib file
- [ ] Documentation updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT7-003: XI Model Evaluation
**Status:** Awaiting Development  
**Priority:** High  
**Labels:** `evaluation` `xi-prediction`

**Description:**
Evaluate XI prediction model with classification metrics.

**Definition of Done:**
- [ ] Test set evaluation completed
- [ ] Metrics calculated: Accuracy, Precision, Recall, F1-Score, ROC-AUC
- [ ] Confusion matrix generated
- [ ] ROC curve and PR curve plotted
- [ ] Probability calibration curve
- [ ] Feature importance analysis
- [ ] SHAP values for interpretability
- [ ] Results logged to MLflow (local dev only)
- [ ] Model registered in MLflow Model Registry
- [ ] Documentation updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT7-004: XI Model Testing
**Status:** Awaiting Development  
**Priority:** High  
**Labels:** `testing` `xi-prediction`

**Description:**
Comprehensive testing for XI prediction model pipeline.

**Definition of Done:**
- [ ] Unit tests for feature engineering
- [ ] Unit tests for model training pipeline
- [ ] Unit tests for evaluation metrics
- [ ] Integration test for complete pipeline
- [ ] Test for probability calibration
- [ ] Test coverage >80%
- [ ] All tests passing

**Assignee:** @TestSpecialist  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT7-005: XI Model Review
**Status:** Awaiting Review  
**Priority:** High  
**Labels:** `review` `xi-prediction`

**Description:**
Project Owner review of XI prediction model performance.

**Definition of Done:**
- [ ] Model performance reviewed (AUC, F1-Score)
- [ ] Probability calibration validated
- [ ] Feature importance reviewed
- [ ] Model registered in MLflow Model Registry
- [ ] Model approved for production use
- [ ] Documentation approved

**Assignee:** @ProjectOwner  
**Status:** Awaiting Review  
**Comments:**

---

## Sprint Summary

**Total Tickets:** 5  
**Development Tickets:** 3  
**Testing Tickets:** 1  
**Review Tickets:** 1

**Expected Outcomes:**
- XI prediction model with calibrated probabilities
- Comprehensive evaluation with classification metrics
- Model ready for integration with performance model
- Full experiment tracking in MLflow (local dev only)
- Model registered in MLflow Model Registry
