"""Demo script to show first results of ML modules."""

from __future__ import annotations

import numpy as np
import polars as pl

from src.models.lightgbm_model import (
    predict_points,
    train_player_model,
)
from src.models.model_evaluation import evaluate_model
from src.models.starting_xi import (
    predict_start_probability,
    train_starting_xi_model,
)
from src.models.xi_model_evaluation import evaluate_xi_model


def generate_synthetic_data(n_samples: int = 1000) -> pl.DataFrame:
    """Generate realistic synthetic FPL data for demonstration."""
    np.random.seed(42)

    data = {
        "player_id": np.random.randint(1, 100, n_samples),
        "gameweek": np.random.randint(1, 38, n_samples),
        "total_points": np.random.poisson(5, n_samples),
        "minutes": np.random.choice(
            [0, 15, 45, 60, 90], n_samples, p=[0.1, 0.1, 0.1, 0.2, 0.5]
        ),
        "xg": np.abs(np.random.normal(0.3, 0.4, n_samples)),
        "xa": np.abs(np.random.normal(0.2, 0.3, n_samples)),
        "ict_index": np.abs(np.random.normal(50, 20, n_samples)),
        "bonus": np.random.choice([0, 1, 2, 3], n_samples, p=[0.6, 0.2, 0.15, 0.05]),
        "shots": np.random.poisson(2, n_samples),
        "key_passes": np.random.poisson(1, n_samples),
        "clean_sheets": np.random.choice([0, 1], n_samples, p=[0.7, 0.3]),
        "goals_conceded": np.random.poisson(1, n_samples),
        "saves": np.random.poisson(3, n_samples),
        "influence": np.abs(np.random.normal(40, 15, n_samples)),
        "creativity": np.abs(np.random.normal(30, 15, n_samples)),
        "threat": np.abs(np.random.normal(40, 25, n_samples)),
    }
    return pl.DataFrame(data)


def main() -> None:
    """Run the demo."""
    print("=" * 60)
    print("FPL ML MODULE DEMO")
    print("=" * 60)

    # 1. Generate Data
    print("\n1. Generating synthetic data...")
    df = generate_synthetic_data(2000)
    print(f"   Generated {df.shape[0]} rows with {df.shape[1]} features.")

    # 2. Prepare Features
    print("\n2. Preparing features...")
    target_col = "total_points"
    xi_target_col = "minutes"

    # Performance Model Features
    perf_feature_cols = [
        c for c in df.columns if c not in ("player_id", "gameweek", target_col)
    ]
    df_perf = df.drop_nulls(subset=perf_feature_cols + [target_col])
    X_perf = df_perf.select(perf_feature_cols).to_numpy()
    y_perf = df_perf.select(target_col).to_numpy().ravel()

    # XI Model Features
    xi_feature_cols = [
        c
        for c in df.columns
        if c not in ("player_id", "gameweek", xi_target_col, target_col)
    ]
    df_xi = df.drop_nulls(subset=xi_feature_cols + [xi_target_col])
    X_xi = df_xi.select(xi_feature_cols).to_numpy()
    y_xi = (df_xi.select(xi_target_col).to_numpy().ravel() >= 60).astype(np.int64)

    # Split Data (80/20)
    split_idx = int(len(X_perf) * 0.8)
    X_train_p, X_test_p = X_perf[:split_idx], X_perf[split_idx:]
    y_train_p, y_test_p = y_perf[:split_idx], y_perf[split_idx:]

    X_train_x, X_test_x = X_xi[:split_idx], X_xi[split_idx:]
    y_train_x, y_test_x = y_xi[:split_idx], y_xi[split_idx:]

    # 3. Train Performance Model
    print("\n3. Training Player Performance Model...")
    perf_model, perf_info = train_player_model(
        X_train_p, y_train_p, log_to_mlflow=False
    )
    print(f"   Trained in {perf_info['training_time_seconds']:.2f}s")
    print(f"   Best iteration: {perf_info['best_iteration']}")

    # 4. Evaluate Performance Model
    print("\n4. Evaluating Performance Model...")
    perf_results = evaluate_model(
        perf_model,
        X_test_p,
        y_test_p,
        feature_names=perf_feature_cols,
        log_to_mlflow=False,
    )
    print("   Metrics:")
    for k, v in perf_results["metrics"].items():
        print(f"     - {k}: {v:.4f}")
    print("   Top 5 Features:")
    for feat, imp in list(perf_results["feature_importance"].items())[:5]:
        print(f"     - {feat}: {imp:.2f}")

    # 5. Train XI Model
    print("\n5. Training Starting XI Predictor...")
    xi_model, xi_info = train_starting_xi_model(
        X_train_x, y_train_x, log_to_mlflow=False
    )
    print(f"   Trained in {xi_info['training_time_seconds']:.2f}s")
    print(f"   Best iteration: {xi_info['best_iteration']}")

    # 6. Evaluate XI Model
    print("\n6. Evaluating XI Predictor...")
    xi_results = evaluate_xi_model(
        xi_model, X_test_x, y_test_x, feature_names=xi_feature_cols, log_to_mlflow=False
    )
    print("   Metrics:")
    for k, v in xi_results["metrics"].items():
        print(f"     - {k}: {v:.4f}")
    print("   Confusion Matrix:")
    cm = xi_results["confusion_matrix"]
    print(f"     - TP: {cm['true_positives']}, TN: {cm['true_negatives']}")
    print(f"     - FP: {cm['false_positives']}, FN: {cm['false_negatives']}")

    # 7. Sample Predictions
    print("\n7. Sample Predictions (Next 5 Players):")
    print(f"   {'Player ID':<12} {'Pred Points':<15} {'Start Prob':<15}")
    print("   " + "-" * 40)
    for i in range(5):
        idx = split_idx + i
        pid = df["player_id"][idx]
        pred_pts = predict_points(perf_model, X_perf[idx : idx + 1])[0]
        pred_start = predict_start_probability(xi_model, X_xi[idx : idx + 1])[0]
        print(f"   {pid:<12} {pred_pts:<15.2f} {pred_start:<15.2%}")

    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
