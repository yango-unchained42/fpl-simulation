"""Player performance prediction model (LightGBM).

Primary model: predicts FPL points, goals, assists, clean sheets
using LightGBM regressor/classifier.
"""

from __future__ import annotations

import logging
from pathlib import Path

import lightgbm as lgb
import numpy as np
import numpy.typing as npt
import polars as pl

logger = logging.getLogger(__name__)

MODEL_PATH = Path("data/models/player_predictor.joblib")


def prepare_features(
    df: pl.DataFrame,
    target_col: str = "points",
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], list[str]]:
    """Prepare feature matrix and target vector.

    Args:
        df: DataFrame with features and target.
        target_col: Column name for the prediction target.

    Returns:
        Tuple of (X, y, feature_names).
    """
    feature_cols = [
        c
        for c in df.columns
        if c != target_col
        and c
        not in (
            "player_id",
            "gameweek",
            "fixture_id",
            "name",
        )
    ]
    df_clean = df.drop_nulls(subset=feature_cols + [target_col])
    x_arr = df_clean.select(feature_cols).to_numpy()
    y_arr = df_clean.select(target_col).to_numpy().ravel()
    return x_arr, y_arr, feature_cols


def train_player_model(
    x_train: npt.NDArray[np.float64],
    y_train: npt.NDArray[np.float64],
) -> lgb.LGBMRegressor:
    """Train the LightGBM player performance model.

    Args:
        x_train: Feature matrix.
        y_train: Target vector.

    Returns:
        Trained LightGBM regressor.
    """
    model = lgb.LGBMRegressor(
        n_estimators=500,
        max_depth=10,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )
    model.fit(x_train, y_train)
    return model


def predict_points(
    model: lgb.LGBMRegressor,
    x: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Predict FPL points for given features.

    Args:
        model: Trained LightGBM model.
        x: Feature matrix.

    Returns:
        Array of predicted points.
    """
    return model.predict(x)  # type: ignore[return-value]
