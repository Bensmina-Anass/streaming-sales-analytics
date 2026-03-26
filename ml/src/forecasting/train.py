import pandas as pd

from ml.configs.paths import (
    FORECASTING_MODELS_DIR,
    METRICS_DIR,
    PREDICTIONS_DIR,
    ensure_project_dirs,
)
from ml.configs.settings import (
    FORECAST_FEATURES,
    FORECAST_TARGET,
    FORECAST_TEST_SIZE,
    RANDOM_STATE,
)
from ml.src.evaluate import evaluate_regression
from ml.src.feature_engineering import build_monthly_sales_features
from ml.src.forecasting.models import get_forecasting_models
from ml.src.model_registry import save_artifact, save_metadata


def time_based_train_test_split(df: pd.DataFrame, test_size: float):
    split_idx = int(len(df) * (1 - test_size))

    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    return train_df, test_df


def train_and_select_best_model():
    ensure_project_dirs()

    df = build_monthly_sales_features().copy()

    train_df, test_df = time_based_train_test_split(df, FORECAST_TEST_SIZE)

    X_train = train_df[FORECAST_FEATURES]
    y_train = train_df[FORECAST_TARGET]

    X_test = test_df[FORECAST_FEATURES]
    y_test = test_df[FORECAST_TARGET]

    models = get_forecasting_models(random_state=RANDOM_STATE)

    metrics_rows = []
    predictions_frames = []

    best_model_name = None
    best_model = None
    best_metrics = None
    best_rmse = float("inf")

    for model_name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        metrics = evaluate_regression(y_test, y_pred)

        metrics_rows.append(
            {
                "model_name": model_name,
                "mae": metrics["mae"],
                "rmse": metrics["rmse"],
                "r2": metrics["r2"],
            }
        )

        pred_df = test_df[
            ["month", "month_date", "revenue", "time_idx", "month_num"]
        ].copy()
        pred_df["model_name"] = model_name
        pred_df["predicted_revenue"] = y_pred
        predictions_frames.append(pred_df)

        if metrics["rmse"] < best_rmse:
            best_rmse = metrics["rmse"]
            best_model_name = model_name
            best_model = model
            best_metrics = metrics

    metrics_df = pd.DataFrame(metrics_rows).sort_values("rmse").reset_index(drop=True)
    all_predictions_df = pd.concat(predictions_frames, ignore_index=True)

    best_predictions_df = all_predictions_df[
        all_predictions_df["model_name"] == best_model_name
    ].copy()

    save_artifact(best_model, FORECASTING_MODELS_DIR / "best_model.joblib")

    metadata = {
        "task": "forecasting",
        "best_model_name": best_model_name,
        "selection_metric": "rmse",
        "mae": best_metrics["mae"],
        "rmse": best_metrics["rmse"],
        "r2": best_metrics["r2"],
        "features": FORECAST_FEATURES,
        "target": FORECAST_TARGET,
        "test_size": FORECAST_TEST_SIZE,
    }
    save_metadata(metadata, FORECASTING_MODELS_DIR / "metadata.json")

    metrics_df.to_csv(METRICS_DIR / "forecasting_model_comparison.csv", index=False)
    all_predictions_df.to_csv(
        PREDICTIONS_DIR / "forecasting_test_predictions_all_models.csv", index=False
    )
    best_predictions_df.to_csv(
        PREDICTIONS_DIR / "forecasting_test_predictions_best_model.csv", index=False
    )

    print("Entraînement forecasting terminé.")
    print("\nComparaison des modèles :")
    print(metrics_df)

    print(f"\nMeilleur modèle : {best_model_name}")
    print(f"RMSE = {best_metrics['rmse']:.2f}")
    print(f"MAE  = {best_metrics['mae']:.2f}")
    print(f"R2   = {best_metrics['r2']:.4f}")

    return {
        "dataframe": df,
        "train_df": train_df,
        "test_df": test_df,
        "metrics_df": metrics_df,
        "best_model_name": best_model_name,
        "best_model": best_model,
        "best_metrics": best_metrics,
    }


if __name__ == "__main__":
    train_and_select_best_model()