import pandas as pd

from ml.configs.paths import (
    MODELS_DIR,
    PREDICTIONS_DIR,
    ensure_project_dirs,
)
from ml.src.feature_engineering import build_daily_sales_features
from ml.src.model_registry import load_artifact, load_metadata


def run_zscore(df: pd.DataFrame, threshold: float = 2.5) -> pd.DataFrame:
    df = df.copy()
    mean_val = df["revenue"].mean()
    std_val = df["revenue"].std()

    if std_val == 0 or pd.isna(std_val):
        df["z_score"] = 0.0
        df["is_anomaly"] = 0
    else:
        df["z_score"] = (df["revenue"] - mean_val) / std_val
        df["is_anomaly"] = (df["z_score"] < -threshold).astype(int)

    return df


def run_isolation_forest(df: pd.DataFrame, model) -> pd.DataFrame:
    df = df.copy()
    X = df[["revenue"]]
    preds = model.predict(X)
    df["is_anomaly"] = (preds == -1).astype(int)
    return df


def run():
    ensure_project_dirs()

    anomalies_dir = MODELS_DIR / "anomalies"
    metadata = load_metadata(anomalies_dir / "metadata.json")
    best_model_name = metadata["best_model_name"]

    df = build_daily_sales_features()

    if best_model_name == "z_score":
        result_df = run_zscore(df)
    elif best_model_name == "isolation_forest":
        model = load_artifact(anomalies_dir / "best_model.joblib")
        result_df = run_isolation_forest(df, model)
    else:
        raise ValueError(f"Modèle anomalies non supporté : {best_model_name}")

    result_df["model_name"] = best_model_name

    result_df.to_csv(PREDICTIONS_DIR / "anomalies_best_model.csv", index=False)
    result_df[result_df["is_anomaly"] == 1].to_csv(
        PREDICTIONS_DIR / "anomalies_only_best_model.csv", index=False
    )

    print("Anomaly inference terminée.")
    print(result_df[result_df["is_anomaly"] == 1].head())


if __name__ == "__main__":
    run()