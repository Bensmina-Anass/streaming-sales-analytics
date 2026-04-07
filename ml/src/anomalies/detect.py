import pandas as pd

from ml.configs.paths import (
    METRICS_DIR,
    PREDICTIONS_DIR,
    MODELS_DIR,
    ensure_project_dirs,
)
from ml.configs.settings import RANDOM_STATE
from ml.src.anomalies.models import get_anomaly_models
from ml.src.feature_engineering import build_daily_sales_features
from ml.src.model_registry import save_artifact, save_metadata


def detect_with_zscore(df: pd.DataFrame, threshold: float = 2.5):
    mean_val = df["revenue"].mean()
    std_val = df["revenue"].std()

    df = df.copy()

    if std_val == 0 or pd.isna(std_val):
        df["z_score"] = 0.0
        df["is_anomaly"] = 0
    else:
        df["z_score"] = (df["revenue"] - mean_val) / std_val
        df["is_anomaly"] = (df["z_score"] < -threshold).astype(int)

    return df


def detect_with_isolation_forest(df: pd.DataFrame, model):
    df = df.copy()
    X = df[["revenue"]]
    preds = model.fit_predict(X)
    df["is_anomaly"] = (preds == -1).astype(int)
    return df, model


def evaluate_anomaly_results(df: pd.DataFrame) -> dict:
    total_points = len(df)
    anomalies = int(df["is_anomaly"].sum())

    return {
        "total_points": int(total_points),
        "num_anomalies": anomalies,
        "anomaly_ratio": float(anomalies / total_points) if total_points else 0.0,
    }


def run_anomaly_detection():
    ensure_project_dirs()

    anomalies_models_dir = MODELS_DIR / "anomalies"
    anomalies_models_dir.mkdir(parents=True, exist_ok=True)

    df = build_daily_sales_features()
    models = get_anomaly_models(random_state=RANDOM_STATE)

    results = []
    outputs = []

    # Z-score
    z_df = detect_with_zscore(df, threshold=2.5)
    z_metrics = evaluate_anomaly_results(z_df)

    results.append(
        {
            "model_name": "z_score",
            **z_metrics,
        }
    )
    z_df["model_name"] = "z_score"
    outputs.append(z_df)

    # Isolation Forest
    iso_model = models["isolation_forest"]
    iso_df, fitted_iso_model = detect_with_isolation_forest(df, iso_model)
    iso_metrics = evaluate_anomaly_results(iso_df)

    results.append(
        {
            "model_name": "isolation_forest",
            **iso_metrics,
        }
    )
    iso_df["model_name"] = "isolation_forest"
    outputs.append(iso_df)

    metrics_df = pd.DataFrame(results)
    all_outputs_df = pd.concat(outputs, ignore_index=True)

    # Choix simple : ratio d'anomalies le plus proche de 2%
    best_model_name = metrics_df.iloc[
        (metrics_df["anomaly_ratio"] - 0.02).abs().argsort()
    ].iloc[0]["model_name"]

    best_df = all_outputs_df[all_outputs_df["model_name"] == best_model_name].copy()
    anomalies_only = best_df[best_df["is_anomaly"] == 1].copy()

    # Sauvegardes
    metrics_df.to_csv(METRICS_DIR / "anomaly_model_comparison.csv", index=False)
    all_outputs_df.to_csv(PREDICTIONS_DIR / "anomalies_all_models.csv", index=False)
    best_df.to_csv(PREDICTIONS_DIR / "anomalies_best_model.csv", index=False)
    anomalies_only.to_csv(PREDICTIONS_DIR / "anomalies_only_best_model.csv", index=False)

    metadata = {
        "task": "anomalies",
        "version": "v2",
        "best_model_name": best_model_name,
        "selection_metric": "anomaly_ratio_closest_to_2_percent",
    }
    save_metadata(metadata, anomalies_models_dir / "metadata.json")

    if best_model_name == "isolation_forest":
        save_artifact(fitted_iso_model, anomalies_models_dir / "best_model.joblib")

    print("Détection anomalies terminée.")
    print("\nComparaison des modèles :")
    print(metrics_df)
    print(f"\nModèle retenu : {best_model_name}")

    return {
        "metrics": metrics_df,
        "best_model": best_model_name,
        "anomalies": anomalies_only,
    }


if __name__ == "__main__":
    run_anomaly_detection()