from ml.configs.paths import (
    SEGMENTATION_MODELS_DIR,
    PREDICTIONS_DIR,
    ensure_project_dirs,
)
from ml.configs.settings import SEGMENTATION_FEATURES
from ml.src.feature_engineering import build_customer_segmentation_features
from ml.src.model_registry import load_artifact, load_metadata
from ml.src.preprocessing import transform_with_scaler


def run():
    ensure_project_dirs()

    model = load_artifact(SEGMENTATION_MODELS_DIR / "best_model.joblib")
    scaler = load_artifact(SEGMENTATION_MODELS_DIR / "scaler.joblib")
    metadata = load_metadata(SEGMENTATION_MODELS_DIR / "metadata.json")

    df = build_customer_segmentation_features().copy()
    X = df[SEGMENTATION_FEATURES].copy()

    X_scaled = transform_with_scaler(scaler, X)
    labels = model.predict(X_scaled)

    df["cluster_id"] = labels
    df["best_model_name"] = metadata["best_model_name"]

    cluster_summary = (
        df.groupby("cluster_id")[SEGMENTATION_FEATURES]
        .mean()
        .reset_index()
        .sort_values("cluster_id")
    )

    df.to_csv(PREDICTIONS_DIR / "customer_segments_latest.csv", index=False)
    cluster_summary.to_csv(
        PREDICTIONS_DIR / "customer_segments_summary_latest.csv", index=False
    )

    print("Segmentation inference terminée.")
    print(cluster_summary)


if __name__ == "__main__":
    run()