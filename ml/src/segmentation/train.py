import pandas as pd

from ml.configs.paths import (
    METRICS_DIR,
    PREDICTIONS_DIR,
    SEGMENTATION_MODELS_DIR,
    ensure_project_dirs,
)
from ml.configs.settings import RANDOM_STATE, SEGMENTATION_FEATURES
from ml.src.evaluate import evaluate_clustering
from ml.src.feature_engineering import build_customer_segmentation_features
from ml.src.model_registry import save_artifact, save_metadata
from ml.src.preprocessing import fit_standard_scaler
from ml.src.segmentation.models import get_segmentation_models


def train_and_select_best_segmentation_model():
    ensure_project_dirs()

    df = build_customer_segmentation_features().copy()
    X = df[SEGMENTATION_FEATURES].copy()

    scaler, X_scaled = fit_standard_scaler(X)

    models = get_segmentation_models(random_state=RANDOM_STATE)

    metrics_rows = []
    labeled_frames = []

    best_model_name = None
    best_model = None
    best_metrics = None
    best_labels = None
    best_score = float("-inf")

    for model_name, model in models.items():
        labels = model.fit_predict(X_scaled)
        metrics = evaluate_clustering(X_scaled, labels)

        metrics_rows.append(
            {
                "model_name": model_name,
                "n_clusters": int(model.n_clusters),
                "silhouette_score": metrics["silhouette_score"],
                "inertia": float(model.inertia_),
            }
        )

        labeled_df = df.copy()
        labeled_df["model_name"] = model_name
        labeled_df["cluster_id"] = labels
        labeled_frames.append(labeled_df)

        if metrics["silhouette_score"] > best_score:
            best_score = metrics["silhouette_score"]
            best_model_name = model_name
            best_model = model
            best_metrics = metrics
            best_labels = labels

    metrics_df = (
        pd.DataFrame(metrics_rows)
        .sort_values(["silhouette_score", "inertia"], ascending=[False, True])
        .reset_index(drop=True)
    )

    best_segmented_df = df.copy()
    best_segmented_df["cluster_id"] = best_labels

    cluster_summary = (
        best_segmented_df.groupby("cluster_id")[SEGMENTATION_FEATURES]
        .mean()
        .reset_index()
        .sort_values("cluster_id")
    )

    all_labeled_df = pd.concat(labeled_frames, ignore_index=True)

    save_artifact(best_model, SEGMENTATION_MODELS_DIR / "best_model.joblib")
    save_artifact(scaler, SEGMENTATION_MODELS_DIR / "scaler.joblib")

    metadata = {
        "task": "segmentation",
        "best_model_name": best_model_name,
        "selection_metric": "silhouette_score",
        "silhouette_score": best_metrics["silhouette_score"],
        "n_clusters": int(best_model.n_clusters),
        "features": SEGMENTATION_FEATURES,
    }
    save_metadata(metadata, SEGMENTATION_MODELS_DIR / "metadata.json")

    metrics_df.to_csv(METRICS_DIR / "segmentation_model_comparison.csv", index=False)
    best_segmented_df.to_csv(
        PREDICTIONS_DIR / "customer_segments_best_model.csv", index=False
    )
    cluster_summary.to_csv(
        PREDICTIONS_DIR / "customer_segments_summary_best_model.csv", index=False
    )
    all_labeled_df.to_csv(
        PREDICTIONS_DIR / "customer_segments_all_models.csv", index=False
    )

    print("Entraînement segmentation terminé.")
    print("\nComparaison des modèles :")
    print(metrics_df)

    print(f"\nMeilleur modèle : {best_model_name}")
    print(f"Silhouette Score = {best_metrics['silhouette_score']:.4f}")
    print(f"Nombre de clusters = {best_model.n_clusters}")

    print("\nRésumé des clusters du meilleur modèle :")
    print(cluster_summary)

    return {
        "metrics_df": metrics_df,
        "best_model_name": best_model_name,
        "best_model": best_model,
        "best_segmented_df": best_segmented_df,
        "cluster_summary": cluster_summary,
    }


if __name__ == "__main__":
    train_and_select_best_segmentation_model()