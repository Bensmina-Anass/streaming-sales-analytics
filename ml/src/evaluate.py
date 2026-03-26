import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    silhouette_score,
)


def evaluate_regression(y_true, y_pred) -> dict:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
        "r2": float(r2_score(y_true, y_pred)),
    }


def evaluate_clustering(X_scaled, labels) -> dict:
    unique_labels = set(labels)
    if len(unique_labels) < 2:
        silhouette = -1.0
    else:
        silhouette = float(silhouette_score(X_scaled, labels))

    return {
        "silhouette_score": silhouette,
    }


def metrics_dict_to_dataframe(metrics: dict, model_name: str) -> pd.DataFrame:
    rows = []
    for metric_name, metric_value in metrics.items():
        rows.append(
            {
                "model_name": model_name,
                "metric": metric_name,
                "value": metric_value,
            }
        )
    return pd.DataFrame(rows)