import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from feature_engineering import build_customer_segmentation_features
from utils import save_dataframe, save_model


def run_customer_segmentation(n_clusters: int = 4):
    df = build_customer_segmentation_features().copy()

    feature_cols = ["recency", "frequency", "monetary", "avg_order_value"]
    X = df[feature_cols]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["cluster_id"] = model.fit_predict(X_scaled)

    cluster_summary = (
        df.groupby("cluster_id")[feature_cols]
        .mean()
        .reset_index()
        .sort_values("cluster_id")
    )

    save_model(model, "customer_segmentation_model.joblib")
    save_dataframe(df, "customer_segments.csv")
    save_dataframe(cluster_summary, "customer_segments_summary.csv")

    return df, cluster_summary


if __name__ == "__main__":
    segmented_df, summary_df = run_customer_segmentation(n_clusters=4)
    print("Segmentation terminée.")
    print(summary_df)