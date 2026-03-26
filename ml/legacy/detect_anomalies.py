import pandas as pd

from feature_engineering import build_daily_sales_features
from utils import save_dataframe


def detect_sales_anomalies(z_threshold: float = 2.5):
    df = build_daily_sales_features().copy()

    mean_revenue = df["revenue"].mean()
    std_revenue = df["revenue"].std()

    if std_revenue == 0 or pd.isna(std_revenue):
        df["z_score"] = 0.0
        df["is_anomaly"] = 0
    else:
        df["z_score"] = (df["revenue"] - mean_revenue) / std_revenue
        df["is_anomaly"] = (df["z_score"] < -z_threshold).astype(int)

    anomalies = df[df["is_anomaly"] == 1].copy()

    save_dataframe(df, "daily_sales_with_anomalies.csv")
    save_dataframe(anomalies, "sales_anomalies_only.csv")

    return df, anomalies


if __name__ == "__main__":
    full_df, anomalies_df = detect_sales_anomalies(z_threshold=2.5)
    print("Détection d'anomalies terminée.")
    print(anomalies_df.head())