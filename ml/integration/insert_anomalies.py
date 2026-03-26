import pandas as pd
from datetime import datetime

from ml.integration.config import get_clickhouse_client


def insert_anomalies():
    client = get_clickhouse_client()

    df = pd.read_csv(
        "ml/outputs/predictions/anomalies_best_model.csv"
    )

    df["created_at"] = datetime.now()

    client.insert_df("sales_anomalies", df)

    print("Anomalies insérées dans ClickHouse")


if __name__ == "__main__":
    insert_anomalies()