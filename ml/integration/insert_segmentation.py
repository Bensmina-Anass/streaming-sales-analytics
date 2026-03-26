import pandas as pd
from datetime import datetime

from ml.integration.config import get_clickhouse_client


def insert_segmentation():
    client = get_clickhouse_client()

    df = pd.read_csv(
        "ml/outputs/predictions/customer_segments_latest.csv"
    )

    df["created_at"] = datetime.now()

    client.insert_df("customer_segments", df)

    print("Segmentation insérée dans ClickHouse")


if __name__ == "__main__":
    insert_segmentation()