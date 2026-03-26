import pandas as pd
from datetime import datetime

from ml.integration.config import get_clickhouse_client


def insert_forecast():
    client = get_clickhouse_client()

    df = pd.read_csv(
        "ml/outputs/predictions/sales_forecast_next_3_months.csv"
    )

    df["forecast_month_date"] = pd.to_datetime(df["forecast_month_date"])
    df["forecast_month"] = pd.to_datetime(df["forecast_month"])
    df["created_at"] = datetime.now()

    client.insert_df("sales_forecast", df)

    print("Forecast inséré dans ClickHouse")


if __name__ == "__main__":
    insert_forecast()