import pandas as pd

from ml.src.data_loader import load_orders, load_payments, load_customers


def build_base_sales_dataframe() -> pd.DataFrame:
    orders = load_orders()
    payments = load_payments()

    orders = orders[orders["order_status"] == "delivered"].copy()

    payments_agg = payments.groupby("order_id", as_index=False)["payment_value"].sum()

    df = orders.merge(payments_agg, on="order_id", how="inner")
    return df


def build_monthly_sales_features() -> pd.DataFrame:
    df = build_base_sales_dataframe().copy()

    df["month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)

    monthly = (
        df.groupby("month", as_index=False)["payment_value"]
        .sum()
        .rename(columns={"payment_value": "revenue"})
    )

    monthly["month_date"] = pd.to_datetime(monthly["month"])
    monthly = monthly.sort_values("month_date").reset_index(drop=True)

    monthly["month_num"] = monthly["month_date"].dt.month
    monthly["time_idx"] = range(len(monthly))

    monthly["lag_1"] = monthly["revenue"].shift(1)
    monthly["lag_2"] = monthly["revenue"].shift(2)
    monthly["lag_3"] = monthly["revenue"].shift(3)

    monthly = monthly.dropna().reset_index(drop=True)
    return monthly


def build_customer_segmentation_features() -> pd.DataFrame:
    sales = build_base_sales_dataframe()
    customers = load_customers()

    df = sales.merge(customers, on="customer_id", how="left")

    snapshot_date = df["order_purchase_timestamp"].max() + pd.Timedelta(days=1)

    customer_features = (
        df.groupby("customer_unique_id")
        .agg(
            recency=("order_purchase_timestamp", lambda x: (snapshot_date - x.max()).days),
            frequency=("order_id", "nunique"),
            monetary=("payment_value", "sum"),
        )
        .reset_index()
    )

    customer_features["avg_order_value"] = (
        customer_features["monetary"] / customer_features["frequency"]
    )

    return customer_features


def build_daily_sales_features() -> pd.DataFrame:
    df = build_base_sales_dataframe().copy()

    df["date"] = df["order_purchase_timestamp"].dt.date
    daily = (
        df.groupby("date", as_index=False)["payment_value"]
        .sum()
        .rename(columns={"payment_value": "revenue"})
    )

    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").reset_index(drop=True)
    return daily