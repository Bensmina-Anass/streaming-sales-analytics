CREATE DATABASE IF NOT EXISTS ecommerce_dw;

CREATE TABLE IF NOT EXISTS ecommerce_dw.sales_forecast (
    forecast_month Date,
    forecast_month_date Date,
    predicted_revenue Float64,
    best_model_name String,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY forecast_month;

CREATE TABLE IF NOT EXISTS ecommerce_dw.customer_segments (
    customer_unique_id String,
    recency Int32,
    frequency Int32,
    monetary Float64,
    avg_order_value Float64,
    cluster_id Int32,
    best_model_name String,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY customer_unique_id;

CREATE TABLE IF NOT EXISTS ecommerce_dw.sales_anomalies (
    date Date,
    revenue Float64,
    is_anomaly UInt8,
    model_name String,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY date;