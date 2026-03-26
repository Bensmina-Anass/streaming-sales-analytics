import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

from feature_engineering import build_monthly_sales_features
from utils import save_dataframe, save_model


def train_forecasting_model():
    df = build_monthly_sales_features().copy()

    feature_cols = ["time_idx", "month_num", "lag_1", "lag_2", "lag_3"]
    target_col = "revenue"

    X = df[feature_cols]
    y = df[target_col]

    # Split temporel simple
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = pd.DataFrame(
        {
            "metric": ["mae", "rmse"],
            "value": [
                mean_absolute_error(y_test, y_pred),
                mean_squared_error(y_test, y_pred) ** 0.5,
            ],
        }
    )

    test_predictions = df.iloc[split_idx:].copy()
    test_predictions["predicted_revenue"] = y_pred

    save_model(model, "forecast_model.joblib")
    save_dataframe(test_predictions, "forecast_test_predictions.csv")
    save_dataframe(metrics, "forecast_metrics.csv")

    return model, df


def forecast_next_3_months(model, historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prévision récursive simple sur les 3 prochains mois.
    """
    df = historical_df.copy().sort_values("month_date").reset_index(drop=True)

    future_rows = []

    last_time_idx = df["time_idx"].max()
    revenues = df["revenue"].tolist()
    last_month_date = df["month_date"].max()

    for step in range(1, 4):
        future_month_date = last_month_date + pd.DateOffset(months=step)
        month_num = future_month_date.month
        time_idx = last_time_idx + step

        lag_1 = revenues[-1]
        lag_2 = revenues[-2]
        lag_3 = revenues[-3]

        features = pd.DataFrame(
            {
                "time_idx": [time_idx],
                "month_num": [month_num],
                "lag_1": [lag_1],
                "lag_2": [lag_2],
                "lag_3": [lag_3],
            }
        )

        predicted_revenue = model.predict(features)[0]
        revenues.append(predicted_revenue)

        future_rows.append(
            {
                "forecast_month": future_month_date.strftime("%Y-%m"),
                "predicted_revenue": predicted_revenue,
            }
        )

    return pd.DataFrame(future_rows)


if __name__ == "__main__":
    model, historical_df = train_forecasting_model()
    future_forecast = forecast_next_3_months(model, historical_df)
    save_dataframe(future_forecast, "sales_forecast_next_3_months.csv")
    print("Forecasting terminé.")
    print(future_forecast)