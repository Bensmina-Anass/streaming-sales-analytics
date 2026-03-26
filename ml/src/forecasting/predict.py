import pandas as pd

from ml.configs.paths import (
    FORECASTING_MODELS_DIR,
    PREDICTIONS_DIR,
    ensure_project_dirs,
)
from ml.configs.settings import FORECAST_FEATURES, FORECAST_HORIZON
from ml.src.feature_engineering import build_monthly_sales_features
from ml.src.model_registry import load_artifact, load_metadata


def forecast_next_months(model, historical_df: pd.DataFrame, horizon: int = 3) -> pd.DataFrame:
    df = historical_df.copy().sort_values("month_date").reset_index(drop=True)

    revenues = df["revenue"].tolist()
    last_month_date = df["month_date"].max()
    last_time_idx = df["time_idx"].max()

    future_rows = []

    for step in range(1, horizon + 1):
        future_month_date = last_month_date + pd.DateOffset(months=step)
        future_time_idx = last_time_idx + step
        future_month_num = future_month_date.month

        lag_1 = revenues[-1]
        lag_2 = revenues[-2]
        lag_3 = revenues[-3]

        X_future = pd.DataFrame(
            {
                "time_idx": [future_time_idx],
                "month_num": [future_month_num],
                "lag_1": [lag_1],
                "lag_2": [lag_2],
                "lag_3": [lag_3],
            }
        )[FORECAST_FEATURES]

        predicted_revenue = float(model.predict(X_future)[0])
        revenues.append(predicted_revenue)

        future_rows.append(
            {
                "forecast_month": future_month_date.strftime("%Y-%m"),
                "forecast_month_date": future_month_date,
                "predicted_revenue": predicted_revenue,
            }
        )

    return pd.DataFrame(future_rows)


def run_forecasting_prediction():
    ensure_project_dirs()

    model = load_artifact(FORECASTING_MODELS_DIR / "best_model.joblib")
    metadata = load_metadata(FORECASTING_MODELS_DIR / "metadata.json")

    historical_df = build_monthly_sales_features()
    forecast_df = forecast_next_months(
        model=model,
        historical_df=historical_df,
        horizon=FORECAST_HORIZON,
    )

    forecast_df["best_model_name"] = metadata["best_model_name"]

    forecast_df.to_csv(
        PREDICTIONS_DIR / "sales_forecast_next_3_months.csv",
        index=False,
    )

    print("Prédiction forecasting terminée.")
    print(f"Modèle utilisé : {metadata['best_model_name']}")
    print(forecast_df)

    return forecast_df


if __name__ == "__main__":
    run_forecasting_prediction()