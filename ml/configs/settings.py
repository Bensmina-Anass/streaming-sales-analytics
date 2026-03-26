RANDOM_STATE = 42

FORECAST_FEATURES = [
    "time_idx",
    "month_num",
    "lag_1",
    "lag_2",
    "lag_3",
]

FORECAST_TARGET = "revenue"

FORECAST_TEST_SIZE = 0.2

FORECAST_HORIZON = 3

SEGMENTATION_FEATURES = [
    "recency",
    "frequency",
    "monetary",
    "avg_order_value",
]