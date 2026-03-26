import pandas as pd
from ml.configs.paths import DATA_DIR


def _read_csv(filename: str) -> pd.DataFrame:
    file_path = DATA_DIR / filename
    if not file_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {file_path}\n"
            f"Vérifie que le dossier data/oltp contient bien {filename}."
        )
    return pd.read_csv(file_path)


def load_orders() -> pd.DataFrame:
    df = _read_csv("olist_orders_dataset.csv")
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )
    df["order_approved_at"] = pd.to_datetime(df["order_approved_at"], errors="coerce")
    df["order_delivered_carrier_date"] = pd.to_datetime(
        df["order_delivered_carrier_date"], errors="coerce"
    )
    df["order_delivered_customer_date"] = pd.to_datetime(
        df["order_delivered_customer_date"], errors="coerce"
    )
    df["order_estimated_delivery_date"] = pd.to_datetime(
        df["order_estimated_delivery_date"], errors="coerce"
    )
    return df


def load_payments() -> pd.DataFrame:
    return _read_csv("olist_order_payments_dataset.csv")


def load_order_items() -> pd.DataFrame:
    df = _read_csv("olist_order_items_dataset.csv")
    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")
    return df


def load_customers() -> pd.DataFrame:
    return _read_csv("olist_customers_dataset.csv")


def load_products() -> pd.DataFrame:
    return _read_csv("olist_products_dataset.csv")


def load_reviews() -> pd.DataFrame:
    df = _read_csv("olist_order_reviews_dataset.csv")
    df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce")
    df["review_answer_timestamp"] = pd.to_datetime(
        df["review_answer_timestamp"], errors="coerce"
    )
    return df