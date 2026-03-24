import os
import time
import pandas as pd
import psycopg2
import psycopg2.extras

DATA_DIR = "/app/data/"
PG_DSN = {
    "host":     os.getenv("PG_HOST", "postgres"),
    "dbname":   os.getenv("PG_DB", "e-commerce"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

# Explicitly define integer columns per table based on the actual Postgres schema
INTEGER_COLUMNS = {
    "products": ["product_name_lenght", "product_description_lenght", "product_photos_qty"],
    "geolocation": [],
    "customers": [],
    "sellers": [],
    "category_translation": [],
}


def connect(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to PostgreSQL (attempt {attempt}/{retries})...")
            conn = psycopg2.connect(**PG_DSN)
            print("Connected!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Not ready: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL.")


def prepare_df(df: pd.DataFrame, table: str) -> list:
    """Convert DataFrame rows to clean Python dicts with correct types."""
    int_cols = set(INTEGER_COLUMNS.get(table, []))
    records = []
    for row in df.itertuples(index=False):
        record = {}
        for col in df.columns:
            val = getattr(row, col)
            if pd.isna(val):
                record[col] = None
            elif col in int_cols:
                record[col] = int(float(val))   # e.g. "46.0" → 46
            else:
                record[col] = val
        records.append(record)
    return records


def bulk_insert(conn, table: str, df: pd.DataFrame, conflict_col: str = None):
    if df.empty:
        return

    records      = prepare_df(df, table)
    cols         = list(df.columns)
    col_names    = ", ".join(cols)
    placeholders = ", ".join([f"%({c})s" for c in cols])

    # Only add ON CONFLICT clause if a unique column is specified
    conflict_clause = f"ON CONFLICT ({conflict_col}) DO NOTHING" if conflict_col else ""

    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        {conflict_clause}
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
    conn.commit()
    print(f"  ✓ {table}: {len(records)} rows loaded")


def table_is_empty(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return cur.fetchone() is None


def run():
    conn = connect()
    print("\nLoading reference tables into PostgreSQL...")

    customers = pd.read_csv(os.path.join(DATA_DIR, "olist_customers_dataset.csv"))
    bulk_insert(conn, "customers", customers, "customer_id")

    sellers = pd.read_csv(os.path.join(DATA_DIR, "olist_sellers_dataset.csv"))
    bulk_insert(conn, "sellers", sellers, "seller_id")

    products = pd.read_csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"))
    bulk_insert(conn, "products", products, "product_id")

    # geolocation has no unique constraint — guard against duplicate seeding
    if table_is_empty(conn, "geolocation"):
        geo = pd.read_csv(os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"))
        bulk_insert(conn, "geolocation", geo, conflict_col=None)
    else:
        print("  ✓ geolocation: already seeded, skipping")

    cat = pd.read_csv(os.path.join(DATA_DIR, "product_category_name_translation.csv"))
    bulk_insert(conn, "category_translation", cat, "product_category_name")

    print("\nAll reference tables seeded. Exiting.")
    conn.close()



if __name__ == "__main__":
    run()
