import os
import time
import pandas as pd
import psycopg2
import psycopg2.extras
import clickhouse_connect

DATA_DIR = "/app/data/"

# --- PostgreSQL Configuration ---
PG_DSN = {
    "host":     os.getenv("PG_HOST", "postgres"),
    "dbname":   os.getenv("PG_DB", "e-commerce"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

# --- ClickHouse Configuration ---
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", 8123))
CH_DB = os.getenv("CH_DB", "ecommerce_dw")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin")

# Explicitly define integer columns per table based on the actual Postgres schema
INTEGER_COLUMNS = {
    "products": ["product_name_lenght", "product_description_lenght", "product_photos_qty"],
    "geolocation": [],
    "customers": [],
    "sellers": [],
    "category_translation": [],
}

# ==========================================
# POSTGRESQL LOGIC (Untouched)
# ==========================================
def connect(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to PostgreSQL (attempt {attempt}/{retries})...")
            conn = psycopg2.connect(**PG_DSN)
            print("Connected to PostgreSQL!")
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
    print(f"  ✓ Postgres ({table}): {len(records)} rows loaded")

def table_is_empty(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return cur.fetchone() is None


# ==========================================
# CLICKHOUSE LOGIC (New)
# ==========================================
def connect_clickhouse(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to ClickHouse (attempt {attempt}/{retries})...")
            client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB
            )
            client.command("SELECT 1")
            print("Connected to ClickHouse!")
            return client
        except Exception as e:
            print(f"Not ready: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to ClickHouse.")

def ch_table_is_empty(client, table: str) -> bool:
    """Check if a ClickHouse table has data."""
    result = client.query(f"SELECT 1 FROM {table} LIMIT 1")
    return len(result.result_rows) == 0

def load_clickhouse(client, table_name: str, df: pd.DataFrame):
    """Clean the data and insert into ClickHouse."""
    if df.empty:
        return
        
    # Operate on a copy so we don't ruin the Postgres insertion logic
    ch_df = df.copy()

    # Fix the typo specifically for the ClickHouse schema
    if 'product_name_lenght' in ch_df.columns:
        ch_df = ch_df.rename(columns={
            'product_name_lenght': 'product_name_length',
            'product_description_lenght': 'product_description_length'
        })

    # Strict Schema Enforcement (Handle Nulls safely)
    for col in ch_df.columns:
        if any(k in col.lower() for k in ['_id', 'prefix', 'city', 'state', 'name', 'status', 'type', 'title', 'message']) and 'length' not in col.lower():
            ch_df[col] = ch_df[col].fillna('Unknown').astype(str)
            ch_df[col] = ch_df[col].replace({'nan': 'Unknown', 'None': 'Unknown'})
        else:
            ch_df[col] = pd.to_numeric(ch_df[col], errors='coerce').fillna(0)

    client.insert_df(table_name, ch_df)
    print(f"  ✓ ClickHouse ({table_name}): {len(ch_df)} rows loaded")


# ==========================================
# MAIN EXECUTION
# ==========================================
def run():
    pg_conn = connect()
    ch_client = connect_clickhouse()
    
    print("\nLoading reference tables into PostgreSQL and ClickHouse...\n")

    # 1. Customers
    customers = pd.read_csv(os.path.join(DATA_DIR, "olist_customers_dataset.csv"))
    bulk_insert(pg_conn, "customers", customers, "customer_id")
    if ch_table_is_empty(ch_client, "bronze_customers"):
        load_clickhouse(ch_client, "bronze_customers", customers)
    else:
        print("  ✓ ClickHouse (bronze_customers): already seeded, skipping")

    # 2. Sellers
    sellers = pd.read_csv(os.path.join(DATA_DIR, "olist_sellers_dataset.csv"))
    bulk_insert(pg_conn, "sellers", sellers, "seller_id")
    if ch_table_is_empty(ch_client, "bronze_sellers"):
        load_clickhouse(ch_client, "bronze_sellers", sellers)
    else:
        print("  ✓ ClickHouse (bronze_sellers): already seeded, skipping")

    # 3. Products
    products = pd.read_csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"))
    bulk_insert(pg_conn, "products", products, "product_id")
    if ch_table_is_empty(ch_client, "bronze_products"):
        load_clickhouse(ch_client, "bronze_products", products)
    else:
        print("  ✓ ClickHouse (bronze_products): already seeded, skipping")

    # 4. Geolocation
    geo = pd.read_csv(os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"))
    if table_is_empty(pg_conn, "geolocation"):
        bulk_insert(pg_conn, "geolocation", geo, conflict_col=None)
    else:
        print("  ✓ Postgres (geolocation): already seeded, skipping")
    
    if ch_table_is_empty(ch_client, "bronze_geolocation"):
        load_clickhouse(ch_client, "bronze_geolocation", geo)
    else:
        print("  ✓ ClickHouse (bronze_geolocation): already seeded, skipping")

    # 5. Category Translation
    cat = pd.read_csv(os.path.join(DATA_DIR, "product_category_name_translation.csv"))
    bulk_insert(pg_conn, "category_translation", cat, "product_category_name")
    if ch_table_is_empty(ch_client, "bronze_category_translation"):
        load_clickhouse(ch_client, "bronze_category_translation", cat)
    else:
        print("  ✓ ClickHouse (bronze_category_translation): already seeded, skipping")


    print("\nAll reference tables seeded successfully. Exiting.")
    pg_conn.close()

if __name__ == "__main__":
    run()