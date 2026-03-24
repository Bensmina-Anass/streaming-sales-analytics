import pandas as pd
import os
import time
import clickhouse_connect

CLICKHOUSE_HOST = "olap_clickhouse_ml_branch" 
DATA_DIR = "/app/data/"             

def wait_clickhouse():
    """Smart retry loop to wait for ClickHouse and the tables to initialize."""
    print("Waiting for ClickHouse to boot and initialize schemas...")
    retries = 15
    for attempt in range(retries):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, 
                port=8123, 
                username='admin', 
                password='admin', 
                database='ecommerce_dw'
            )
            client.command("SELECT 1 FROM bronze_orders LIMIT 1")
            print("ClickHouse is fully initialized and ready!")
            return client
        except Exception:
            print(f"ClickHouse not ready yet. Retrying in 5 seconds (Attempt {attempt+1}/{retries})...")
            time.sleep(5)
            
    raise Exception("Fatal: ClickHouse did not become available in time.")

def seed_data_warehouse():
    client = wait_clickhouse()
    print("\n--- PHASE 1: Loading Bronze Layer ---")
    files_to_tables = {
        'olist_geolocation_dataset.csv': 'bronze_geolocation',
        'olist_customers_dataset.csv': 'bronze_customers',
        'olist_sellers_dataset.csv': 'bronze_sellers',
        'olist_products_dataset.csv': 'bronze_products',
        'product_category_name_translation.csv': 'bronze_category_translation',
        'olist_orders_dataset.csv': 'bronze_orders',
        'olist_order_items_dataset.csv': 'bronze_order_items',
        'olist_order_payments_dataset.csv': 'bronze_order_payments',
        'olist_order_reviews_dataset.csv': 'bronze_order_reviews'
    }

    for filename, table_name in files_to_tables.items():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"Skipping {filename}: File not found.")
            continue
            
        print(f"Reading {filename}...")
        df = pd.read_csv(filepath)
        
        df = df.rename(columns={
            'product_name_lenght': 'product_name_length',
            'product_description_lenght': 'product_description_length'
        })
        
        for col in df.columns:
            if 'date' in col.lower() or 'timestamp' in col.lower() or 'approved_at' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].replace({pd.NaT: None})
            
            elif any(k in col.lower() for k in ['_id', 'prefix', 'city', 'state', 'name', 'status', 'type', 'title', 'message']) and 'length' not in col.lower():
                df[col] = df[col].fillna('Unknown')
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({'nan': 'Unknown', 'None': 'Unknown'})
            
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        print(f"Inserting {len(df)} rows into {table_name}...")
        client.insert_df(table_name, df)


    print("\n--- PHASE 2: Building Silver Layer ---")
    client.command("""
        INSERT INTO silver_products
        SELECT p.product_id, coalesce(t.product_category_name_english, p.product_category_name),
               p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm
        FROM bronze_products p LEFT JOIN bronze_category_translation t ON p.product_category_name = t.product_category_name
    """)

    client.command("""
        INSERT INTO silver_customers
        SELECT c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state,
               any(g.geolocation_lat) as customer_lat, any(g.geolocation_lng) as customer_lng
        FROM bronze_customers c LEFT JOIN bronze_geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
        GROUP BY c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state
    """)

    client.command("INSERT INTO silver_orders SELECT * FROM bronze_orders")
    client.command("INSERT INTO silver_order_items SELECT * FROM bronze_order_items")
    client.command("INSERT INTO silver_order_payments SELECT * FROM bronze_order_payments")
    client.command("INSERT INTO silver_order_reviews SELECT review_id, order_id, review_score, review_creation_date, review_answer_timestamp FROM bronze_order_reviews")


    print("\n--- PHASE 3: Building Gold Fact Table ---")
    client.command("""
        INSERT INTO fact_order_items
        SELECT i.order_id, i.order_item_id, o.customer_id, i.product_id, i.seller_id,
               toYYYYMMDD(o.order_purchase_timestamp) AS purchase_date_key,
               toYYYYMMDD(o.order_approved_at) AS approved_date_key,
               toYYYYMMDD(o.order_delivered_carrier_date) AS delivered_carrier_date_key,
               toYYYYMMDD(o.order_delivered_customer_date) AS delivered_customer_date_key,
               toYYYYMMDD(o.order_estimated_delivery_date) AS estimated_delivery_date_key,
               o.order_status, i.price, i.freight_value, p.payment_value_total,
               p.payment_installments_max, 'credit_card' AS payment_type_main, r.review_score
        FROM silver_order_items i
        JOIN silver_orders o ON i.order_id = o.order_id
        LEFT JOIN (SELECT order_id, sum(payment_value) as payment_value_total, max(payment_installments) as payment_installments_max FROM silver_order_payments GROUP BY order_id) p ON i.order_id = p.order_id
        LEFT JOIN (SELECT order_id, avg(review_score) as review_score FROM silver_order_reviews GROUP BY order_id) r ON i.order_id = r.order_id
    """)

    print("\n--- PHASE 4: Reloading RAM Dictionaries ---")
    client.command("SYSTEM RELOAD DICTIONARIES")
    print("\nData Warehouse successfully seeded. Your colleagues are unblocked!")

if __name__ == "__main__":
    seed_data_warehouse()