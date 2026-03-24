import pandas as pd
import time
import os
from kafka import KafkaProducer
import json

kafka_nodes = "kafka:9092"
data_dir = "/app/data/"
state_file = os.path.join(data_dir, 'producer_state.txt')

def load_and_prep_data():
    orders = pd.read_csv(os.path.join(data_dir, 'olist_orders_dataset.csv'))
    items = pd.read_csv(os.path.join(data_dir, 'olist_order_items_dataset.csv'))
    payments = pd.read_csv(os.path.join(data_dir, 'olist_order_payments_dataset.csv'))
    reviews = pd.read_csv(os.path.join(data_dir, 'olist_order_reviews_dataset.csv'))

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    orders = orders.sort_values('order_purchase_timestamp')
    
    return orders, items, payments, reviews

def get_last_ingested_time():
    """Reads the last processed timestamp from the local state file."""
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            last_time = f.read().strip()
            if last_time:
                return pd.to_datetime(last_time)
    return None

def save_last_ingested_time(current_time):
    """Saves the latest timestamp to the state file."""
    with open(state_file, 'w') as f:
        f.write(str(current_time))

def stream_transactions(orders, items, payments, reviews, chunk_size=100, delay_sec=60):
    total_orders = len(orders)
    
    prod = None
    retries = 10
    for attempt in range(retries):
        try:
            print(f"Attempting to connect to Kafka (Attempt {attempt+1}/{retries})...")
            prod = KafkaProducer(
                bootstrap_servers=kafka_nodes, 
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka broker!")
            break  # Exit the loop if connection is successful
        except Exception as e:
            print(f"Kafka is still booting up. Retrying in 5 seconds...")
            time.sleep(5)
            
    if prod is None:
        raise Exception("Fatal Error: Could not connect to Kafka after 10 attempts. Exiting.")

    for i in range(0, total_orders, chunk_size):
        # Isolate the current temporal micro-batch
        chunk_orders = orders.iloc[i : i + chunk_size]
        order_ids = chunk_orders['order_id'].unique()
        
        # Filter dependent tables for the current batch's order_ids
        chunk_items = items[items['order_id'].isin(order_ids)]
        chunk_payments = payments[payments['order_id'].isin(order_ids)]
        chunk_reviews = reviews[reviews['order_id'].isin(order_ids)]
        
        # Transactional Insertion Sequence (Enforces Foreign Key Constraints)
        #chunk_orders.astype(str).to_sql('orders', engine, if_exists='append', index=False)
        #chunk_items.astype(str).to_sql('order_items', engine, if_exists='append', index=False)
        #chunk_payments.astype(str).to_sql('order_payments', engine, if_exists='append', index=False)
        #chunk_reviews.astype(str).to_sql('order_reviews', engine, if_exists='append', index=False)
        
        data_to_send = {
            "topic_orders": chunk_orders,
            "topic_order_items": chunk_items,
            "topic_order_payments": chunk_payments,
            "topic_order_reviews": chunk_reviews
        }
        
        for topic_name, df in data_to_send.items():
            if not df.empty:
                records = df.astype(str).to_dict(orient='records') 
                
                for record in records:
                    prod.send(topic=topic_name, value=record).get(timeout=10)

        # Force the producer to empty its internal buffer to the network
        prod.flush()

        current_time = chunk_orders['order_purchase_timestamp'].max()
        save_last_ingested_time(current_time)
        print(f"[{current_time}] Streamed {len(chunk_orders)} orders and related entities. Sleeping {delay_sec}s...")
        
        time.sleep(delay_sec)

if __name__ == "__main__":
    print("Initializing Chronological Data Stream Simulation...")
    orders_df, items_df, payments_df, reviews_df = load_and_prep_data()
    last_time = get_last_ingested_time()
    if last_time:
        print(f"Found state file. Resuming stream after {last_time}...")
        orders_df = orders_df[orders_df['order_purchase_timestamp'] > last_time]
    else:
        print("No prior state found. Starting from the beginning...")

    stream_transactions(orders_df, items_df, payments_df, reviews_df)