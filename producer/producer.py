import pandas as pd
import time
import os
import json
from confluent_kafka import Producer, KafkaException

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
DATA_DIR = "/app/data/"
STATE_FILE = os.path.join(DATA_DIR, "producer_state.txt")


# ── Data Loading ──────────────────────────────────────────────────────────────
def load_and_prep_data():
    orders   = pd.read_csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"))
    items    = pd.read_csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"))
    payments = pd.read_csv(os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"))
    reviews  = pd.read_csv(os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"))

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
    orders = orders.sort_values("order_purchase_timestamp")

    return orders, items, payments, reviews


# ── State Management ──────────────────────────────────────────────────────────
def get_last_ingested_time():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            last_time = f.read().strip()
            if last_time:
                return pd.to_datetime(last_time)
    return None


def save_last_ingested_time(current_time):
    with open(STATE_FILE, "w") as f:
        f.write(str(current_time))


# ── Delivery Callback ─────────────────────────────────────────────────────────
def on_delivery(err, msg):
    if err:
        print(f"[ERROR] Delivery failed for topic '{msg.topic()}': {err}")
    # Uncomment for per-message confirmation:
    # else:
    #     print(f"Delivered → {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


# ── Producer Factory with Retry ───────────────────────────────────────────────
def create_producer(retries: int = 30, retry_delay: int = 5) -> Producer:
    """
    confluent-kafka connects lazily, so we probe with list_topics()
    to confirm the broker is reachable before returning the Producer.
    """
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "olist-producer",
        # Delivery reliability settings
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 300,
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to broker at {BOOTSTRAP_SERVERS} (attempt {attempt}/{retries})...")
            prod = Producer(conf)
            # Trigger an actual network call to verify connectivity
            prod.list_topics(timeout=5)
            print("Successfully connected to broker!")
            return prod
        except KafkaException as e:
            print(f"Broker not ready: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    raise RuntimeError(f"Fatal: Could not connect to broker after {retries} attempts.")


# ── Streaming Logic ───────────────────────────────────────────────────────────
def stream_transactions(orders, items, payments, reviews, chunk_size=100, delay_sec=60):
    prod = create_producer()
    total_orders = len(orders)

    for i in range(0, total_orders, chunk_size):
        chunk_orders = orders.iloc[i : i + chunk_size]
        order_ids    = chunk_orders["order_id"].unique()

        chunk_items    = items[items["order_id"].isin(order_ids)]
        chunk_payments = payments[payments["order_id"].isin(order_ids)]
        chunk_reviews  = reviews[reviews["order_id"].isin(order_ids)]

        topics = {
            "topic_orders":        chunk_orders,
            "topic_order_items":   chunk_items,
            "topic_order_payments": chunk_payments,
            "topic_order_reviews": chunk_reviews,
        }

        for topic_name, df in topics.items():
            if df.empty:
                continue

            for record in df.astype(str).to_dict(orient="records"):
                # confluent-kafka: serialization is manual, no value_serializer
                prod.produce(
                    topic=topic_name,
                    value=json.dumps(record).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                # poll(0) serves the delivery callback queue without blocking
                prod.poll(0)

        # flush() blocks until all queued messages are acknowledged
        prod.flush()

        current_time = chunk_orders["order_purchase_timestamp"].max()
        save_last_ingested_time(current_time)
        print(
            f"[{current_time}] Streamed {len(chunk_orders)} orders "
            f"and related entities. Sleeping {delay_sec}s..."
        )

        time.sleep(delay_sec)


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Initializing Chronological Data Stream Simulation...")

    orders_df, items_df, payments_df, reviews_df = load_and_prep_data()

    last_time = get_last_ingested_time()
    if last_time:
        print(f"Found state file. Resuming stream after {last_time}...")
        orders_df = orders_df[orders_df["order_purchase_timestamp"] > last_time]
    else:
        print("No prior state found. Starting from the beginning...")

    stream_transactions(orders_df, items_df, payments_df, reviews_df)
