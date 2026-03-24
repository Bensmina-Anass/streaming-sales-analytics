import os
import json
import time
import clickhouse_connect

import math
from confluent_kafka import Consumer, KafkaException, TopicPartition

from collections import defaultdict
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_DB       = os.getenv("CH_DB", "ecommerce_dw")        # ← was "olist"
CH_USER     = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin")

TOPICS      = ["topic_orders", "topic_order_items", "topic_order_payments", "topic_order_reviews"]
BATCH_SIZE  = 500
FLUSH_EVERY = 10

# ── Schema: (column_name, type, nullable, default_if_missing) ────────────────
# Types: "str", "float", "int", "datetime"
SCHEMA = {
    "topic_orders": [
        ("order_id",                      "str",      False, ""),
        ("customer_id",                   "str",      False, ""),
        ("order_status",                  "str",      False, ""),
        ("order_purchase_timestamp",      "datetime", False, None),
        ("order_approved_at",             "datetime", True,  None),
        ("order_delivered_carrier_date",  "datetime", True,  None),
        ("order_delivered_customer_date", "datetime", True,  None),
        ("order_estimated_delivery_date", "datetime", True,  None),
    ],
    "topic_order_items": [
        ("order_id",            "str",      False, ""),
        ("order_item_id",       "int",      False, 0),
        ("product_id",          "str",      False, ""),
        ("seller_id",           "str",      False, ""),
        ("shipping_limit_date", "datetime", False, None),
        ("price",               "float",    False, 0.0),
        ("freight_value",       "float",    False, 0.0),
    ],
    "topic_order_payments": [
        ("order_id",             "str",   False, ""),
        ("payment_sequential",   "int",   False, 0),
        ("payment_type",         "str",   False, ""),
        ("payment_installments", "int",   False, 0),
        ("payment_value",        "float", False, 0.0),
    ],
    "topic_order_reviews": [
        ("review_id",               "str",      False, ""),
        ("order_id",                "str",      False, ""),
        ("review_score",            "int",      False, 0),
        ("review_comment_title",    "str",      False, ""),   # non-nullable String → default ""
        ("review_comment_message",  "str",      False, ""),   # non-nullable String → default ""
        ("review_creation_date",    "datetime", False, None),
        ("review_answer_timestamp", "datetime", True,  None),
    ],
}

TABLE_MAP = {
    "topic_orders":         "bronze_orders",          # ← prefixed
    "topic_order_items":    "bronze_order_items",
    "topic_order_payments": "bronze_order_payments",
    "topic_order_reviews":  "bronze_order_reviews",
}

DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]

# ── Type Casting ──────────────────────────────────────────────────────────────
def parse_datetime(v: str):
    for fmt in DT_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    return None  # unparseable → NULL


def cast(value, dtype: str, nullable: bool, default):
    # Catch both string "nan" AND actual float nan
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None if nullable else default
    if isinstance(value, str) and value in ("nan", "None", "NaT", ""):
        return None if nullable else default

    try:
        if dtype == "str":
            return str(value)
        elif dtype == "int":
            return int(float(value))
        elif dtype == "float":
            return float(value)
        elif dtype == "datetime":
            return parse_datetime(str(value))
    except (ValueError, TypeError):
        return None if nullable else default

def record_to_row(record: dict, schema: list) -> list:
    return [cast(record.get(col, None), dtype, nullable, default)
            for col, dtype, nullable, default in schema]


def get_columns(schema: list) -> list:
    return [col for col, *_ in schema]


# ── Connection Helpers ────────────────────────────────────────────────────────
def connect_clickhouse(retries: int = 15, delay: int = 5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to ClickHouse (attempt {attempt}/{retries})...")
            client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT,
                username=CH_USER, password=CH_PASSWORD,
                database=CH_DB,
            )
            client.ping()
            print("ClickHouse connected!")
            return client
        except Exception as e:
            print(f"ClickHouse not ready: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Fatal: Could not connect to ClickHouse.")


def create_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "clickhouse-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    for attempt in range(1, 16):
        try:
            print(f"Connecting to Redpanda (attempt {attempt}/15)...")
            c = Consumer(conf)
            c.list_topics(timeout=5)
            print("Redpanda connected!")
            return c
        except KafkaException as e:
            print(f"Broker not ready: {e}. Retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Fatal: Could not connect to Redpanda.")


# ── Flush ─────────────────────────────────────────────────────────────────────
def flush_buffer(ch_client, buffers: dict, offsets: dict, consumer: Consumer):
    for topic, rows in buffers.items():
        if not rows:
            continue
        table = TABLE_MAP[topic]
        cols  = get_columns(SCHEMA[topic])
        try:
            ch_client.insert(table, rows, column_names=cols)
            print(f"[CH] Inserted {len(rows)} rows → {CH_DB}.{table}")
        except Exception as e:
            print(f"[ERROR] Insert failed for {table}: {e}")
            return  # don't commit, messages will replay

    
    consumer.commit(offsets=list(offsets.values()))

    for topic in buffers:
        buffers[topic].clear()
    offsets.clear()



# ── Main Loop ─────────────────────────────────────────────────────────────────
def run():
    ch_client  = connect_clickhouse()
    consumer   = create_consumer()
    consumer.subscribe(TOPICS)

    buffers    = defaultdict(list)
    offsets    = {}
    last_flush = time.time()

    print(f"Subscribed to: {TOPICS}")
    print(f"Flushing every {BATCH_SIZE} rows/topic or every {FLUSH_EVERY}s")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.time()

            if msg is not None and not msg.error():
                topic  = msg.topic()
                record = json.loads(msg.value().decode("utf-8"))
                row    = record_to_row(record, SCHEMA[topic])

                buffers[topic].append(row)
                offsets[(msg.topic(), msg.partition())] = TopicPartition(
    msg.topic(), msg.partition(), msg.offset() + 1
)

            elif msg is not None and msg.error():
                print(f"[ERROR] {msg.error()}")

            should_flush = (
                any(len(rows) >= BATCH_SIZE for rows in buffers.values())
                or (now - last_flush >= FLUSH_EVERY and any(buffers.values()))
            )

            if should_flush:
                flush_buffer(ch_client, buffers, offsets, consumer)
                last_flush = now

    except KeyboardInterrupt:
        print("Flushing remaining buffer before shutdown...")
        flush_buffer(ch_client, buffers, offsets, consumer)
        print("Shutdown complete.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
