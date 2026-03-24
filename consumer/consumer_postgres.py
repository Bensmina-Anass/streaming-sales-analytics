import os
import json
import time
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaException, TopicPartition
import math
from collections import deque


def make_tp(msg) -> TopicPartition:
    if msg is None:
        return None
    return TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)


# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
PG_DSN = {
    "host":     os.getenv("PG_HOST", "postgres"),
    "port":     os.getenv("PG_PORT", "5432"),
    "dbname":   os.getenv("PG_DB", "e-commerce"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}
TOPICS = ["topic_orders", "topic_order_items", "topic_order_payments", "topic_order_reviews"]

# FK-safe insertion order: parent tables must come before child tables
TOPIC_INSERT_ORDER = [
    "topic_orders",          # parent — no FK deps
    "topic_order_items",     # FK → orders
    "topic_order_payments",  # FK → orders
    "topic_order_reviews",   # FK → orders
]

MAX_RETRIES   = 10   # max times a record can be retried before being dead-lettered
RETRY_DELAY   = 2.0  # seconds to wait before retrying FK-failed records
POLL_TIMEOUT  = 0.05 # short poll so we drain quickly and re-sort each cycle

# ── Upserts ───────────────────────────────────────────────────────────────────
UPSERTS = {
    "topic_orders": """
        INSERT INTO orders (
            order_id, customer_id, order_status, order_purchase_timestamp,
            order_approved_at, order_delivered_carrier_date,
            order_delivered_customer_date, order_estimated_delivery_date
        ) VALUES (
            %(order_id)s, %(customer_id)s, %(order_status)s, %(order_purchase_timestamp)s,
            %(order_approved_at)s, %(order_delivered_carrier_date)s,
            %(order_delivered_customer_date)s, %(order_estimated_delivery_date)s
        ) ON CONFLICT (order_id) DO NOTHING
    """,
    "topic_order_items": """
        INSERT INTO order_items (
            order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value
        ) VALUES (
            %(order_id)s, %(order_item_id)s, %(product_id)s, %(seller_id)s,
            %(shipping_limit_date)s, %(price)s, %(freight_value)s
        ) ON CONFLICT (order_id, order_item_id) DO NOTHING
    """,
    "topic_order_payments": """
        INSERT INTO order_payments (
            order_id, payment_sequential, payment_type,
            payment_installments, payment_value
        ) VALUES (
            %(order_id)s, %(payment_sequential)s, %(payment_type)s,
            %(payment_installments)s, %(payment_value)s
        ) ON CONFLICT (order_id, payment_sequential) DO NOTHING
    """,
    "topic_order_reviews": """
    INSERT INTO order_reviews (
        review_id, order_id, review_score, review_comment_title,
        review_comment_message, review_creation_date, review_answer_timestamp
    ) VALUES (
        %(review_id)s, %(order_id)s, %(review_score)s, %(review_comment_title)s,
        %(review_comment_message)s, %(review_creation_date)s, %(review_answer_timestamp)s
    ) ON CONFLICT (review_id, order_id) DO NOTHING
    """,

}

# ── Helpers ───────────────────────────────────────────────────────────────────

def clean(record: dict) -> dict:
    NULL_VALUES = {"nan", "NaN", "None", "NaT", "none", "null", "NULL", ""}
    result = {}
    for k, v in record.items():
        if isinstance(v, float) and math.isnan(v):
            result[k] = None
        elif isinstance(v, str) and v in NULL_VALUES:
            result[k] = None
        else:
            result[k] = v
    return result

def is_fk_violation(e: psycopg2.Error) -> bool:
    return e.pgcode == "23503"   # foreign_key_violation


def connect_pg(retries: int = 15, delay: int = 5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to PostgreSQL (attempt {attempt}/{retries})...")
            conn = psycopg2.connect(**PG_DSN)
            conn.autocommit = False
            print("PostgreSQL connected!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"PG not ready: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Fatal: Could not connect to PostgreSQL.")


def create_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "postgres-consumer-group",
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


# ── Insert (single record) ────────────────────────────────────────────────────
def insert_record(conn, topic: str, record: dict) -> str:
    sql = UPSERTS[topic]
    try:
        with conn.cursor() as cur:
            cur.execute(sql, record)
        conn.commit()
        return "ok"
    except psycopg2.Error as e:
        conn.rollback()
        if is_fk_violation(e):
            return "fk_violation"
        # Print first 4 fields and the exact error to expose silent drops
        preview = {k: v for k, v in list(record.items())[:4]}
        print(f"[ERROR] topic='{topic}' error={e.pgcode} msg={e.pgerror.strip()}")
        print(f"[ERROR] record preview: {preview}")
        if conn.closed:
            raise
        return "error"



# ── Main Loop ─────────────────────────────────────────────────────────────────
def run():
    conn     = connect_pg()
    consumer = create_consumer()
    consumer.subscribe(TOPICS)

    # retry_queue entries: {"topic": str, "record": dict, "msg": msg, "attempts": int, "retry_after": float}
    retry_queue: deque = deque()
    # dead_letter: records that exceeded MAX_RETRIES
    dead_letter: list  = []

    print(f"Subscribed to topics: {TOPICS}")
    print("Waiting for messages...")

    try:
        while True:

            # ── 1. Drain retry queue first (process pending FK retries) ───────
            now = time.time()
            pending = []
            while retry_queue:
                entry = retry_queue.popleft()
                if now < entry["retry_after"]:
                    pending.append(entry)   # not ready yet, put back
                    continue

                result = insert_record(conn, entry["topic"], entry["record"])
                if result == "ok":
                    tp = make_tp(entry["msg"])
                    if tp:
                        consumer.commit(offsets=[tp])

                elif result == "fk_violation":
                    entry["attempts"] += 1
                    if entry["attempts"] >= MAX_RETRIES:
                        print(f"[DEAD LETTER] Giving up on order_id={entry['record'].get('order_id')} "
                              f"after {MAX_RETRIES} attempts.")
                        dead_letter.append(entry)
                    else:
                        entry["retry_after"] = time.time() + RETRY_DELAY
                        pending.append(entry)
                # "error" → drop silently (already logged in insert_record)

            retry_queue.extend(pending)

            # ── 2. Poll a batch of raw messages ──────────────────────────────
            # Collect up to 200 messages across topics, then re-order before inserting
            batch: dict[str, list] = {t: [] for t in TOPIC_INSERT_ORDER}

            for _ in range(200):
                msg = consumer.poll(timeout=POLL_TIMEOUT)
                if msg is None:
                    break
                if msg.error():
                    print(f"[ERROR] {msg.error()}")
                    continue
                batch[msg.topic()].append(msg)

            # ── 3. Insert in FK-safe order within the batch ───────────────────
            for topic in TOPIC_INSERT_ORDER:
                for msg in batch[topic]:
                    record = clean(json.loads(msg.value().decode("utf-8")))
                    result = insert_record(conn, topic, record)

                    if result == "ok":
                        tp = make_tp(msg)
                        if tp:
                            consumer.commit(offsets=[tp])
                    elif result == "fk_violation":
                        # Parent not yet in DB — defer with retry
                        retry_queue.append({
                            "topic":       topic,
                            "record":      record,
                            "msg":         msg,
                            "attempts":    1,
                            "retry_after": time.time() + RETRY_DELAY,
                        })
                    # "error" → skip, offset not committed → replayed on restart

            # Reconnect if connection dropped
            if conn.closed:
                conn = connect_pg()

    except KeyboardInterrupt:
        print(f"Shutting down. Retry queue size: {len(retry_queue)} | Dead letters: {len(dead_letter)}")
        if dead_letter:
            print("[DEAD LETTERS]:")
            for e in dead_letter:
                print(f"  topic={e['topic']} order_id={e['record'].get('order_id')}")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()
