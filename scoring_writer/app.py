"""Persist Kafka scores with business dimensions and expose committed-write metrics."""
import json
import logging
import os
import time

import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer
from prometheus_client import start_http_server

from metrics import DUPLICATES, ERRORS, recorded

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_pass"),
        "options": "-c timezone=UTC",
    }


def create_table(conn):
    """Add columns/indexes without resetting existing scores; provision a read-only Grafana role."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id BIGSERIAL PRIMARY KEY, transaction_id TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL, fraud_flag INTEGER NOT NULL,
                model_version TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS model_version TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS us_state TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS merch TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS cat_id TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS event_time TIMESTAMPTZ;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;
            CREATE INDEX IF NOT EXISTS scores_transaction_idx ON scores(transaction_id);
            CREATE INDEX IF NOT EXISTS scores_created_idx ON scores(created_at DESC);
            CREATE INDEX IF NOT EXISTS scores_dimensions_idx ON scores(us_state, merch, created_at DESC);
        """)
        role = os.getenv("GRAFANA_DB_USER", "grafana_ro")
        password = os.getenv("GRAFANA_DB_PASSWORD", "local_grafana_ro")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)))
        cur.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(sql.Identifier(role)))
        cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
        cur.execute(sql.SQL("GRANT SELECT ON scores TO {}").format(sql.Identifier(role)))
    conn.commit()


def insert_score(conn, data):
    """Store once per transaction id; retries do not change metrics or duplicate rows."""
    score = float(data["score"])
    flag = int(data["fraud_flag"])
    if not 0 <= score <= 1 or flag not in (0, 1):
        raise ValueError("Invalid score or fraud flag")
    started = time.perf_counter()
    with conn.cursor() as cur:
        # Serialize duplicate ids across writer processes without removing legacy rows.
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (data["transaction_id"],))
        cur.execute("""
            INSERT INTO scores (
                transaction_id, score, fraud_flag, model_version,
                us_state, merch, cat_id, amount, event_time, scored_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM scores WHERE transaction_id = %s)
            RETURNING id
        """, (data["transaction_id"], score, flag, data.get("model_version"),
              data.get("us_state"), data.get("merch"), data.get("cat_id"),
              data.get("amount"), data.get("event_time"), data.get("scored_at"),
              data["transaction_id"]))
        inserted = cur.fetchone() is not None
    conn.commit()
    if inserted:
        recorded(score, flag, time.perf_counter() - started)
    else:
        DUPLICATES.inc()
    return inserted


def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "scoring-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    conn = psycopg2.connect(**get_db_config())
    create_table(conn)
    consumer.subscribe([os.getenv("KAFKA_SCORING_TOPIC", "scoring")])
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            try:
                batch = json.loads(msg.value().decode("utf-8"))
                if not isinstance(batch, list) or not batch:
                    raise ValueError("Expected a nonempty list of scores")
                for row in batch:
                    insert_score(conn, row)
                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                conn.rollback()
                ERRORS.inc()
                logger.exception("Writer failed; offset is not advanced")
                raise
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))
    run_consumer()
