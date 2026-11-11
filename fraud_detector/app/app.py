"""Kafka scorer with acknowledged delivery, CPU inference and Prometheus metrics."""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

sys.path.insert(0, os.path.abspath("./src"))
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred
from metrics import ERRORS, recorded

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "ml-scorer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe([os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")])
        self.producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        })
        self.output_topic = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

    def process_message(self, msg):
        started = time.perf_counter()
        try:
            message = json.loads(msg.value().decode("utf-8"))
            raw = message["data"]
            processed = run_preproc(pd.DataFrame([raw]))
            submission, probabilities, version = make_pred(processed, "kafka_stream")
            score = float(probabilities[0])
            if not 0 <= score <= 1:
                raise ValueError("Model returned an invalid score")
            submission["transaction_id"] = message["transaction_id"]
            submission["model_version"] = version
            for field in ("us_state", "merch", "cat_id", "amount"):
                submission[field] = raw.get(field)
            submission["event_time"] = raw.get("transaction_time")
            submission["scored_at"] = datetime.now(timezone.utc).isoformat()
            delivery_errors = []
            def delivered(error, _message):
                if error:
                    delivery_errors.append(str(error))
            self.producer.produce(
                self.output_topic, key=message["transaction_id"],
                value=submission.to_json(orient="records"), on_delivery=delivered,
            )
            remaining = self.producer.flush(15)
            if remaining or delivery_errors:
                raise RuntimeError(f"Output not acknowledged: {remaining}, {delivery_errors}")
            event_age = None
            if message.get("emitted_at"):
                emitted = datetime.fromisoformat(message["emitted_at"])
                event_age = (datetime.now(timezone.utc) - emitted).total_seconds()
            recorded(score, time.perf_counter() - started, event_age)
            self.consumer.commit(message=msg, asynchronous=False)
            return True
        except Exception:
            ERRORS.inc()
            logger.exception("Scoring failed; offset is not advanced")
            # Fail-stop prevents a later commit from skipping the failed offset.
            raise

    def process_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(msg.error())
                self.process_message(msg)
        finally:
            self.consumer.close()


if __name__ == "__main__":
    start_http_server(int(os.getenv("METRICS_PORT", "8000")))
    ensure_bootstrap()
    ProcessingService().process_messages()
