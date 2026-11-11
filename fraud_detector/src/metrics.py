"""Bounded-cardinality metrics for successful Kafka scoring and real model loads."""
import time

from prometheus_client import Counter, Gauge, Histogram

SCORE_BUCKETS = tuple(i / 20 for i in range(21))
PROCESSED = Counter("fraud_scored_transactions_total", "Scores acknowledged by Kafka")
FRAUD = Counter("fraud_scored_fraud_total", "Acknowledged scores above the fraud threshold")
LATENCY = Histogram("fraud_scoring_seconds", "Preprocessing, CPU inference and Kafka delivery", buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5))
SCORES = Histogram("fraud_scoring_score", "Distribution of acknowledged scores", buckets=SCORE_BUCKETS)
ERRORS = Counter("fraud_scoring_errors_total", "Failed scoring or Kafka delivery attempts")
LAST_SUCCESS = Gauge("fraud_scoring_last_success_timestamp_seconds", "Unix time of last acknowledged score")
RATIO = Gauge("fraud_scoring_fraud_ratio", "Fraud fraction since process start")
EVENT_AGE = Histogram("fraud_event_age_seconds", "Time from simulator emission to acknowledged score", buckets=(.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300))
MODEL_LOADS = Counter("fraud_model_reload_total", "Actual model file loads", ["version"])
MODEL_LOADED_AT = Gauge("fraud_model_loaded_at_seconds", "Unix time of actual model file load", ["version"])
MODEL_LOAD_ERRORS = Counter("fraud_model_load_errors_total", "Failed model reloads")
_total = 0
_fraud = 0


def model_loaded(version):
    """Called only after a model file was successfully loaded, including bootstrap."""
    MODEL_LOADS.labels(version=version).inc()
    MODEL_LOADED_AT.labels(version=version).set(time.time())


def recorded(score, elapsed, event_age=None):
    """Update only after the output broker acknowledged delivery."""
    global _total, _fraud
    _total += 1
    _fraud += int(score > .5)
    PROCESSED.inc()
    FRAUD.inc(int(score > .5))
    SCORES.observe(score)
    LATENCY.observe(elapsed)
    LAST_SUCCESS.set(time.time())
    RATIO.set(_fraud / _total)
    if event_age is not None:
        EVENT_AGE.observe(max(0, event_age))
