"""Writer metrics count only committed new rows, not retried duplicates."""
import time
from prometheus_client import Counter, Gauge, Histogram

WRITTEN = Counter("fraud_writer_scores_total", "Committed new scores")
FRAUD = Counter("fraud_writer_fraud_total", "Committed new fraud flags")
LATENCY = Histogram("fraud_writer_seconds", "Database transaction duration", buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2))
SCORES = Histogram("fraud_writer_score", "Committed score distribution", buckets=tuple(i / 20 for i in range(21)))
ERRORS = Counter("fraud_writer_errors_total", "Failed write or decode attempts")
DUPLICATES = Counter("fraud_writer_duplicates_total", "Redeliveries already stored")
LAST_SUCCESS = Gauge("fraud_writer_last_success_timestamp_seconds", "Unix time of last committed new score")


def recorded(score, flag, elapsed):
    WRITTEN.inc()
    FRAUD.inc(int(flag))
    LATENCY.observe(elapsed)
    SCORES.observe(score)
    LAST_SUCCESS.set(time.time())
