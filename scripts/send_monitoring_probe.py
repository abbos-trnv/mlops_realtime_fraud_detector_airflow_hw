"""Send real CSV transactions to Kafka; use --fraud-only to exercise the fraud alert.

Run inside transaction_simulator with docker compose exec. No score/metric is
forged: the normal scoring service computes every result using the active model.
"""
import argparse
import csv
import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="/data/train.csv")
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--interval", type=float, default=.1)
    parser.add_argument("--fraud-only", action="store_true")
    args = parser.parse_args()
    rows = []
    with open(args.csv, newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            if not args.fraud_only or int(row.get("target", 0)) == 1:
                for key in ("amount", "lat", "lon", "merchant_lat", "merchant_lon", "population_city", "post_code"):
                    row[key] = float(row[key])
                row.pop("target", None)
                rows.append(row)
            if len(rows) >= 100:
                break
    if not rows:
        raise SystemExit("No matching input rows")
    producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda x: json.dumps(x).encode())
    for i in range(args.count):
        data = dict(rows[i % len(rows)])
        data["transaction_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        producer.send("transactions", {"transaction_id": str(uuid.uuid4()), "emitted_at": datetime.now(timezone.utc).isoformat(), "data": data}).get(timeout=20)
        time.sleep(args.interval)
    producer.close()
    print(f"Sent {args.count} transactions for actual scoring")


if __name__ == "__main__":
    main()
