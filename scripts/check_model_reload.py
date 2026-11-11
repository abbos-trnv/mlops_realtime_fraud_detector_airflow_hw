"""Execute the real Airflow retraining DAG, then assert hot reload and PostgreSQL lineage."""
import json
from pathlib import Path
import subprocess
import time

from check_monitoring import eventually, fetch, query


def docker(*args):
    return subprocess.check_output(["docker", "compose", "--profile", "training", *args], text=True).strip()


def main():
    def ready():
        health = fetch("http://127.0.0.1:8081/health")
        assert health["metadatabase"]["status"] == "healthy", health
    eventually(ready, 300)
    container = docker("ps", "-q", "fraud_detector")
    before = docker("exec", "-T", "fraud_detector", "cat", "/models/latest.txt")
    # Dags test runs every real task in graph order, persists the successful DAG run,
    # and does not require waiting for the next scheduled 5-minute tick.
    result = subprocess.run(
        ["docker", "compose", "--profile", "training", "exec", "-T", "airflow",
         "airflow", "dags", "test", "retrain_catboost", "2026-11-11"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600,
    )
    Path("monitoring/evidence").mkdir(parents=True, exist_ok=True)
    Path("monitoring/evidence/retrain.log").write_text(result.stdout, encoding="utf-8")
    assert result.returncode == 0, result.stdout[-6000:]
    after = docker("exec", "-T", "fraud_detector", "cat", "/models/latest.txt")
    assert after != before, ("DAG did not publish a new version", before, after)
    def reloaded():
        series = query('fraud_model_reload_total{version="' + after + '"}')
        assert series and float(series[0]["value"][1]) >= 1, series
        rows = docker("exec", "-T", "postgres", "psql", "-U", "fraud_user", "-d", "fraud_db", "-tAc",
                      "SELECT count(*) FROM scores WHERE model_version = '" + after.replace("'", "''") + "'")
        assert int(rows) > 0, rows
        return int(rows)
    count = eventually(reloaded, 90)
    assert docker("ps", "-q", "fraud_detector") == container, "Scorer container was replaced"
    annotation = query('fraud_model_loaded_at_seconds{version="' + after + '"} > (time() - 30)')
    assert annotation, "The 30 second model-change annotation is missing"
    evidence = {"previous_version": before, "published_version": after, "new_scores": count,
                "same_scorer_container": True, "annotation": annotation}
    Path("monitoring/evidence/model-reload.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
