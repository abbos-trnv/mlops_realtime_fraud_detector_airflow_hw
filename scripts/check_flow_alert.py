"""Temporarily pause the input source and verify Grafana's provisioned alert fires."""
import json
from pathlib import Path
import subprocess

from check_monitoring import eventually, fetch, GRAFANA


def find_alert():
    response = fetch(GRAFANA + "/api/prometheus/grafana/api/v1/rules", auth=True)
    rules = [rule for group in response["data"]["groups"] for rule in group["rules"]]
    rule = next(rule for rule in rules if rule.get("grafana_alert", {}).get("uid") == "fraud-flow-stopped")
    assert rule["state"] == "firing", rule
    return {"name": rule["name"], "state": rule["state"], "alerts": rule.get("alerts", [])}


def main():
    subprocess.run(["docker", "compose", "pause", "transaction_simulator"], check=True)
    try:
        alert = eventually(find_alert, 180)
        Path("monitoring/evidence").mkdir(parents=True, exist_ok=True)
        Path("monitoring/evidence/flow-alert.json").write_text(json.dumps(alert, indent=2), encoding="utf-8")
        print(json.dumps(alert, indent=2))
    finally:
        subprocess.run(["docker", "compose", "unpause", "transaction_simulator"], check=True)


if __name__ == "__main__":
    main()
