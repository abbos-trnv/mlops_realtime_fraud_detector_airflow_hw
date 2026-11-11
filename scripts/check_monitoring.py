"""Read-only integration checks against the live monitoring stack; no extra packages."""
import base64
import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler

OPENER = build_opener(ProxyHandler({}))
GRAFANA = os.getenv("GRAFANA_URL", "http://127.0.0.1:3000")
PROM = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090")


def fetch(url, body=None, auth=False):
    headers = {}
    if auth:
        pair = os.getenv("GRAFANA_ADMIN_USER", "admin") + ":" + os.getenv("GRAFANA_ADMIN_PASSWORD", "local_grafana_admin")
        headers["Authorization"] = "Basic " + base64.b64encode(pair.encode()).decode()
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(url, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    with OPENER.open(request, timeout=30) as response:
        return json.load(response)


def query(expr):
    result = fetch(PROM + "/api/v1/query?" + urlencode({"query": expr}))
    assert result["status"] == "success", result
    return result["data"]["result"]


def eventually(check, timeout=180):
    start = time.monotonic()
    error = None
    while time.monotonic() - start < timeout:
        try:
            return check()
        except Exception as exc:
            error = exc
            time.sleep(3)
    raise AssertionError(f"Live stack did not become ready: {error}")


def main():
    def ready():
        targets = fetch(PROM + "/api/v1/targets")["data"]["activeTargets"]
        assert len(targets) == 3 and all(t["health"] == "up" for t in targets), targets
        count = query("fraud_writer_scores_total")
        assert count and float(count[0]["value"][1]) > 5, count
        return float(count[0]["value"][1])
    before = eventually(ready)
    def growing():
        current = float(query("fraud_writer_scores_total")[0]["value"][1])
        assert current > before, (before, current)
        return current
    after = eventually(growing, 60)
    checks = []
    for file in sorted(Path("monitoring/grafana/dashboards").glob("*.json")):
        dashboard = json.loads(file.read_text(encoding="utf-8"))
        live = fetch(GRAFANA + "/api/dashboards/uid/" + dashboard["uid"], auth=True)
        assert live["dashboard"]["uid"] == dashboard["uid"]
        for panel in dashboard["panels"]:
            target = dict(panel["targets"][0])
            target["datasource"] = panel["datasource"]
            target.update(intervalMs=10000, maxDataPoints=300)
            if "rawSql" in target:
                target["rawSql"] = target["rawSql"].replace("${state:sqlstring}", "'__all'").replace("${merchant:sqlstring}", "'__all'").replace("$__interval_ms", "10000").replace("$__interval", "10s")
            else:
                result = query(target["expr"])
                assert result, (file.name, panel["title"], "empty Prometheus panel")
                checks.append(panel["title"])
                continue
            end = int(time.time() * 1000)
            result = fetch(GRAFANA + "/api/ds/query", {"from": str(end - 900000), "to": str(end), "queries": [target]}, auth=True)
            response = result["results"]["A"]
            assert not response.get("error"), response
            frames = response.get("frames", [])
            assert any(f.get("data", {}).get("values") and len(f["data"]["values"][0]) for f in frames), (panel["title"], response)
            checks.append(panel["title"])
    rules = fetch(GRAFANA + "/api/v1/provisioning/alert-rules", auth=True)
    assert len(rules) >= 2, rules
    result = {
        "writer_before": before, "writer_after": after,
        "panels_with_data": checks, "provisioned_alerts": [r["uid"] for r in rules],
        "model_loads": query("fraud_model_reload_total"),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
