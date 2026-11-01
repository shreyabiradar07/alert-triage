#!/usr/bin/env bash
# What is running, what Prometheus can scrape, what is firing, and what triage has received.
cd "$(dirname "$0")/.." || exit 1
docker compose -f chaos/docker-compose.yml ps --format '{{.Service}}: {{.State}}' | paste -sd' ' -
echo "--- scrape targets"
curl -s localhost:9090/api/v1/targets | python3 -c '
import json, sys
for t in json.load(sys.stdin)["data"]["activeTargets"]:
    print(" ", t["labels"]["service"], t["health"], t.get("lastError", ""))'
echo "--- alerts in Prometheus"
curl -s localhost:9090/api/v1/alerts | python3 -c '
import json, sys
alerts = json.load(sys.stdin)["data"]["alerts"]
for a in alerts:
    print(" ", a["state"], a["labels"]["alertname"], a["labels"].get("service", ""))
print("  (none)" if not alerts else "")'
echo "--- triage"
curl -s localhost:9099/alerts.json | python3 -c '
import json, sys
rows = json.load(sys.stdin)
print("  %d alerts" % len(rows))
for r in rows:
    a = r["alert"]
    score = "unscored" if r["actionable"] is None else "%.2f %s" % (r["actionable"], r["band"])
    print("  #%d %-20s %-8s %-15s %-10s %s" % (r["rank"], score, a.get("status"), a["labels"]["alertname"],
          a["labels"].get("service", ""), "; ".join(r["annotations"])))'
