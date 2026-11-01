"""Post the simulated alerts and deploys to a running receiver, in time order.

Alerts go one webhook per alert, the way Alertmanager would deliver them over time.
Every fifth alert is sent a second time and then resolved, to exercise repeats.

    python -m experiments.replay             # first 40 alerts
    python -m experiments.replay --limit 300     # all of them
"""

import argparse
import json
import urllib.request

from alert_triage.triage import DATA, load_alerts


def post(url: str, body: dict) -> dict:
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:9099")
    parser.add_argument("--limit", type=int, default=40)
    args = parser.parse_args()

    alerts = load_alerts(DATA / "alerts.json")[:args.limit]
    deploys = [d for d in json.loads((DATA / "deploys.json").read_text()) if d["at"] <= alerts[-1]["startsAt"]]
    events = sorted([(d["at"], "deploy", d) for d in deploys] + [(a["startsAt"], "alert", a) for a in alerts],
                    key=lambda e: e[:2])
    new = sent = 0
    for n, (_, kind, item) in enumerate(events):
        if kind == "deploy":
            post(f"{args.url}/deploys", item)
            continue
        payload = {"version": "4", "receiver": "alert-triage", "status": "firing", "alerts": [item]}
        new += post(f"{args.url}/alerts", payload)["new"]
        sent += 1
        if n % 5 == 0:
            new += post(f"{args.url}/alerts", payload)["new"]
            resolved = {**item, "status": "resolved", "endsAt": item["startsAt"]}
            new += post(f"{args.url}/alerts", {**payload, "status": "resolved", "alerts": [resolved]})["new"]
    print(f"sent {sent} alerts and {len(deploys)} deploys; the receiver counted {new} new alerts. Open {args.url}/")


if __name__ == "__main__":
    main()
