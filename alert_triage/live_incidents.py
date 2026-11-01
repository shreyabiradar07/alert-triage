"""Incidents as alerts arrive: Alertmanager's own groups, kept for the receiver's page.

Every webhook payload is one Alertmanager group, named by its groupKey and described by its
groupLabels (the route's group_by), so the team's own grouping is used whatever labels it is built on.
An alert joins the open group with the same key, or opens a new one when that group has been quiet for
GROUP_GAP. A payload without a groupKey (not from Alertmanager) is grouped by the labels all its alerts
share, which is how Alertmanager computes commonLabels. No model is involved.

Jev merging of groups that share a cause was tried and removed: on ITBench-Lite it kept at most a
quarter of a fault's alert pairs together, and on one half of the scenarios it did no better than
Alertmanager's groups alone. README.md, "Incidents: Jev merging", has the numbers.
"""

import json
import threading
from datetime import timedelta

from alert_triage.triage import when

GROUP_GAP = timedelta(minutes=15)


def payload_group(payload: dict) -> tuple[str, dict]:
    """(key, labels) of the Alertmanager group a webhook payload belongs to."""
    if key := payload.get("groupKey"):
        return key, dict(payload.get("groupLabels") or {})
    alerts = payload.get("alerts") or []
    labels = payload.get("commonLabels") or (
        {k: v for k, v in alerts[0].get("labels", {}).items()
         if all(a.get("labels", {}).get(k) == v for a in alerts)} if alerts else {})
    return json.dumps(sorted(labels.items())), labels


def describe_group(labels: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(labels.items())) or "(no group labels)"


class LiveIncidents:
    """The Alertmanager groups received so far, one incident each."""

    def __init__(self):
        self.groups: list[dict] = []  # {"key", "labels", "alerts", "last"}
        self.lock = threading.Lock()

    def receive(self, alerts: list[dict], group: tuple[str, dict] | None = None):
        """Place the new alerts of one webhook payload, oldest first. group is payload_group(payload);
        without it the alerts are grouped by the labels they all share."""
        key, labels = group or payload_group({"alerts": alerts})
        with self.lock:
            for alert in sorted(alerts, key=lambda a: a["startsAt"]):
                t = when(alert["startsAt"])
                open_group = next((g for g in reversed(self.groups)
                                   if g["key"] == key and t - g["last"] <= GROUP_GAP), None)
                if open_group:
                    open_group["alerts"].append(alert)
                    open_group["last"] = max(open_group["last"], t)
                else:
                    self.groups.append({"key": key, "labels": labels, "alerts": [alert], "last": t})

    def view(self, with_alerts: bool = False) -> list[dict]:
        """Incidents, firing ones first, newest first. with_alerts adds the alert dicts under "members",
        for the receiver's page."""
        with self.lock:
            out = [{"incident": n + 1,
                    "started": min(a["startsAt"] for a in g["alerts"]),
                    "firing": sum(a.get("status") != "resolved" for a in g["alerts"]),
                    "alerts": len(g["alerts"]),
                    "group": describe_group(g["labels"]),
                    "labels": dict(g["labels"]),
                    "alertnames": [a["labels"].get("alertname") for a in g["alerts"]],
                    **({"members": list(g["alerts"])} if with_alerts else {})}
                   for n, g in enumerate(self.groups)]
        out.sort(key=lambda i: i["started"], reverse=True)
        out.sort(key=lambda i: i["firing"] == 0)
        return out
