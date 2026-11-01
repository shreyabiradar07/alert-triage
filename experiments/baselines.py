"""What you get without a model: the severity label, a few hand-written rules, and grouping by label.

These stand in for a tuned Prometheus and Alertmanager setup. They need no Jev requests.
The rules were written by the same person who wrote the simulator, so they fit this data
better than rules written for a real system would. Read them as a strong baseline.
"""

import re

from alert_triage.triage import context_for


def severity(alert: dict) -> float:
    """Sort by the severity label the rule already carries. Zero extra work."""
    return {"page": 1.0, "critical": 1.0, "warning": 0.5}.get(alert["labels"].get("severity"), 0.0)


def rules(alert: dict) -> float:
    """Five rules an on-call team would plausibly write: route staging away, tier a few thresholds."""
    labels, text = alert["labels"], alert["annotations"]["description"]
    if labels.get("env") != "prod":
        return 0.0                                    # staging goes to a channel, not the pager
    if labels.get("service") == "batch-reports":
        return 0.1                                    # the nightly job is known to run hot
    if labels["alertname"] == "CertExpiringSoon":
        return 0.9 if int(re.search(r"in (\d+) days", text).group(1)) <= 7 else 0.1
    if labels.get("severity") == "page":
        return 1.0
    if labels["alertname"] == "HighLatencyP99" and float(re.search(r"is ([\d.]+)s", text).group(1)) >= 2:
        return 0.7                                    # a second, higher latency threshold
    return 0.5


def same_service(k: int, alerts: list[dict], deploys: list[dict]) -> float:
    """Alertmanager's group_by: [service]. Same event if a recent alert carries the same service label."""
    service = alerts[k]["labels"].get("service")
    return float(any(a["labels"].get("service") == service for a in context_for(k, alerts, deploys)[0]))


def any_recent(k: int, alerts: list[dict], deploys: list[dict]) -> float:
    """Group everything that fires within the window, whatever its labels."""
    return float(bool(context_for(k, alerts, deploys)[0]))
