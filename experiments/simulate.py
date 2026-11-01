"""Simulate three days of failures and write the alerts Alertmanager would send.

Faults and noise are injected on a small service topology. Each one fires the rules in
experiments/rules/alerts.rules.yml, and the alerts are written in Alertmanager's webhook format
(data/alerts.json), so triage.py reads the same shape a real Alertmanager would post.

The labels (data/labels.csv) come from what was injected: an alert is actionable when a
real fault produced it, and two alerts share an event when the same fault or deploy
produced both. Edit labels.csv by hand to override any of them; evaluate.py reads the file.

Values overlap on purpose: a p99 of 1.3s is noise on its own and a symptom during an
incident, and CPU at 92% is expected right after a deploy. The alert text alone should
not be enough to tell them apart.
"""

import csv
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 7
START = datetime(2026, 10, 5, tzinfo=timezone.utc)
HOURS = 72
N_INCIDENTS = 48
N_NOISE = 150
N_QUIET_DEPLOYS = 70
DATA = Path(__file__).parent.parent / "data"

APPS = ["checkout", "payments", "cart", "search"]
# service -> the services that call it (and so show symptoms when it fails)
CALLERS = {"payments": ["checkout"], "payments-db": ["payments"], "cart": ["checkout"],
           "redis-cart": ["cart"], "search-index": ["search"],
           "checkout": ["ingress-nginx"], "search": ["ingress-nginx"]}
SEVERITY = {"HighErrorRate": "page", "HighLatencyP99": "warning", "HighCPU": "warning",
            "PodCrashLooping": "page", "PodOOMKilled": "warning", "DBConnectionsSaturated": "page",
            "ReplicationLag": "warning", "DiskWillFillSoon": "warning", "NodeNotReady": "page",
            "TargetDown": "warning", "CertExpiringSoon": "warning"}

rng = random.Random(SEED)


def pod(service):
    return f"{service}-{rng.choice('abcdef')}{rng.randint(10, 99)}{rng.choice('xyz')}"


def alert(name, service, description, env="prod", **labels):
    return {"labels": {"alertname": name, "severity": SEVERITY[name], "service": service,
                       "env": env, **labels},
            "annotations": {"summary": f"{name} on {service}", "description": description}}


def errors(service, pct, env="prod"):
    return alert("HighErrorRate", service, f"5xx ratio for {service} is {pct:.1f}% over 5m (threshold 5%)", env)


def latency(service, secs, env="prod"):
    return alert("HighLatencyP99", service, f"p99 latency for {service} is {secs:.2f}s over 5m (threshold 1s)", env)


def cpu(service, pct):
    p = pod(service)
    return alert("HighCPU", service, f"CPU usage of {p} is {pct}% for 5m (threshold 85%)", pod=p)


def crashloop(service):
    p = pod(service)
    return alert("PodCrashLooping", service, f"{p} restarted {rng.randint(3, 9)} times in 10m", pod=p)


def oom(service):
    p = pod(service)
    return alert("PodOOMKilled", service, f"{p} was OOMKilled (memory limit {rng.choice([256, 512, 1024])}Mi)", pod=p)


def target_down(service, down, total):
    return alert("TargetDown", service, f"{down} of {total} {service} targets are down")


def disk(service, volume, pct, hours):
    return alert("DiskWillFillSoon", service,
                 f"volume {volume} is {pct}% full, predicted to fill in {hours}h (threshold 24h)", volume=volume)


def cert(host, days):
    return alert("CertExpiringSoon", "ingress-nginx", f"certificate for {host} expires in {days} days (threshold 30d)")


def deploy(service):
    return {"service": service, "version": f"v{rng.randint(1, 4)}.{rng.randint(0, 60)}.{rng.randint(0, 9)}",
            "summary": rng.choice(["dependency bumps", "new feature flag", "refactor request handlers",
                                   "config change", "bug fix", "library upgrade"])}


# Each scenario returns (steps, deploy or None). A step is (minutes after the event starts, alert).
# A deploy, when present, lands 2-6 minutes before the first alert.

def bad_deploy():
    s = rng.choice(APPS)
    steps = [(0, errors(s, rng.uniform(6, 60)))]
    if rng.random() < 0.6:
        steps.append((rng.uniform(1, 4), crashloop(s)))
    for up in CALLERS.get(s, []):
        if rng.random() < 0.7:
            steps.append((rng.uniform(1, 6), latency(up, rng.uniform(1.1, 5))))
    return steps, deploy(s)


def db_saturation():
    steps = [(0, alert("DBConnectionsSaturated", "payments-db",
                       f"payments-db is using {rng.randint(93, 100)}% of max_connections")),
             (rng.uniform(1, 4), latency("payments", rng.uniform(1.2, 6)))]
    if rng.random() < 0.7:
        steps.append((rng.uniform(2, 7), errors("checkout", rng.uniform(6, 40))))
    if rng.random() < 0.5:
        steps.append((rng.uniform(0, 3), cpu("payments-db", rng.randint(88, 99))))
    return steps, None


def memory_leak():
    s = rng.choice(APPS)
    steps = [(0, oom(s)), (rng.uniform(3, 8), oom(s)), (rng.uniform(5, 10), crashloop(s))]
    if rng.random() < 0.6:
        steps.append((rng.uniform(6, 12), errors(s, rng.uniform(5.5, 25))))
    return steps, None


def node_down():
    node = f"node-{rng.randint(1, 5)}"
    steps = [(0, alert("NodeNotReady", "kubelet", f"{node} has been NotReady for 3m", node=node))]
    for s in rng.sample(APPS + ["redis-cart"], rng.randint(1, 3)):
        steps.append((rng.uniform(0.5, 3), target_down(s, 1, 3)))
        if rng.random() < 0.5:
            steps.append((rng.uniform(1, 5), latency(s, rng.uniform(1.05, 2.5))))
    return steps, None


def db_disk_filling():
    steps = [(0, disk("payments-db", "pgdata", rng.randint(84, 96), rng.randint(2, 14)))]
    if rng.random() < 0.6:
        steps.append((rng.uniform(4, 12), alert("ReplicationLag", "payments-db",
                                                f"replica lag is {rng.randint(40, 600)}s (threshold 30s)")))
    return steps, None


def dependency_outage():
    dep, app = rng.choice([("redis-cart", "cart"), ("search-index", "search")])
    steps = [(0, target_down(dep, rng.choice([2, 3]), 3)), (rng.uniform(0.5, 3), errors(app, rng.uniform(8, 70)))]
    for up in CALLERS[app]:
        if rng.random() < 0.6:
            steps.append((rng.uniform(2, 6), latency(up, rng.uniform(1.1, 3))))
    return steps, None


def cert_about_to_expire():
    return [(0, cert(rng.choice(["shop.example.com", "api.example.com"]), rng.randint(1, 4)))], None


def deploy_blip():
    """A healthy rollout: pods warm up, one target is briefly down, a marginal error ratio."""
    s = rng.choice(APPS)
    options = [cpu(s, rng.randint(86, 97)), target_down(s, 1, 3), latency(s, rng.uniform(1.02, 1.5)),
               errors(s, rng.uniform(5.1, 7.5))]
    picked = rng.sample(options, rng.choice([1, 1, 2]))
    return [(rng.uniform(0, 3), a) for a in picked], deploy(s)


def latency_flap():
    return [(0, latency(rng.choice(APPS + ["search-index"]), rng.uniform(1.01, 1.4)))], None


def batch_job():
    """The nightly report job runs hot and fills its scratch volume, then cleans up."""
    picked = rng.sample([cpu("batch-reports", rng.randint(88, 99)),
                         disk("batch-reports", "scratch", rng.randint(55, 88), rng.randint(6, 22))],
                        rng.choice([1, 2]))
    return [(rng.uniform(0, 5), a) for a in picked], None


def staging_breakage():
    s = rng.choice(APPS)
    steps = [(0, errors(s, rng.uniform(6, 80), env="staging"))]
    if rng.random() < 0.4:
        steps.append((rng.uniform(1, 4), latency(s, rng.uniform(1.1, 4), env="staging")))
    return steps, None


def cert_weeks_away():
    return [(0, cert(rng.choice(["shop.example.com", "docs.example.com"]), rng.randint(19, 29)))], None


def cpu_spike():
    return [(0, cpu(rng.choice(APPS), rng.randint(86, 93)))], None


INCIDENTS = [bad_deploy, bad_deploy, db_saturation, memory_leak, node_down, db_disk_filling,
             dependency_outage, cert_about_to_expire]
NOISE = [deploy_blip, deploy_blip, deploy_blip, latency_flap, latency_flap, batch_job,
         staging_breakage, cert_weeks_away, cpu_spike]


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    alerts, deploys, labels = [], [], []
    incident_starts = []

    def emit(event_id, scenario, start, actionable):
        steps, dep = scenario()
        if dep:
            deploys.append({**dep, "at": iso(start - timedelta(minutes=rng.uniform(2, 6)))})
        for offset, a in steps:
            starts_at = iso(start + timedelta(minutes=offset))
            fp = hashlib.sha1(json.dumps([a["labels"], starts_at], sort_keys=True).encode()).hexdigest()[:16]
            alerts.append({"status": "firing", **a, "startsAt": starts_at, "endsAt": "0001-01-01T00:00:00Z",
                           "generatorURL": "http://prometheus:9090/graph", "fingerprint": fp})
            labels.append({"fingerprint": fp, "actionable": int(actionable), "event_id": event_id,
                           "scenario": scenario.__name__})

    for i in range(N_INCIDENTS):
        start = START + timedelta(minutes=rng.uniform(30, HOURS * 60 - 30))
        incident_starts.append(start)
        emit(f"inc-{i:03d}", rng.choice(INCIDENTS), start, True)

    for i in range(N_NOISE):
        scenario = rng.choice(NOISE)
        if scenario is batch_job:  # nightly, 01:00-03:00 UTC
            start = START + timedelta(days=rng.randrange(HOURS // 24), hours=1, minutes=rng.uniform(0, 120))
        elif rng.random() < 0.25:  # noise that lands in the middle of an incident
            start = rng.choice(incident_starts) + timedelta(minutes=rng.uniform(-6, 10))
        else:
            start = START + timedelta(minutes=rng.uniform(30, HOURS * 60 - 30))
        emit(f"noise-{i:03d}", scenario, start, False)

    for _ in range(N_QUIET_DEPLOYS):  # most deploys cause nothing
        at = START + timedelta(minutes=rng.uniform(0, HOURS * 60))
        deploys.append({**deploy(rng.choice(APPS + ["batch-reports"])), "at": iso(at)})

    alerts.sort(key=lambda a: a["startsAt"])
    deploys.sort(key=lambda d: d["at"])
    order = {a["fingerprint"]: k for k, a in enumerate(alerts)}
    labels.sort(key=lambda r: order[r["fingerprint"]])

    DATA.mkdir(exist_ok=True)
    payload = {"version": "4", "receiver": "alert-triage", "status": "firing", "alerts": alerts}
    (DATA / "alerts.json").write_text(json.dumps(payload, indent=1))
    (DATA / "deploys.json").write_text(json.dumps(deploys, indent=1))
    with open(DATA / "labels.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["fingerprint", "actionable", "event_id", "scenario"])
        w.writeheader()
        w.writerows(labels)

    n_act = sum(r["actionable"] for r in labels)
    print(f"{len(alerts)} alerts ({n_act} actionable, {len(alerts) - n_act} noise), {len(deploys)} deploys -> {DATA}")


if __name__ == "__main__":
    main()
