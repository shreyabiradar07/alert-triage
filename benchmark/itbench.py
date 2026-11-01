"""Run alert-triage on ITBench-Lite's SRE scenarios: real Prometheus alerts with a ground truth written
by someone else (IBM Research, Apache-2.0, https://huggingface.co/datasets/ibm-research/ITBench-Lite).

Each scenario is one injected fault on a Kubernetes app, with a snapshot of the firing alerts every
minute and a ground_truth.yaml naming the fault, the alerts it caused and the entities it touched.

    python -m benchmark.itbench fetch                      # alerts/ + ground_truth.yaml of every SRE scenario (~13 MB)
    python -m benchmark.itbench run --group-by namespace   # replay through the receiver's scoring and live incidents
    python -m benchmark.itbench run --group-by ...         # '...' = Alertmanager's "no grouping": every label set alone
    python -m benchmark.itbench score
    python -m benchmark.itbench demo Scenario-35           # play one scenario into a running receiver, to watch the page

Replay is in arrival order: each minute's new alerts are scored and placed with only what fired before
them, as the live receiver would. An alert that drops out of the snapshots is marked resolved.

The results files written before Jev merging was removed (October 2026) also hold the merged incidents;
experiments/experiment_questions.json notes what they showed.

Labels for scoring, from ground_truth.yaml (the triage never sees them):
    strict   the alert is one the ground truth lists (alert name + an entity of its group)
    broad    the alert fires on any entity the ground truth says the fault touched
    noise    neither
"""

import argparse
import asyncio
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from alert_triage import triage
from alert_triage.live_incidents import LiveIncidents
from alert_triage.receiver import Store, jev_scorer

REPO = "https://huggingface.co/api/datasets/ibm-research/ITBench-Lite/tree/main/snapshots/sre"
FILES = "https://huggingface.co/datasets/ibm-research/ITBench-Lite/resolve/main/"
RAW = triage.DATA / "itbench" / "raw"
RESULTS = triage.DATA / "itbench"
ENTITY_LABELS = ("service_name", "service", "pod", "container", "deployment", "statefulset", "daemonset", "job_name")


# --- fetch ---

def listing(url: str) -> list[dict]:
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r)


def download(path: str, to: Path):
    if to.exists():
        return
    to.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(FILES + path, timeout=60) as r:
        to.write_bytes(r.read())


def fetch():
    (version,) = [d["path"] for d in listing(REPO) if d["type"] == "directory"]
    scenarios = [d["path"] for d in listing(f"{REPO}/{version.split('/')[-1]}") if d["type"] == "directory"]
    jobs, skipped = [], []
    for s in scenarios:
        name = s.split("/")[-1]
        try:
            files = listing(f"{REPO}/{'/'.join(s.split('/')[2:])}/alerts")
        except urllib.error.HTTPError as exc:
            skipped.append(f"{name} ({exc.code})")
            continue
        jobs.append((f"{s}/ground_truth.yaml", RAW / name / "ground_truth.yaml"))
        for f in files:
            if f["path"].split("/")[-1].startswith("alerts_at_"):
                jobs.append((f["path"], RAW / name / "alerts" / f["path"].split("/")[-1]))
    if skipped:
        print("no alerts folder listed for: " + ", ".join(skipped))
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda j: download(*j), jobs))
    print(f"{len(scenarios)} scenarios, {len(jobs)} files in {RAW}")


# --- convert ---

def snapshot_time(path: Path) -> str:
    """alerts_at_2025-12-15T17-30-09.384366.json -> 2025-12-15T17:30:09Z"""
    stamp = path.stem.removeprefix("alerts_at_")
    day, clock = stamp.split("T")
    return f"{day}T{clock[:8].replace('-', ':')}Z"


def fingerprint(labels: dict) -> str:
    return hashlib.sha1(json.dumps(labels, sort_keys=True).encode()).hexdigest()[:16]


def scenario_alerts(folder: Path):
    """Yield (snapshot time, alerts first seen firing in it), one snapshot at a time. Before each yield,
    an alert missing from that snapshot is marked resolved in place, so a replay never sees a resolve
    before it happened."""
    seen = {}
    for path in sorted((folder / "alerts").glob("alerts_at_*.json"), key=snapshot_time):
        t = snapshot_time(path)
        firing = {fingerprint(a["labels"]) + "@" + a["activeAt"]: a
                  for a in json.loads(path.read_text()) if a.get("state") == "firing"}
        for key, a in seen.items():
            if a["status"] == "firing" and key not in firing:
                a["status"], a["endsAt"] = "resolved", t
        fresh = []
        for key, a in firing.items():
            if key not in seen:
                seen[key] = {"status": "firing", "labels": a["labels"], "annotations": a.get("annotations", {}),
                             "startsAt": a["activeAt"], "endsAt": "0001-01-01T00:00:00Z",
                             "fingerprint": fingerprint(a["labels"]), "_arrived": t}
                fresh.append(seen[key])
        yield t, fresh


def ground_truth(folder: Path) -> dict:
    return yaml.safe_load((folder / "ground_truth.yaml").read_text()) or {}


def matches(alert: dict, group: dict) -> bool:
    labels = alert["labels"]
    if group.get("namespace") and labels.get("namespace") not in (None, group["namespace"]):
        return False
    values = [labels[k] for k in ENTITY_LABELS if labels.get(k)]
    return any(re.match(pattern(f), v) for f in group.get("filter", []) for v in values)


def pattern(f: str) -> str:
    """The ground truth's filters are regexes, except Scenario-38's '*.*' (every autoscaler), which is
    read as the regex '.*.*'."""
    try:
        re.compile(f)
        return f
    except re.error:
        return "." + f if f.startswith("*") else re.escape(f)


def label(alert: dict, gt: dict) -> str:
    groups = {g["id"]: g for g in gt.get("groups", [])}
    for listed in gt.get("alerts", []):
        g = groups.get(listed.get("group_id"))
        if listed.get("id") == alert["labels"].get("alertname") and g and matches(alert, g):
            return "strict"
    return "broad" if any(matches(alert, g) for g in groups.values()) else "noise"


# --- run ---

def group_of(alert: dict, group_by: list[str]) -> tuple[str, dict]:
    labels = alert["labels"] if group_by == ["..."] else {k: alert["labels"][k] for k in group_by if k in alert["labels"]}
    return json.dumps(sorted(labels.items())), labels


def replay(folder: Path, group_by: list[str]) -> list[dict]:
    store, live = Store(), LiveIncidents()
    rows = []
    for t, fresh in scenario_alerts(folder):
        if not fresh:
            continue
        added = store.add(fresh)
        results = asyncio.run(jev_scorer([(e["alert"], *store.context(e["alert"])) for e in added]))
        for e, r in zip(added, results):
            e["scores"], e["note"] = (None, f"unscored: {r}") if isinstance(r, Exception) else (r, "")
        for e in added:
            live.receive([e["alert"]], group_of(e["alert"], group_by))
    incident_of = {id(a): (i["incident"], i["group"]) for i in live.view(with_alerts=True) for a in i["members"]}
    gt = ground_truth(folder)
    for e in store.entries.values():
        a, s = e["alert"], e["scores"] or {}
        n, key = incident_of[id(a)]
        rows.append({"scenario": folder.name, "alertname": a["labels"].get("alertname"), "labels": a["labels"],
                     "arrived": a["_arrived"], "label": label(a, gt), "actionable": s.get("actionable"),
                     "user_impact": (s.get("user_impact") or {}).get("score"), "incident": n, "group": key,
                     "jev": s.get("jev")})
    return rows


def run(group_by: list[str], only: list[str]):
    folders = sorted(p for p in RAW.iterdir() if (p / "ground_truth.yaml").exists() and (not only or p.name in only))
    out = RESULTS / f"results-{'all-labels' if group_by == ['...'] else '-'.join(group_by)}.json"
    rows = []
    for f in folders:
        rows += replay(f, group_by)
        print(f"{f.name}: {sum(r['scenario'] == f.name for r in rows)} alerts", flush=True)
    out.write_text(json.dumps(rows, indent=1))
    print(f"wrote {out}")


# --- demo ---

def post(url: str, body: dict) -> dict:
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def webhooks(folder: Path, group_by: list[str], offset: timedelta):
    """Yield (snapshot time, Alertmanager webhook payloads), one snapshot at a time: new alerts and resolves,
    one payload per group. Times move by offset, so spacing, and so Jev's context windows, stay as recorded."""
    def moved(stamp: str) -> str:
        return (triage.when(stamp) + offset).strftime("%Y-%m-%dT%H:%M:%SZ")

    def posted(a: dict) -> dict:
        ended = a["endsAt"] if a["status"] == "firing" else moved(a["endsAt"])
        return {k: v for k, v in a.items() if k != "_arrived"} | {"startsAt": moved(a["startsAt"]), "endsAt": ended}

    firing = []
    for t, fresh in scenario_alerts(folder):
        changed = [a for a in firing if a["status"] == "resolved"] + fresh
        firing = [a for a in firing if a["status"] == "firing"] + fresh
        groups = {}
        for a in changed:
            key, labels = group_of(a, group_by)
            groups.setdefault(key, (labels, []))[1].append(posted(a))
        yield t, [{"version": "4", "groupKey": key, "groupLabels": labels, "alerts": alerts}
                  for key, (labels, alerts) in groups.items()]


def demo(name: str, group_by: list[str], speed: float, url: str):
    """Play one scenario into a running receiver, so it fills the page as it would have live."""
    folder = RAW / name
    if not (folder / "ground_truth.yaml").exists():
        sys.exit(f"{name} is not in {RAW}. Run `python -m benchmark.itbench fetch`, or check the name.")
    times = sorted(snapshot_time(p) for p in (folder / "alerts").glob("alerts_at_*.json"))
    offset = datetime.now(timezone.utc) - triage.when(times[-1])
    started, sent = time.monotonic(), {}
    try:
        for t, payloads in webhooks(folder, group_by, offset):
            elapsed = (triage.when(t) - triage.when(times[0])).total_seconds() / speed
            time.sleep(max(0, started + elapsed - time.monotonic()))
            for payload in payloads:
                post(url + "/alerts", payload)
                sent.update({(a["fingerprint"], a["startsAt"]): a for a in payload["alerts"]})
            if payloads:
                print(f"{t}  {sum(len(p['alerts']) for p in payloads)} alert updates", flush=True)
        with urllib.request.urlopen(url + "/alerts.json", timeout=30) as r:
            ranked = [e for e in json.load(r) if (e["alert"].get("fingerprint"), e["alert"]["startsAt"]) in sent]
    except urllib.error.URLError as exc:
        sys.exit(f"cannot reach the receiver at {url}: {exc}. Start it with `python -m alert_triage.receiver`.")
    gt = ground_truth(folder)

    def priority(e):
        impact = (e.get("user_impact") or {}).get("score")
        return (e.get("actionable") or 0) * (impact / 3 if impact is not None else 1)

    print(f"\n{name}: {len(sent)} alerts. Highest urgency x user impact, against IBM's answer key:")
    for rank, e in enumerate(sorted(ranked, key=priority, reverse=True)[:5], 1):
        labels = e["alert"]["labels"]
        print(f"  {rank}. {label(e['alert'], gt):<6}  {priority(e):.2f}  {labels.get('alertname')}")


# --- score ---

SEVERITY = {"critical": 3, "page": 3, "warning": 2, "info": 1, "none": 0}


def auc(pos: list[float], neg: list[float]) -> float | None:
    """P(a random fault alert ranks above a random noise alert); ties count half."""
    if not pos or not neg:
        return None
    wins = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


def fault_grouping(rows: list[dict], key: str) -> tuple[int, int, int, int]:
    """(fault pairs together, fault pairs, fault alerts in the main fault cluster, size of that cluster),
    where the clusters are rows[key] and the main one holds the most fault alerts."""
    fault = [r for r in rows if r["label"] != "noise"]
    together = sum(a[key] == b[key] for i, a in enumerate(fault) for b in fault[i + 1:])
    pairs = len(fault) * (len(fault) - 1) // 2
    if not fault:
        return 0, 0, 0, 0
    clusters = {}
    for r in fault:
        clusters[r[key]] = clusters.get(r[key], 0) + 1
    main = max(clusters, key=clusters.get)
    return together, pairs, clusters[main], sum(r[key] == main for r in rows)


# Meta-alerts that fire on purpose to show alerting works (or to feed inhibition): never the fault, even
# when they carry a pod or namespace the fault touched. InfoInhibitor made up 143 of 251 "broad" labels.
META = {"InfoInhibitor", "Watchdog"}


def cost(rows: list[dict]):
    """Time and tokens per Jev call, for results written since they were recorded."""
    calls = [r["jev"] for r in rows if r.get("jev")]
    if not calls:
        return
    seconds = sorted(c["seconds"] for c in calls)
    pct = lambda q: seconds[min(len(seconds) - 1, int(q * len(seconds)))]
    tokens = [(c["input_tokens"], c.get("output_tokens") or 0) for c in calls if c.get("input_tokens") is not None]
    line = f"  Jev calls {len(calls)}: median {pct(0.5):.2f} s, p95 {pct(0.95):.2f} s, total {sum(seconds):.0f} s"
    if tokens:
        line += (f"; tokens per call {sum(i for i, _ in tokens) / len(tokens):.0f} in / {sum(o for _, o in tokens) / len(tokens):.0f}"
                 f" out, {sum(i + o for i, o in tokens):,} in all; ${sum(c.get('cost_usd') or 0 for c in calls):.4f} at list price")
    print(line)


def score():
    severity = lambda r: SEVERITY.get(r["labels"].get("severity", "none"), 0)
    orders = (("urgency alone", lambda r: r["actionable"]),
              ("user impact", lambda r: r["user_impact"] or 0),
              ("urgency x user impact (the page)", lambda r: r["actionable"] * (r["user_impact"] or 0) / 3),
              ("severity label", severity))
    for path in sorted(RESULTS.glob("results-*.json")):
        rows = [r for r in json.loads(path.read_text()) if r["actionable"] is not None]
        for r in rows:
            if r["alertname"] in META:
                r["label"] = "noise"
        print(f"\n== {path.name}: {len(rows)} scored alerts, "
              f"{len({r['scenario'] for r in rows})} scenarios ({', '.join(sorted(META))} counted as noise)")
        for name, is_fault in (("strict", lambda r: r["label"] == "strict"),
                               ("strict+broad", lambda r: r["label"] != "noise")):
            pos = [r for r in rows if is_fault(r)]
            neg = [r for r in rows if r["label"] == "noise"]
            print(f"  AUC, {name} vs noise: urgency {auc([r['actionable'] for r in pos], [r['actionable'] for r in neg]):.2f}"
                  f"  ·  user impact {auc([r['user_impact'] or 0 for r in pos], [r['user_impact'] or 0 for r in neg]):.2f}"
                  f"  ·  severity label {auc([severity(r) for r in pos], [severity(r) for r in neg]):.2f}")
        scenarios = [s for s in sorted({r["scenario"] for r in rows})
                     if any(r["label"] != "noise" for r in rows if r["scenario"] == s)]
        print(f"  a fault alert ranked 1st / in the top 3, of {len(scenarios)} scenarios with one:")
        for name, key in orders:
            top1 = top3 = 0
            for s in scenarios:
                ranked = sorted((r for r in rows if r["scenario"] == s), key=lambda r: -key(r))
                top1 += ranked[0]["label"] != "noise"
                top3 += any(r["label"] != "noise" for r in ranked[:3])
            print(f"    sorted by {name:<26} {top1:>2} / {top3:>2}")
        for lab in ("strict", "broad", "noise"):
            vals = [r["user_impact"] for r in rows if r["label"] == lab and r["user_impact"] is not None]
            share = sum(r["actionable"] >= 0.5 for r in rows if r["label"] == lab) / max(1, sum(r["label"] == lab for r in rows))
            print(f"  {lab:<6} n={sum(r['label'] == lab for r in rows):>4}  urgent (>=50%) {share:.0%}"
                  f"  mean user impact {sum(vals) / max(1, len(vals)):.2f} of 3")
        cost(rows)
        for key, name in (("group", "Alertmanager groups"),):
            t = p = inside = size = 0
            for s in scenarios:
                a, b, c, d = fault_grouping([r for r in rows if r["scenario"] == s], key)
                t, p, inside, size = t + a, p + b, inside + c, size + d
            print(f"  {name:<26} fault pairs together {t / max(1, p):.2f}  ·  main fault cluster is "
                  f"{inside / max(1, size):.0%} fault alerts")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("fetch")
    r = sub.add_parser("run")
    r.add_argument("--group-by", required=True, help="the group_by to replay with, comma-separated, or '...'")
    r.add_argument("--only", nargs="*", default=[], help="scenario folder names, e.g. Scenario-1")
    sub.add_parser("score")
    d = sub.add_parser("demo")
    d.add_argument("scenario", help="a scenario folder name, e.g. Scenario-35")
    d.add_argument("--group-by", default="namespace", help="the group_by to replay with, comma-separated, or '...'")
    d.add_argument("--speed", type=float, default=30, help="how many times faster than recorded (default 30)")
    d.add_argument("--url", default="http://127.0.0.1:9099", help="the running receiver")
    args = parser.parse_args()
    if args.command == "fetch":
        fetch()
    elif args.command == "score":
        score()
    elif args.command == "demo":
        demo(args.scenario, [g.strip() for g in args.group_by.split(",")], max(args.speed, 0.1), args.url)
    else:
        if not triage.api_key():
            sys.exit("TYPESAFE_API_KEY is not set.")
        run([g.strip() for g in args.group_by.split(",")], args.only)


if __name__ == "__main__":
    main()
