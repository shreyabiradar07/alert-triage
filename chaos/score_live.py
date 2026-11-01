#!/usr/bin/env python3
"""Check the live ranking against what chaos.py injected.

Reads the alerts the receiver holds (GET /alerts.json) and data/chaos-log.jsonl. An alert is
labelled by the fault that was running when it started firing, on a service that fault can
reach: real fault or noise. Alerts from the staging copy and the batch job are noise whenever
they fire. Anything else that started outside every fault window is left out.

    python3 chaos/score_live.py
    python3 chaos/score_live.py --since 2026-10-11T04:00:00Z     # only faults from this run

Standard library only, Python 3.8 or later.
"""

import argparse
import json
import urllib.request
from pathlib import Path

LOG = Path(__file__).resolve().parent.parent / "data" / "chaos-log.jsonl"
SLACK = 45  # seconds after a fault ends in which a late alert still belongs to it


def seconds(stamp):
    from datetime import datetime, timezone
    return datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def label(alert, faults):
    labels, t = alert["labels"], seconds(alert["startsAt"])
    if labels.get("env") == "staging" or labels.get("service") == "batch-reports":
        return {"scenario": "background", "service": labels.get("service", ""), "actionable": False}
    return next((f for f in faults if seconds(f["start"]) <= t <= seconds(f["end"]) + SLACK
                 and labels.get("service") in f.get("affects", [labels.get("service")])), None)


def auc(scored):
    """Chance that a real alert outscores a noise alert (ties count half)."""
    pos = [p for p, y in scored if y]
    neg = [p for p, y in scored if not y]
    if not pos or not neg:
        return float("nan")
    return sum((a > b) + 0.5 * (a == b) for a in pos for b in neg) / (len(pos) * len(neg))


def fatigue(pairs, cut=0.5):
    """What the ranking saves a person, and what it costs. pairs: (score, is real)."""
    total, real = len(pairs), sum(y for _, y in pairs)
    noise = total - real
    last = [y for p, y in pairs if p < cut]            # the "probably noise" band
    delayed = sum(last)
    last_real = min((p for p, y in pairs if y), default=None)
    before = 0 if last_real is None else sum(1 for p, y in pairs if not y and p >= last_real)

    def share(part, whole):
        return "%d of %d (%.0f%%)" % (part, whole, 100.0 * part / whole) if whole else "0 of 0"

    print("\nalert fatigue (an alert below %.1f can be read last)" % cut)
    print("  alerts that can be read last:           %s" % share(len(last), total))
    print("  noise moved out of the way:             %s" % share(len(last) - delayed, noise))
    print("  real alerts delayed by mistake:         %s" % share(delayed, real))
    print("  noise still scored %.1f or higher:       %s" % (cut, share(noise - (len(last) - delayed), noise)))
    print("  noise read before the last real alert:  %s, reading top-down" % share(before, noise))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:9099")
    parser.add_argument("--since", default="", help="ignore faults that started before this UTC time")
    args = parser.parse_args()

    faults = [json.loads(line) for line in LOG.read_text().splitlines() if line.strip()]
    faults = [f for f in faults if f["start"] >= args.since]
    with urllib.request.urlopen(args.url + "/alerts.json", timeout=10) as resp:
        rows = json.loads(resp.read())

    labelled, outside = [], 0
    for r in rows:
        fault = label(r["alert"], faults)
        if fault is None:
            outside += 1
        else:
            labelled.append((r, fault))
    scored = [(r, f) for r, f in labelled if r["actionable"] is not None]
    print("%d alerts held by the receiver: %d inside a fault window (%d scored), %d outside (left out)"
          % (len(rows), len(labelled), len(scored), outside))
    print("%d faults in the log: %d real, %d noise\n"
          % (len(faults), sum(f["actionable"] for f in faults), sum(not f["actionable"] for f in faults)))
    if not scored:
        return

    print("%-16s %-10s %-5s %-6s %-15s %-15s %s" % ("scenario", "target", "truth", "score", "band", "alert", "on"))
    for r, f in sorted(scored, key=lambda x: -x[0]["actionable"]):
        labels = r["alert"]["labels"]
        print("%-16s %-10s %-5s %-6.2f %-15s %-15s %s" % (f["scenario"], f["service"], "real" if f["actionable"] else "noise",
              r["actionable"], r["band"], labels["alertname"], labels.get("service", "")))

    pairs = [(r["actionable"], f["actionable"]) for r, f in scored]
    severity = [(1.0 if r["alert"]["labels"].get("severity") == "page" else 0.5, f["actionable"]) for r, f in scored]
    real = sum(y for _, y in pairs)
    print("\n%d scored alerts: %d from real faults, %d from noise" % (len(pairs), real, len(pairs) - real))
    print("ranking (AUC): Jev %.3f, severity label %.3f" % (auc(pairs), auc(severity)))
    print("\n%-16s %7s %12s" % ("band", "alerts", "were real"))
    for band in ("look first", "likely real", "unsure", "probably noise"):
        inside = [f["actionable"] for r, f in scored if r["band"] == band]
        print("%-16s %7d %12s" % (band, len(inside), "-" if not inside else "%d of %d" % (sum(inside), len(inside))))

    fatigue(pairs)

    silent = [f for f in faults if not any(g is f for _, g in labelled)]
    if silent:
        print("\nfaults that produced no alert: " + ", ".join("%s on %s" % (f["scenario"], f["service"]) for f in silent))


if __name__ == "__main__":
    main()
