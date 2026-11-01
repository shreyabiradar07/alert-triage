"""Compare the three conditions against the labels and write report.html.

Reads data/alerts.json, data/deploys.json, data/labels.csv and data/scores.json (from
`python -m alert_triage.triage score`). The report has the calibration plots, the per-condition numbers, and
the busiest 45 minutes of the simulation as a ranked list.

    python -m experiments.evaluate
"""

import csv
import html
import json
import sys
from datetime import timedelta
from pathlib import Path

import numpy as np

from experiments import baselines
from alert_triage.triage import CONDITIONS, DATA, context_for, deploy_note, load_alerts, when

ROOT = Path(__file__).parent.parent
BINS = 10
DEMO_WINDOW = timedelta(minutes=45)
COLORS = {"alert": "#8a8f98", "alert+recent": "#2f6fde", "alert+recent+deploys": "#d9730d"}


def auc(p, y):
    """Probability that a positive outscores a negative (ties count half)."""
    pos, neg = p[y == 1], p[y == 0]
    if not len(pos) or not len(neg):
        return float("nan")
    wins = (pos[:, None] > neg[None, :]).sum() + 0.5 * (pos[:, None] == neg[None, :]).sum()
    return wins / (len(pos) * len(neg))


def precision_at(p, y, k):
    """Fraction true among the k highest scores. Ties at the cut-off count in proportion."""
    cut = np.sort(p)[::-1][k - 1]
    above, tied = p > cut, p == cut
    return (y[above].sum() + (k - above.sum()) * y[tied].mean()) / k


def versus_baselines(alerts, deploys, scores, y_act, y_same):
    """Ranking only: the baselines give an order, not a probability, so calibration does not apply."""
    k_top, k_all = 36, int(y_act.sum())
    act = {"severity label": np.array([baselines.severity(a) for a in alerts]),
           "hand-written rules": np.array([baselines.rules(a) for a in alerts])}
    act |= {f"Jev, {c}": np.array([s["actionable"] for s in scores[c]]) for c in CONDITIONS}
    act_rows = [(name, auc(p, y_act), precision_at(p, y_act, k_top), precision_at(p, y_act, k_all))
                for name, p in act.items()]

    asked = np.array([s["same_event"] is not None for s in scores[CONDITIONS[-1]]])
    same = {"any alert in the window": np.array([baselines.any_recent(k, alerts, deploys) for k in range(len(alerts))]),
            "group_by: [service]": np.array([baselines.same_service(k, alerts, deploys) for k in range(len(alerts))])}
    same |= {f"Jev, {c}": np.array([s["same_event"] or 0.0 for s in scores[c]]) for c in CONDITIONS[1:]}
    same_rows = []
    for name, p in same.items():
        p, y = p[asked], y_same[asked]
        said = p >= 0.5
        same_rows.append((name, auc(p, y), y[said].mean() if said.any() else float("nan"), said[y == 1].mean()))
    return (("actionable", ("AUC", f"true in top {k_top}", f"true in top {k_all}"), act_rows),
            ("same event", ("AUC", "precision", "recall"), same_rows))


def reliability(p, y):
    """Per bin: (mean score, fraction true, count). Equal-width bins on [0, 1]."""
    idx = np.minimum((p * BINS).astype(int), BINS - 1)
    return [(p[idx == b].mean(), y[idx == b].mean(), int((idx == b).sum())) for b in range(BINS) if (idx == b).any()]


def metrics(p, y):
    bins = reliability(p, y)
    top = p >= 0.9
    return {"n": len(p), "base": y.mean(), "auc": auc(p, y), "brier": ((p - y) ** 2).mean(),
            "ece": sum(n * abs(conf - acc) for conf, acc, n in bins) / len(p),
            "top_n": int(top.sum()), "top_true": y[top].mean() if top.any() else float("nan"), "bins": bins}


def plot(curves: dict) -> str:
    """Reliability diagram: mean score against observed frequency, dot area proportional to bin count."""
    size, pad = 340, 44
    span = size - 2 * pad

    def xy(x, yv):
        return pad + x * span, size - pad - yv * span

    out = [f'<svg viewBox="0 0 {size} {size}" role="img">']
    for t in (0, 0.25, 0.5, 0.75, 1):
        x, y = xy(t, t)
        out.append(f'<line x1="{x}" y1="{size - pad}" x2="{x}" y2="{pad}" class="grid"/>'
                   f'<line x1="{pad}" y1="{y}" x2="{size - pad}" y2="{y}" class="grid"/>'
                   f'<text x="{x}" y="{size - pad + 16}" text-anchor="middle">{t:g}</text>'
                   f'<text x="{pad - 8}" y="{y + 4}" text-anchor="end">{t:g}</text>')
    (x0, y0), (x1, y1) = xy(0, 0), xy(1, 1)
    out.append(f'<line x1="{x0}" y1="{y0}" x2="{x1}" y2="{y1}" class="ideal"/>')
    for name, bins in curves.items():
        pts = [xy(conf, acc) for conf, acc, _ in bins]
        out.append(f'<polyline points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in pts)}" '
                   f'fill="none" stroke="{COLORS[name]}" stroke-width="1.5"/>')
        for (x, y), (conf, acc, n) in zip(pts, bins):
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{1.5 + n ** 0.5 * 0.9:.1f}" fill="{COLORS[name]}" '
                       f'fill-opacity="0.75"><title>{name}: mean score {conf:.2f}, {acc:.0%} true, n={n}</title></circle>')
    out.append(f'<text x="{size / 2}" y="{size - 6}" text-anchor="middle">score</text>'
               f'<text transform="translate(12 {size / 2}) rotate(-90)" text-anchor="middle">fraction true</text></svg>')
    return "".join(out)


def metrics_table(results: dict) -> str:
    rows = "".join(
        f'<tr><td><span class="dot" style="background:{COLORS[c]}"></span>{c}</td><td>{m["n"]}</td>'
        f'<td>{m["auc"]:.3f}</td><td>{m["brier"]:.3f}</td><td>{m["ece"]:.3f}</td>'
        f'<td>{m["top_n"]}</td><td>{"-" if m["top_n"] == 0 else format(m["top_true"], ".0%")}</td></tr>'
        for c, m in results.items())
    return ('<table><tr><th>condition</th><th>alerts</th><th>AUC</th><th>Brier</th><th>ECE</th>'
            '<th>scored &ge; 0.9</th><th>of those, true</th></tr>' + rows + '</table>')


def ranked_list(alerts, deploys, labels, scores) -> str:
    times = [when(a["startsAt"]) for a in alerts]
    start = max(range(len(alerts)), key=lambda i: sum(times[i] <= t < times[i] + DEMO_WINDOW for t in times))
    window = [k for k, t in enumerate(times) if times[start] <= t < times[start] + DEMO_WINDOW]
    full, alone = scores[CONDITIONS[-1]], scores["alert"]
    window.sort(key=lambda k: -full[k]["actionable"])

    rows = []
    for rank, k in enumerate(window, 1):
        a, s, lab = alerts[k], full[k], labels[alerts[k]["fingerprint"]]
        notes = []
        if s["same_event"] is not None:
            notes.append(f'same event as a recent alert: {s["same_event"]:.2f}')
        if note := deploy_note(a, context_for(k, alerts, deploys)[1]):
            notes.append(note)
        truth = "actionable" if lab["actionable"] == "1" else "noise"
        rows.append(
            f'<tr><td>{rank}</td>'
            f'<td><div class="bar"><i style="width:{s["actionable"] * 100:.0f}%"></i></div>{s["actionable"]:.2f}</td>'
            f'<td class="dim">{alone[k]["actionable"]:.2f}</td>'
            f'<td>{times[k].strftime("%H:%M")}</td>'
            f'<td><b>{html.escape(a["labels"]["alertname"])}</b><br><span class="dim">'
            f'{html.escape(a["labels"].get("service", ""))} &middot; {html.escape(a["labels"].get("env", ""))}</span></td>'
            f'<td>{html.escape(a["annotations"]["description"])}<br><span class="dim">{html.escape("; ".join(notes))}</span></td>'
            f'<td><span class="tag {truth}">{truth}</span><br><span class="dim">{lab["event_id"]}</span></td></tr>')
    head = (f'<p>{times[start].strftime("%a %H:%M")} to {(times[start] + DEMO_WINDOW).strftime("%H:%M")} UTC, the busiest '
            f'{DEMO_WINDOW.seconds // 60} minutes of the simulation. {len(window)} alerts in, {len(window)} out: '
            f'ranked by P(actionable) with recent alerts and deploys as context, none suppressed. '
            f'The label column is the answer key, which the model never saw.</p>')
    return head + ('<table class="ranked"><tr><th>#</th><th>P(actionable)</th><th>alert alone</th><th>fired</th>'
                   '<th>alert</th><th>detail</th><th>label</th></tr>' + "".join(rows) + '</table>')


CSS = """
:root{--bg:#fff;--fg:#1c1e21;--dim:#6b7280;--line:#e3e5e8;--good:#1a7f4b;--noise:#8a8f98}
@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e6e8eb;--dim:#9aa1ab;--line:#2c3036;--good:#4cc38a}}
body{background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,sans-serif;max-width:1040px;margin:0 auto;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:16px;margin:32px 0 8px}p{margin:6px 0}.dim{color:var(--dim);font-size:12px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--dim);font-weight:600}.wrap{overflow-x:auto}
.plots{display:flex;flex-wrap:wrap;gap:16px}.plots figure{margin:0;flex:1 1 320px;max-width:420px}
figcaption{font-weight:600;margin-bottom:4px}svg text{font-size:11px;fill:var(--dim)}
.grid{stroke:var(--line)}.ideal{stroke:var(--dim);stroke-dasharray:4 4}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}
.bar{display:inline-block;width:70px;height:8px;background:var(--line);border-radius:4px;margin-right:6px;overflow:hidden}
.bar i{display:block;height:100%;background:#d9730d}
.tag{font-size:12px;font-weight:600}.tag.actionable{color:var(--good)}.tag.noise{color:var(--noise)}
"""


def main():
    if not (DATA / "scores.json").exists():
        sys.exit("data/scores.json is missing. Run `python -m alert_triage.triage score` first.")
    alerts = load_alerts(DATA / "alerts.json")
    deploys = json.loads((DATA / "deploys.json").read_text())
    with open(DATA / "labels.csv", newline="") as f:
        labels = {r["fingerprint"]: r for r in csv.DictReader(f)}
    scores = json.loads((DATA / "scores.json").read_text())
    if scores["fingerprints"] != [a["fingerprint"] for a in alerts]:
        sys.exit("data/scores.json was made from different alerts. Run `python -m alert_triage.triage score` again.")

    y_act = np.array([int(labels[a["fingerprint"]]["actionable"]) for a in alerts])
    events = [labels[a["fingerprint"]]["event_id"] for a in alerts]
    fps = [a["fingerprint"] for a in alerts]
    # Same event: some alert in the recent window came from the same injected fault or deploy.
    y_same = np.array([int(any(labels[r["fingerprint"]]["event_id"] == events[k]
                               for r in context_for(k, alerts, deploys)[0])) for k in range(len(alerts))])
    assert len(set(fps)) == len(fps), "duplicate fingerprints"

    act, same = {}, {}
    for c in CONDITIONS:
        act[c] = metrics(np.array([s["actionable"] for s in scores[c]]), y_act)
        asked = np.array([s["same_event"] is not None for s in scores[c]])
        if asked.any():
            same[c] = metrics(np.array([s["same_event"] for s in scores[c] if s["same_event"] is not None]), y_same[asked])

    print(f"{len(alerts)} alerts, {y_act.mean():.0%} actionable\n")
    for title, results in (("actionable", act), ("same event", same)):
        print(f"{title:<12}{'condition':<24}{'n':>5}{'AUC':>8}{'Brier':>8}{'ECE':>8}{'>=0.9':>7}{'true':>7}")
        for c, m in results.items():
            print(f"{'':<12}{c:<24}{m['n']:>5}{m['auc']:>8.3f}{m['brier']:>8.3f}{m['ece']:>8.3f}"
                  f"{m['top_n']:>7}{m['top_true']:>7.0%}")
        print()

    comparison = versus_baselines(alerts, deploys, scores, y_act, y_same)
    baseline_html = ""
    for title, columns, rows in comparison:
        print(f"{title:<12}{'method':<28}" + "".join(f"{c:>16}" for c in columns))
        for name, *values in rows:
            print(f"{'':<12}{name:<28}" + "".join(f"{v:>16.3f}" if j == 0 else f"{v:>16.0%}" for j, v in enumerate(values)))
        print()
        baseline_html += (f"<h3>{title}</h3><div class=\"wrap\"><table><tr><th>method</th>"
                          + "".join(f"<th>{c}</th>" for c in columns) + "</tr>"
                          + "".join(f"<tr><td>{html.escape(name)}</td>"
                                    + "".join(f"<td>{v:.3f}</td>" if j == 0 else f"<td>{v:.0%}</td>" for j, v in enumerate(values))
                                    + "</tr>" for name, *values in rows) + "</table></div>")

    page = f"""<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Alert triage</title><style>{CSS}</style>
<h1>Alert triage with Jev</h1>
<p class="dim">{len(alerts)} simulated alerts, {y_act.mean():.0%} actionable &middot; model {html.escape(scores["model"])}
&middot; labels come from the injected faults</p>
<h2>Ranked alerts</h2><div class="wrap">{ranked_list(alerts, deploys, labels, scores)}</div>
<h2>Calibration</h2>
<p>A calibrated score sits on the dashed line: of the alerts scored 0.9, nine in ten are true.
Dot area is the number of alerts in the bin; hover a dot for the count.</p>
<div class="plots"><figure><figcaption>P(actionable)</figcaption>{plot({c: m["bins"] for c, m in act.items()})}</figure>
<figure><figcaption>P(same event as a recent alert)</figcaption>{plot({c: m["bins"] for c, m in same.items()})}</figure></div>
<h2>P(actionable)</h2><div class="wrap">{metrics_table(act)}</div>
<h2>P(same event as a recent alert)</h2>
<p class="dim">Asked only when at least one alert fired in the 15 minutes before; the alert-alone condition has nothing to compare against.</p>
<div class="wrap">{metrics_table(same)}</div>
<p class="dim">AUC: chance that an actionable alert outranks a noise alert. Brier: mean squared error of the score (lower is better).
ECE: average gap between score and observed frequency over {BINS} bins (lower is better).</p>
<h2>Against no model</h2>
<p>The severity label, five hand-written rules, and Alertmanager-style grouping, on the same alerts.
These give an order, not a probability, so only ranking is compared. The rules were written by the
author of the simulator and fit it better than rules for a real system would.
For same event, precision and recall treat a Jev score of 0.5 or more as a yes.</p>{baseline_html}
</html>"""
    (ROOT / "report.html").write_text(page, encoding="utf-8")
    print(f"wrote {ROOT / 'report.html'}")


if __name__ == "__main__":
    main()
