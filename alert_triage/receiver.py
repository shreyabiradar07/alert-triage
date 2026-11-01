"""Rank and annotate Alertmanager's alerts for the person on call.

Alertmanager posts its alerts here as a webhook. Each new alert gets two Jev questions in one
request: is this a real production problem someone needs to act on now (a Noul), and how much
end-user traffic does it affect (a Score, from "no user traffic" to "users broadly affected")? The
page lists every alert, sorted by urgency × user impact, with the facts a person would otherwise look up: a recent deploy
whose labels match the alert's, how often this alert has fired before, what else is firing in its Alertmanager group.

The page shows the alerts in incidents, one per Alertmanager group (each webhook's groupKey and
groupLabels; live_incidents.py). Grouping only changes the display: nothing is dropped or muted, and
Alertmanager's own routing is untouched.

    POST /alerts          Alertmanager webhook payload
    POST /deploys         {"labels": {"job": "payments"}, "version": "...", "summary": "...", "at": "<RFC 3339, optional>"}
                          labels are the alert labels the rollout belongs to, in your own label names
    GET  /                incidents and ranked alerts (HTML, refreshes itself)
    GET  /alerts.json     ranked alerts (JSON)
    GET  /incidents.json  incidents (JSON)

An alert is scored once, the first time it is seen; repeat notifications and resolves update
it without another Jev call. If scoring fails, or no API key is set, the alert is still
listed, unscored, above the scored ones.

The store is memory only: a restart forgets it. Alerts stay listed for 24 hours.

    python -m alert_triage.receiver                  # http://127.0.0.1:9099

Alertmanager side (alongside the paging route, so nothing is taken away from it):

    route:
      routes:
        - receiver: alert-triage
          continue: true
    receivers:
      - name: alert-triage
        webhook_configs:
          - url: http://127.0.0.1:9099/alerts
"""

import argparse
import asyncio
import hashlib
import html
import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from alert_triage import triage
from alert_triage.live_incidents import LiveIncidents, payload_group
from alert_triage.triage import DEPLOY_WINDOW, MAX_RECENT, RECENT_WINDOW, deploy_note, when

RETENTION = timedelta(hours=24)
MAX_BODY = 5 * 1024 * 1024
ZERO_TIME = "0001-01-01T00:00:00Z"  # Alertmanager's endsAt for an alert that is still firing

# What a score has meant so far: the share of alerts in each band that were real problems on
# the 300 simulated alerts (evaluate.py, full context). Not measured on live traffic.
BANDS = ((0.9, "look first", "94% were real"), (0.8, "likely real", "79% were real"),
         (0.5, "unsure", "about 4 in 10 were real"), (0.0, "probably noise", "5% were real"))


def band(p: float) -> tuple[str, str]:
    return next((name, basis) for floor, name, basis in BANDS if p >= floor)


def fingerprint(alert: dict) -> str:
    return alert.get("fingerprint") or hashlib.sha1(
        json.dumps(alert.get("labels", {}), sort_keys=True).encode()).hexdigest()[:16]


def alert_key(alert: dict) -> str:
    """Alertmanager's fingerprint covers the labels only, so the same alert firing again
    later shares it. Adding startsAt keeps each firing separate."""
    return f"{fingerprint(alert)}@{alert['startsAt']}"


class Store:
    """What the receiver has seen. Times are the alerts' own startsAt, not arrival time,
    so a replay of old alerts behaves like the original."""

    def __init__(self):
        self.entries: dict[str, dict] = {}
        self.deploys: list[dict] = []
        self.usage = {"calls": 0, "seconds": 0.0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
        self.lock = threading.Lock()

    def add(self, alerts: list[dict], group: str | None = None) -> list[dict]:
        """Record a payload; group is its Alertmanager groupKey. Returns the entries seen for the first
        time, oldest first."""
        fresh = []
        with self.lock:
            for a in sorted(alerts, key=lambda a: a["startsAt"]):
                key = alert_key(a)
                if entry := self.entries.get(key):
                    entry["notifications"] += 1
                    entry["alert"]["status"] = a.get("status", "firing")
                    entry["alert"]["endsAt"] = a.get("endsAt", ZERO_TIME)
                else:
                    entry = {"key": key, "alert": a, "notifications": 1, "scores": None, "note": "scoring",
                             "group": group}
                    self.entries[key] = entry
                    fresh.append(entry)
            self._prune()
        return fresh

    def add_usage(self, jev: dict):
        """Add one Jev call to the running totals. Kept apart from the entries, which are pruned after 24 hours."""
        with self.lock:
            self.usage["calls"] += 1
            self.usage["seconds"] += jev["seconds"]
            for key in ("input_tokens", "output_tokens", "cost_usd"):
                self.usage[key] += jev.get(key) or 0

    def usage_totals(self) -> dict:
        with self.lock:
            return dict(self.usage)

    def add_deploy(self, deploy: dict):
        with self.lock:
            self.deploys.append(deploy)
            self.deploys.sort(key=lambda d: d["at"])

    def context(self, alert: dict) -> tuple[list[dict], list[dict]]:
        """The alerts and deploys shortly before this alert, as triage.context_for gives for a file."""
        t = when(alert["startsAt"])
        with self.lock:
            before = sorted((e["alert"] for e in self.entries.values()
                             if e["alert"] is not alert and timedelta(0) <= t - when(e["alert"]["startsAt"]) <= RECENT_WINDOW
                             and e["alert"]["startsAt"] < alert["startsAt"]), key=lambda a: a["startsAt"])
            rollouts = [d for d in self.deploys if timedelta(0) <= t - when(d["at"]) <= DEPLOY_WINDOW]
        return before[-MAX_RECENT:], rollouts

    def annotations(self, alert: dict) -> list[str]:
        """Facts for the person reading the row. Looked up in the store, not judged by the model, and
        without assuming any label names: deploys match on the labels they were posted with, and
        "also firing" means the same Alertmanager group."""
        fp = fingerprint(alert)
        with self.lock:
            own = self.entries.get(alert_key(alert), {})
            others = [e for e in self.entries.values() if e["alert"] is not alert]
        notes = []
        if note := deploy_note(alert, self.context(alert)[1]):
            notes.append(note)
        if earlier := sum(fingerprint(e["alert"]) == fp and e["alert"]["startsAt"] < alert["startsAt"] for e in others):
            notes.append(f"fired {earlier} time{'s' if earlier > 1 else ''} before in the last 24h")
        if (group := own.get("group")) and alert.get("status") != "resolved":
            also = sorted({e["alert"].get("labels", {}).get("alertname", "?") for e in others
                           if e["group"] == group and e["alert"].get("status") != "resolved"})
            if also:
                notes.append(f"also firing in its group: {', '.join(also)}")
        return notes

    def ranked(self) -> list[dict]:
        """Firing before resolved; within each, unscored first, then by priority (urgency × user impact)."""
        with self.lock:
            entries = list(self.entries.values())
        return sorted(entries, key=lambda e: (e["alert"].get("status") == "resolved", e["scores"] is not None,
                                              -(priority(e["scores"]) or 0), e["alert"]["startsAt"]))

    def _prune(self):
        if not self.entries:
            return
        cutoff = max(when(e["alert"]["startsAt"]) for e in self.entries.values()) - RETENTION
        self.entries = {k: e for k, e in self.entries.items() if when(e["alert"]["startsAt"]) >= cutoff}
        self.deploys = [d for d in self.deploys if when(d["at"]) >= cutoff]


async def jev_scorer(jobs: list[tuple[dict, list[dict], list[dict]]]) -> list:
    """Score (alert, recent, rollouts) jobs. A job that fails returns its exception."""
    limit = asyncio.Semaphore(triage.CONCURRENCY)
    async with triage.new_client() as client:
        return await asyncio.gather(*(triage.judge(client, a, recent, rollouts, triage.CONDITIONS[-1], limit,
                                                   same_event=False, states=True, impact=True)
                                      for a, recent, rollouts in jobs), return_exceptions=True)


def receive(store: Store, payload: dict, scorer=jev_scorer, incidents: LiveIncidents | None = None) -> int:
    """Store a webhook payload, score what is new, and place it in an incident. Returns how many alerts
    were new."""
    group = payload_group(payload)
    fresh = store.add(payload.get("alerts", []), group[0])
    if not fresh:
        return 0
    if scorer is jev_scorer and not triage.api_key():
        results = [RuntimeError("TYPESAFE_API_KEY is not set")] * len(fresh)
    else:
        try:
            results = asyncio.run(scorer([(e["alert"], *store.context(e["alert"])) for e in fresh]))
        except Exception as exc:  # the alert stays listed whatever went wrong
            results = [exc] * len(fresh)
    for entry, result in zip(fresh, results):
        if isinstance(result, Exception):
            entry["note"] = f"unscored: {type(result).__name__}: {str(result)[:120]}"
        else:
            entry["scores"], entry["note"] = result, ""
            if result.get("jev"):
                store.add_usage(result["jev"])
    if incidents is not None:
        incidents.receive([e["alert"] for e in fresh], group)
    return len(fresh)


def user_impact(scores: dict | None) -> dict | None:
    """The user-impact answer with its level's name, or None when it was not asked."""
    impact = (scores or {}).get("user_impact")
    return impact and {**impact, "name": triage.IMPACT_LEVELS[impact["level"]]}


def priority(scores: dict | None) -> float | None:
    """What the page sorts by: urgency × user impact, the impact's expected level scaled to 0-1. On
    ITBench-Lite this put a fault alert in the top three in 26 of 30 scenarios, against 8 for urgency
    alone. An alert without an impact answer is sorted by urgency alone, so a missing rating cannot bury it."""
    if not scores:
        return None
    impact = scores.get("user_impact")
    if not impact:
        return scores["actionable"]
    return scores["actionable"] * impact["score"] / (len(triage.IMPACT_LEVELS) - 1)


def as_json(store: Store) -> list[dict]:
    return [{"rank": rank, "actionable": e["scores"] and e["scores"]["actionable"],
             "band": e["scores"] and band(e["scores"]["actionable"])[0],
             "user_impact": user_impact(e["scores"]), "jev": e["scores"] and e["scores"].get("jev"), "note": e["note"],
             "annotations": store.annotations(e["alert"]), "notifications": e["notifications"], "alert": e["alert"]}
            for rank, e in enumerate(store.ranked(), 1)]


PAGE = """<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Alert triage</title><style>
/* One light theme: a soft grey-blue page, off-white cards. Each section has its own colour (.section). */
:root{color-scheme:light;--bg:#eceff3;--card:#fafbfc;--fg:#16181d;--dim:#5a616d;--faint:#868d98;--line:#dde1e7;--chip:#eef1f4;
--hot:#c0362c;--hot-bg:#fbe9e7;--warm:#b25e09;--warm-bg:#fcefdc;--cool:#56606d;--cool-bg:#e9ecf0;
--good:#2f7d4f;--good-bg:#e3f2e9;--link:#1d4ed8;--deploy:#6941c6;--deploy-bg:#efe9fb;
--m0:#dde1e7;--m1:#5b7083;
--s-attention:#d04437;--s-attention-bg:#fbf1f0;--s-noise:#8f99a8;--s-noise-bg:#f2f4f7;
--s-resolved:#3f9a66;--s-resolved-bg:#eff7f2;--s-all:#4a74c9;--s-all-bg:#f0f4fb}
*{box-sizing:border-box}body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif;margin:0}
main{max-width:1000px;margin:0 auto;padding:22px 16px 56px}a{color:var(--link)}code{font-size:12px}
.dim{color:var(--dim)}.faint{color:var(--faint)}.small{font-size:12px}
header{display:flex;justify-content:space-between;align-items:center;gap:12px}header .small{margin-top:2px}
h1{font-size:21px;margin:0;letter-spacing:-.01em}
.tiles{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:14px 0 10px}
.tile{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 14px;text-decoration:none;color:inherit}
.tile b{display:block;font-size:26px;line-height:1.1;font-variant-numeric:tabular-nums}.tile span{font-size:13px;color:var(--dim)}
.tile em{display:block;font-style:normal;font-size:12px;color:var(--faint);margin-top:1px}
.tile{border-top:4px solid var(--sc)}.tile.attention{--sc:var(--s-attention)}.tile.noise{--sc:var(--s-noise)}
.tile.resolved{--sc:var(--s-resolved)}.tile.hot{background:var(--hot-bg)}.tile.hot b{color:var(--hot)}
.section{border:1.5px solid var(--sc);background:var(--sbg);border-radius:16px;padding:6px 14px 4px;margin:18px 0}
.section>h2,.section>summary{margin:6px 0 10px;color:var(--sc)}.section>h2 .n,.section>summary .n{background:var(--sc);color:#fff}
.section.attention{--sc:var(--s-attention);--sbg:var(--s-attention-bg)}.section.noise{--sc:var(--s-noise);--sbg:var(--s-noise-bg)}
.section.noise>summary{color:var(--dim)}.section.resolved{--sc:var(--s-resolved);--sbg:var(--s-resolved-bg)}
.section.all{--sc:var(--s-all);--sbg:var(--s-all-bg)}.section:not([open])>summary{margin-bottom:6px}
.section .card{border-left:4px solid var(--sc)}.section .empty{margin-bottom:10px}.section .wrap{margin-bottom:10px}
.usage{display:flex;flex-wrap:wrap;gap:6px 24px;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:8px 14px;margin:0 0 10px}
.usage div{display:flex;flex-direction:column}.usage span{font-size:11px;color:var(--faint);text-transform:uppercase;letter-spacing:.04em}
.usage b{font-size:15px;font-variant-numeric:tabular-nums}
.legend{font-size:12.5px;color:var(--dim);margin:2px 0 0}
h2{font-size:15px;margin:26px 0 10px;display:flex;align-items:center;gap:8px}
h2 .n{font-size:12px;font-weight:600;background:var(--chip);color:var(--dim);border-radius:999px;padding:1px 8px}
details{margin:12px 0}summary{cursor:pointer;font-weight:600;font-size:15px;padding:4px 0}
details>summary .n{font-size:12px;font-weight:600;background:var(--chip);color:var(--dim);border-radius:999px;padding:1px 8px;margin-left:4px}
.helpbtn{font:inherit;font-size:13px;font-weight:600;color:var(--fg);background:var(--card);border:1px solid var(--line);
border-radius:999px;padding:4px 12px;cursor:pointer}.helpbtn:hover{border-color:var(--faint)}
.drawer{position:fixed;top:0;right:0;bottom:0;width:min(380px,100%);background:var(--card);border-left:1px solid var(--line);
box-shadow:-12px 0 32px rgba(0,0,0,.18);padding:18px 20px 28px;overflow-y:auto;z-index:10;
transform:translateX(100%);visibility:hidden;transition:transform .2s ease,visibility .2s}
.drawer.open{transform:none;visibility:visible}.drawer.still{transition:none}
.dhead{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}.dhead b{font-size:15px}
.x{font:inherit;font-size:20px;line-height:1;background:none;border:0;color:var(--dim);cursor:pointer;padding:2px 6px}
.drawer h4{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);margin:18px 0 6px}
.drawer dl{margin:0;display:grid;grid-template-columns:auto 1fr;gap:6px 12px;font-size:13px;line-height:1.45}
.drawer dt{font-weight:600;white-space:nowrap}.drawer dd{margin:0;color:var(--dim)}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;background:var(--sc)}
.dot.attention{--sc:var(--s-attention)}.dot.noise{--sc:var(--s-noise)}.dot.resolved{--sc:var(--s-resolved)}.dot.all{--sc:var(--s-all)}
.helpbtn{flex:none}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;margin:0 0 12px;
display:grid;grid-template-columns:44px 1fr 210px;gap:0 14px;padding:14px 16px}
.card.quiet{grid-template-columns:1fr 210px;opacity:.92}
.card.resolved{grid-template-columns:1fr 210px;opacity:.7}
.rank{font-size:22px;font-weight:700;color:var(--faint);font-variant-numeric:tabular-nums;line-height:1.2}
.head{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.head h3{font-size:17px;margin:0;letter-spacing:-.01em}
.desc{margin:3px 0 6px;font-size:14px}
.sev{font-size:11px;font-weight:650;text-transform:uppercase;letter-spacing:.04em;padding:1px 7px;border-radius:5px;
background:var(--cool-bg);color:var(--cool)}.sev.critical,.sev.page{background:var(--hot-bg);color:var(--hot)}
.sev.warning{background:var(--warm-bg);color:var(--warm)}.sev.resolved{background:var(--good-bg);color:var(--good)}
.chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center;font-size:12px}
.chip{background:var(--chip);border-radius:6px;padding:1px 7px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11.5px}
.meta{margin-top:5px}.facts{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.fact{font-size:12px;padding:2px 8px;border-radius:6px;background:var(--chip);color:var(--dim)}
.fact.deploy{background:var(--deploy-bg);color:var(--deploy);font-weight:600}
.side{border-left:1px solid var(--line);padding-left:14px;display:flex;flex-direction:column;gap:10px;justify-content:center}
.k{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--faint);font-weight:600}
.u{display:flex;align-items:baseline;gap:6px}.u b{font-size:20px;font-variant-numeric:tabular-nums}
.u.hot b{color:var(--hot)}.u.warm b{color:var(--warm)}.u.cool b{color:var(--cool)}
.u span{font-size:12.5px;color:var(--dim)}
.meter{display:flex;gap:3px;margin:3px 0 2px}.meter i{flex:1;height:7px;border-radius:3px;background:var(--m0)}
.meter i.on{background:var(--m1)}
.iname{font-size:12.5px}.cost{font-size:11.5px;color:var(--faint);margin-top:3px;white-space:normal}
.others{grid-column:1/-1;margin-top:12px;border-top:1px solid var(--line);padding-top:6px}
.card.attention .others{margin-left:58px}
.others .k{margin:4px 0 2px}
.row{display:grid;grid-template-columns:48px 1fr 150px;gap:2px 12px;padding:6px 0;border-bottom:1px dashed var(--line)}
.row:last-child{border-bottom:0}.row .t{color:var(--faint);font-variant-numeric:tabular-nums;font-size:12.5px}
.row .s{text-align:right;font-size:12.5px;white-space:nowrap}.row.resolved{opacity:.6}
.row .facts{margin-top:4px}
.empty{background:var(--card);border:1px dashed var(--line);border-radius:12px;padding:18px;color:var(--dim)}
.empty.calm{border-style:solid;border-color:var(--good);background:var(--good-bg);color:var(--good);font-weight:600}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;background:var(--card);border-radius:12px;overflow:hidden}
th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--line);vertical-align:top;font-size:13px}
th{font-size:11.5px;color:var(--faint);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
tr.resolved{opacity:.6}.wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px}
@media (max-width:720px){.card,.card.quiet,.card.resolved{grid-template-columns:1fr}.rank{display:none}
.side{border-left:0;padding-left:0;border-top:1px solid var(--line);padding-top:10px;margin-top:10px;flex-direction:row;gap:24px}
.side>div{flex:1}.card.attention .others{margin-left:0}.row{grid-template-columns:44px 1fr}.row .s{grid-column:2;text-align:left}
.tiles{gap:6px}.tile b{font-size:22px}}
</style><main>__BODY__</main><script>
// Reload every 15 seconds, keeping open sections open.
try { JSON.parse(sessionStorage.getItem("open") || "[]").forEach(function (id) {
  var d = document.getElementById(id); if (d) d.open = true; }); } catch (e) {}
setTimeout(function () {
  try { sessionStorage.setItem("open", JSON.stringify(Array.prototype.map.call(
    document.querySelectorAll("details[open]"), function (d) { return d.id; }).filter(Boolean))); } catch (e) {}
  location.reload(); }, 15000);
// The summary tiles link to sections; open a collapsed one when it is linked to.
function openLinked() { var d = document.getElementById(location.hash.slice(1)); if (d && d.tagName === "DETAILS") d.open = true; }
window.addEventListener("hashchange", openLinked); openLinked();
// The help panel: opened from the header, kept open across the reload, closed with × or Esc.
var help = document.getElementById("help");
function setHelp(on) { help.classList.toggle("open", on); try { sessionStorage.setItem("help", on ? "1" : ""); } catch (e) {} }
try { if (sessionStorage.getItem("help")) help.classList.add("open"); } catch (e) {}
requestAnimationFrame(function () { requestAnimationFrame(function () { help.classList.remove("still"); }); });
document.addEventListener("click", function (e) {
  if (e.target.closest("[data-help-open]")) setHelp(!help.classList.contains("open"));
  else if (e.target.closest("[data-help-close]")) setHelp(false); });
document.addEventListener("keydown", function (e) { if (e.key === "Escape") setHelp(false); });
</script></html>"""

HELP = """<aside class="drawer still" id="help" aria-label="How to read this page"><div class="dhead">
<b>How to read this page</b><button type="button" class="x" data-help-close aria-label="Close">×</button></div>
<h4>Sections</h4><dl>
<dt><i class="dot attention"></i>Needs attention</dt><dd>Firing, with at least one alert 50% urgent or more, or not scored yet.</dd>
<dt><i class="dot noise"></i>Probably noise</dt><dd>Firing, every alert under 50% urgent.</dd>
<dt><i class="dot resolved"></i>Resolved</dt><dd>Every alert in the incident has resolved: Alertmanager sent its resolve.</dd>
<dt><i class="dot all"></i>All alerts</dt><dd>Every alert received, one row each.</dd></dl>
<h4>Cards</h4><dl>
<dt>Incident</dt><dd>One Alertmanager group, as your route's <code>group_by</code> made it.</dd>
<dt>Headline</dt><dd>The group's highest-priority alert. The rest are listed under "Also in this group · N more", so a card holds 1 + N alerts.</dd>
<dt>Order</dt><dd>Urgency × user impact, highest first.</dd></dl>
<h4>Scores (Jev)</h4><dl>
<dt>Urgency</dt><dd>Probability the alert needs action now. __BANDS__.</dd>
<dt>User impact</dt><dd>End-user traffic affected, 0 to 3 bars: none, not yet, some, broad.</dd></dl>
<h4>Usage</h4><dl>
<dt>Totals</dt><dd>Jev calls since the receiver started; a restart resets them.</dd>
<dt>Cost</dt><dd>Estimate: input tokens × $0.042 per million. Output tokens are free. Failed attempts are not counted.</dd></dl>
<h4>Tags</h4><dl>
<dt>Facts, not judged</dt><dd>A recent deploy matching the alert's labels (purple); earlier firings in the last 24 h.</dd></dl>
<p class="faint small">Nothing is muted; paging is unchanged.</p>
</aside>"""


def hhmm(stamp: str) -> str:
    return when(stamp).astimezone(timezone.utc).strftime("%H:%M")


def ago(stamp: str, now: datetime | None = None) -> str:
    minutes = int(((now or datetime.now(timezone.utc)) - when(stamp)).total_seconds() // 60)
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{minutes} min ago"
    return f"{minutes // 60} h ago" if minutes < 24 * 60 else f"{minutes // (24 * 60)} d ago"


def urgency(scores: dict | None) -> tuple[str, str]:
    """(band name, colour class) for an alert's actionable score."""
    if scores is None:
        return "not scored", ""
    name = band(scores["actionable"])[0]
    return name, {"look first": "hot", "likely real": "warm"}.get(name, "cool")


def cost_text(scores: dict | None) -> str:
    """What judging the alert cost: "Jev 1.4 s · 812 in / 41 out tokens · $0.000034", or "" when not judged."""
    jev = (scores or {}).get("jev")
    if not jev:
        return ""
    text = f'Jev {jev["seconds"]:.1f} s'
    if jev.get("input_tokens") is not None:
        text += f' · {jev["input_tokens"]:,} in / {jev.get("output_tokens") or 0:,} out tokens'
        if jev.get("cost_usd") is not None:
            text += f' · ${jev["cost_usd"]:.6f}'
    return text + (f' · {jev["attempts"]} tries' if jev.get("attempts", 1) > 1 else "")


def usage_html(store: Store) -> str:
    """The Jev calls since the receiver started: calls, average time, tokens, and cost at list price."""
    u = store.usage_totals()
    average = f'{u["seconds"] / u["calls"]:.1f} s' if u["calls"] else "-"
    stats = (("Jev calls", f'{u["calls"]:,}'), ("Avg time", average), ("Input tokens", f'{u["input_tokens"]:,}'),
             ("Output tokens", f'{u["output_tokens"]:,}'), ("Total cost", f"${u['cost_usd']:.{4 if u['cost_usd'] >= 0.01 else 6}f}"))
    return ('<div class="usage" aria-label="Jev usage since the receiver started">'
            + "".join(f'<div><span>{name}</span><b>{value}</b></div>' for name, value in stats) + "</div>")


def urgency_html(entry: dict | None) -> str:
    s = entry and entry["scores"]
    if not s:
        note = entry["note"] if entry else "not scored"
        return f'<div><div class="k">Urgency</div><div class="u"><span>{html.escape(note)}</span></div></div>'
    name, cls = urgency(s)
    return (f'<div><div class="k">Urgency</div><div class="u {cls}"><b>{s["actionable"] * 100:.0f}%</b>'
            f'<span>{html.escape(name)}</span></div><div class="cost">{html.escape(cost_text(s))}</div></div>')


def impact_html(entry: dict | None) -> str:
    impact = user_impact(entry and entry["scores"])
    if not impact:
        return '<div><div class="k">User impact</div><div class="iname faint">not rated</div></div>'
    level = impact["level"]
    # One neutral colour that only fills up, so the meter does not read like the urgency colours.
    steps = "".join('<i class="on"></i>' if k <= level else "<i></i>" for k in range(1, len(triage.IMPACT_LEVELS)))
    return (f'<div><div class="k">User impact</div><div class="meter">{steps}</div>'
            f'<div class="iname">{html.escape(impact["name"])}</div></div>')


def facts_html(alert: dict, store: Store) -> str:
    """The looked-up facts as tags. "Also firing in its group" is left out: the card lists the group."""
    tags = [f'<span class="fact{" deploy" if " deployed " in n else ""}">{html.escape(n)}</span>'
            for n in store.annotations(alert) if not n.startswith("also firing")]
    return f'<div class="facts">{"".join(tags)}</div>' if tags else ""


def describe(alert: dict) -> str:
    annotations = alert.get("annotations", {})
    return annotations.get("description") or annotations.get("summary", "")


def severity_html(alert: dict) -> str:
    severity = alert.get("labels", {}).get("severity")
    tags = f'<span class="sev {html.escape(severity)}">{html.escape(severity)}</span>' if severity else ""
    if alert.get("status") == "resolved":
        tags += '<span class="sev resolved">resolved</span>'
    return tags


def alert_row(entry: dict | None, alert: dict, store: Store) -> str:
    """Another alert of the incident: time, name, description, urgency and user impact, facts."""
    esc, s = html.escape, entry and entry["scores"]
    impact = user_impact(s)
    score = (f'{s["actionable"] * 100:.0f}% urgent<br><span class="dim">{esc(impact["name"]) if impact else ""}</span>'
             f'<div class="cost">{esc(cost_text(s))}</div>'
             if s else f'<span class="dim">{esc(entry["note"] if entry else "not scored")}</span>')
    return (f'<div class="row {esc(alert.get("status", "firing"))}"><div class="t">{hhmm(alert["startsAt"])}</div>'
            f'<div><div class="head"><b>{esc(alert.get("labels", {}).get("alertname", "?"))}</b>{severity_html(alert)}</div>'
            f'<div class="dim">{esc(describe(alert))}</div>{facts_html(alert, store)}</div>'
            f'<div class="s">{score}</div></div>')


def incident_card(i: dict, entries: dict, store: Store, kind: str, rank: int | None = None) -> str:
    """An incident (one Alertmanager group), headed by its most important alert: the firing one with the
    highest urgency × user impact. The rest of the group follows in the same order."""
    esc = html.escape

    def order(a):
        e = entries.get(id(a))
        p = priority(e and e["scores"])
        return (a.get("status") == "resolved", p is not None, -(p or 0), a["startsAt"])

    members = sorted(i["members"], key=order)
    lead, rest = members[0], members[1:]
    entry = entries.get(id(lead))
    chips = "".join(f'<span class="chip">{esc(k)}={esc(v)}</span>' for k, v in sorted(i["labels"].items()))
    count = f'{i["alerts"]} alert{"s" if i["alerts"] != 1 else ""}' + (
        f', {i["firing"]} firing' if i["alerts"] > 1 and i["firing"] else "")
    when_ = (f'started {hhmm(i["started"])} UTC · {ago(i["started"])}' if i["firing"]
             else f'started {hhmm(i["started"])} UTC')
    others = ""
    if rest:
        others = (f'<div class="others"><div class="k">Also in this group · {len(rest)} more</div>'
                  + "".join(alert_row(entries.get(id(a)), a, store) for a in rest) + "</div>")
    return (f'<article class="card {kind}" id="incident-{i["incident"]}">'
            + (f'<div class="rank">{rank}</div>' if rank is not None else "")
            + f'<div><div class="head"><h3>{esc(lead.get("labels", {}).get("alertname", "?"))}</h3>{severity_html(lead)}</div>'
            f'<p class="desc">{esc(describe(lead))}</p>'
            f'<div class="chips">{chips or "<span class=faint>(no group labels)</span>"}</div>'
            f'<div class="meta faint small">{esc(count)} · {esc(when_)}</div>'
            f'{facts_html(lead, store)}</div>'
            f'<div class="side">{urgency_html(entry)}{impact_html(entry)}</div>{others}</article>')


def alert_table(store: Store) -> str:
    """Every alert in one list, highest urgency × user impact first."""
    esc, rows = html.escape, []
    for rank, e in enumerate(store.ranked(), 1):
        a, s = e["alert"], e["scores"]
        labels, status = a.get("labels", {}), a.get("status", "firing")
        impact = user_impact(s)
        score = (f'{s["actionable"] * 100:.0f}% · ' if s else "") + urgency(s)[0]
        context = ", ".join(f"{k}={v}" for k, v in sorted(labels.items()) if k != "alertname")
        facts = "".join(f'<br><span class="dim">{esc(n)}</span>' for n in store.annotations(a))
        rows.append(
            f'<tr class="{esc(status)}"><td>{rank}</td><td>{esc(score)}</td>'
            f'<td>{esc(impact["name"]) if impact else "-"}</td><td>{hhmm(a["startsAt"])}</td>'
            f'<td><b>{esc(labels.get("alertname", "?"))}</b><br><span class="dim">{esc(context)}</span></td>'
            f'<td>{esc(describe(a))}{facts}</td>'
            f'<td>{esc(status)}<br><span class="dim">notified {e["notifications"]}x</span>'
            f'<div class="cost">{esc(cost_text(s))}</div></td></tr>')
    return ('<div class="wrap"><table><tr><th>#</th><th>urgency</th><th>user impact</th><th>fired (UTC)</th>'
            '<th>alert</th><th>detail</th><th>status</th></tr>' + "".join(rows) + "</table></div>")


def as_html(store: Store, incidents: LiveIncidents | None = None) -> str:
    esc = html.escape
    with store.lock:
        entries = {id(e["alert"]): e for e in store.entries.values()}
    all_entries = store.ranked()
    bands = ", ".join(f"<b>{esc(name)}</b> {'≥' if floor else '<'} {(floor or BANDS[-2][0]) * 100:.0f}%"
                      for floor, name, _ in BANDS)
    last = max((e["alert"]["startsAt"] for e in all_entries), default=None)
    received = (f'{len(all_entries)} alert{"s" if len(all_entries) != 1 else ""} received'
                + (f', the last {ago(last)}' if last else "") + " · refreshes every 15 s")
    parts = [f'<header><div><h1>Alert triage</h1><div class="dim small">{esc(received)}</div></div>'
             '<button type="button" class="helpbtn" data-help-open aria-controls="help">How to read this page</button>'
             f'</header>{HELP.replace("__BANDS__", bands)}']
    if incidents is None:
        firing = sum(e["alert"].get("status") != "resolved" for e in all_entries)
        parts += [usage_html(store), f'<p class="legend">{firing} firing, sorted by urgency × user impact.</p>',
                  "<h2>Alerts</h2>",
                  alert_table(store) if all_entries else '<div class="empty">No alerts yet.</div>']
        return PAGE.replace("__BODY__", "".join(parts))

    view = incidents.view(with_alerts=True)

    def top(i, measure):
        return max((measure(entries[id(a)]["scores"]) for a in i["members"]
                    if id(a) in entries and entries[id(a)]["scores"]), default=None)

    def top_actionable(i):
        return top(i, lambda s: s["actionable"])

    def top_priority(i):
        return top(i, priority)

    # Unscored incidents count as needing attention: nothing says they are noise. Within each section,
    # incidents are sorted by their highest priority; unscored ones first.
    attention = [i for i in view if i["firing"] and (top_actionable(i) is None or top_actionable(i) >= 0.5)]
    noise = [i for i in view if i["firing"] and i not in attention]
    resolved = [i for i in view if not i["firing"]]
    for section in (attention, noise, resolved):
        section.sort(key=lambda i: -(top_priority(i) if top_priority(i) is not None else 2))

    def tile(section, label, target, hot=False):
        """A section's incident count, and how many alerts they hold: the header counts alerts."""
        n, alerts = len(section), sum(i["alerts"] for i in section)
        return (f'<a class="tile {target}{" hot" if hot and n else ""}" href="#{target}"><b>{n}</b>'
                f'<span>incident{"s" if n != 1 else ""} {label}</span>'
                f'<em>{alerts} alert{"s" if alerts != 1 else ""}</em></a>')

    parts.append('<div class="tiles">' + tile(attention, "need attention" if len(attention) != 1 else "needs attention",
                                              "attention", hot=True)
                 + tile(noise, "probably noise", "noise") + tile(resolved, "resolved", "resolved") + "</div>"
                 + usage_html(store))
    parts.append(f'<section class="section attention" id="attention"><h2>Needs attention <span class="n">{len(attention)}</span></h2>'
                 + ("".join(incident_card(i, entries, store, "attention", n) for n, i in enumerate(attention, 1))
                    or ('<div class="empty calm">Nothing needs attention right now.</div>' if all_entries
                        else '<div class="empty">No alerts yet. They appear here as Alertmanager sends them.</div>'))
                 + '<div style="height:8px"></div></section>')
    # Every section is always drawn, so the tiles and the help panel always have it to point at.
    parts.append(f'<details class="section noise" id="noise"><summary>Probably noise <span class="n">{len(noise)}</span></summary>'
                 + ("".join(incident_card(i, entries, store, "quiet") for i in noise)
                    or '<div class="empty">No firing incidents rated as noise.</div>') + "</details>")
    parts.append(f'<details class="section resolved" id="resolved"><summary>Resolved <span class="n">{len(resolved)}</span></summary>'
                 + ("".join(incident_card(i, entries, store, "resolved") for i in resolved)
                    or '<div class="empty">No incident has resolved yet.</div>') + "</details>")
    parts.append(f'<details class="section all" id="all"><summary>All alerts as one list <span class="n">{len(all_entries)}</span></summary>'
                 + (alert_table(store) if all_entries else '<div class="empty">No alerts yet.</div>') + "</details>")
    return PAGE.replace("__BODY__", "".join(parts))


def make_handler(store: Store, scorer=jev_scorer, incidents: LiveIncidents | None = None):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: str, kind: str = "application/json"):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", f"{kind}; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/":
                self._send(200, as_html(store, incidents), "text/html")
            elif self.path == "/alerts.json":
                self._send(200, json.dumps(as_json(store)))
            elif self.path == "/incidents.json":
                self._send(200, json.dumps(incidents.view() if incidents else []))
            else:
                self._send(404, '{"error": "not found"}')

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length > MAX_BODY:
                    return self._send(413, '{"error": "payload too large"}')
                body = json.loads(self.rfile.read(length))
                if self.path == "/alerts":
                    alerts = body.get("alerts")
                    if not isinstance(alerts, list) or not all(isinstance(a, dict) and "startsAt" in a for a in alerts):
                        raise ValueError("expected an Alertmanager webhook payload with an alerts list")
                    for a in alerts:
                        when(a["startsAt"])
                    self._send(200, json.dumps({"received": len(alerts), "new": receive(store, body, scorer, incidents)}))
                elif self.path == "/deploys":
                    at = body.get("at") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    when(at)
                    labels = body.get("labels")
                    if not isinstance(labels, dict) or not labels or not all(
                            isinstance(k, str) and isinstance(v, str) for k, v in labels.items()):
                        raise ValueError('expected "labels": the alert labels the rollout belongs to, e.g. {"job": "payments"}')
                    store.add_deploy({"labels": labels, "version": str(body.get("version", "")),
                                      "summary": str(body.get("summary", "")), "at": at})
                    self._send(200, '{"ok": true}')
                else:
                    self._send(404, '{"error": "not found"}')
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                self._send(400, json.dumps({"error": str(exc)}))

        def log_message(self, fmt, *args):
            if self.command == "POST":
                super().log_message(fmt, *args)

    return Handler


def main():
    parser = argparse.ArgumentParser(description="Alertmanager webhook receiver for alert-triage")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9099)
    args = parser.parse_args()
    if not triage.api_key():
        print("TYPESAFE_API_KEY is not set: alerts will be listed unscored.")
    server = ThreadingHTTPServer((args.host, args.port), make_handler(Store(), incidents=LiveIncidents()))
    print(f"listening on http://{args.host}:{args.port}  (POST /alerts, POST /deploys, GET /, GET /incidents.json)")
    server.serve_forever()


if __name__ == "__main__":
    main()
