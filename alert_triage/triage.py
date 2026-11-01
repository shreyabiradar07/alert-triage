"""Score alerts with Jev: P(actionable) and P(same event as a recent alert), and, for the receiver,
how much end-user traffic the alert affects.

The scores only rank and annotate. Nothing here drops, silences or groups an alert.

Each alert is one request with two Nouls (plus the user-impact Score when asked for). Three conditions
differ only in the state Jev sees:

    alert                   the alert's own labels and annotations
    alert+recent            plus the alerts that fired in the 15 minutes before it
    alert+recent+deploys    plus the deploys in the 45 minutes before it

    python -m alert_triage.triage score                     # all three conditions over data/, for evaluate.py
    python -m alert_triage.triage rank webhook.json         # rank the alerts in an Alertmanager webhook payload
    python -m alert_triage.triage rank webhook.json --deploys deploys.json
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from typesafe_sdk import AsyncTypeSafeClient, Noul, NoulCriteria, Score

from alert_triage.jev_questions import EXPERIMENTS, Q

MODEL = "jev-1.13.0"  # pinned: jev-latest moves when a new release ships
INPUT_PRICE_PER_MTOK = 0.042  # USD, list price of MODEL (docs.typesafe.ai/models); output tokens are free
CONDITIONS = ("alert", "alert+recent", "alert+recent+deploys")
RECENT_WINDOW = timedelta(minutes=15)
DEPLOY_WINDOW = timedelta(minutes=45)
MAX_RECENT = 12
CONCURRENCY = 8
ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"


def api_key() -> str:
    """The environment first, then a gitignored .env in the project root. Never printed."""
    if key := os.environ.get("TYPESAFE_API_KEY"):
        return key
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            name, _, value = line.strip().strip('"').partition("=")
            if name.strip() == "TYPESAFE_API_KEY":
                return value.strip().strip('"\'')
    return ""


def when(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def describe(alert: dict) -> dict:
    return {**alert["labels"], **alert["annotations"]}


def context_for(k: int, alerts: list[dict], deploys: list[dict]) -> tuple[list[dict], list[dict]]:
    """The alerts and deploys that came before alerts[k], inside their windows. alerts is sorted by startsAt."""
    t = when(alerts[k]["startsAt"])
    recent = [a for a in alerts[:k] if t - when(a["startsAt"]) <= RECENT_WINDOW][-MAX_RECENT:]
    rollouts = [d for d in deploys if timedelta(0) <= t - when(d["at"]) <= DEPLOY_WINDOW]
    return recent, rollouts


def minutes_before(t: datetime, stamp: str) -> float:
    return round((t - when(stamp)).total_seconds() / 60, 1)


def build_state(alert: dict, recent: list[dict], rollouts: list[dict], condition: str,
                states: bool = False) -> dict:
    """states=True adds whether each recent alert is still firing. Only the live receiver knows
    that; the simulated alerts carry no resolve times."""
    t = when(alert["startsAt"])
    state = {"alert": {**describe(alert), "fired_at_utc": t.strftime("%a %H:%M")}}
    if condition != "alert":
        # Named entries, not a list: logsift measured list positions bleeding into each other.
        state["recent_alerts"] = {
            f"alert_{j + 1:02d}": {**describe(a), "minutes_before_alert": minutes_before(t, a["startsAt"]),
                                   **({"state": "resolved" if a.get("status") == "resolved" else "firing"} if states else {})}
            for j, a in enumerate(recent)}
    if condition == "alert+recent+deploys":
        state["recent_deploys"] = {
            f"deploy_{j + 1:02d}": {**({"labels": d["labels"]} if "labels" in d else {"service": d["service"]}),
                                    "version": d["version"], "change": d["summary"],
                                    "minutes_before_alert": minutes_before(t, d["at"])}
            for j, d in enumerate(rollouts)}
    return state


def build_questions(condition: str, has_recent: bool, states: bool = False) -> dict:
    """The urgency question (and, for the batch experiment, the same-event one), worded from questions.json."""
    # v1 only said the context was there. Jev then read any nearby alert as evidence of an incident:
    # noise with other alerts around it rose from 0.52 to 0.65, and the 0.9+ scores went from 100% to
    # 79% true. v2 says which context counts (urgency.context_rules).
    urgency, rules = Q["urgency"], Q["urgency"]["context_rules"]
    context = {"alert": [], "alert+recent": [rules["recent_alerts"]],
               "alert+recent+deploys": [rules["recent_alerts"], rules["recent_deploys"]]}[condition]
    if states and condition != "alert":
        # Live chaos runs: a marginal alert scored 0.76 because the same service had errors four
        # minutes earlier, already resolved. Of four wordings rescored on 34 live alerts, the shipped
        # alert_states rule left no noise at 0.5 or above and no real alert below it.
        context.append(rules["alert_states"])
    no = urgency["no"] + ("" if condition == "alert" else ", " + urgency["no_when_recent_alerts_are_sent"])
    qs = {"actionable": Noul(instructions=" ".join([urgency["question"], *context]),
                             criteria=NoulCriteria(true=urgency["yes"], false=no))}
    if has_recent:  # with nothing recent the answer is no by definition, so it is not asked
        same = EXPERIMENTS["same_event_as_any_recent_alert"]
        qs["same_event"] = Noul(instructions=same["question"], criteria=NoulCriteria(true=same["yes"], false=same["no"]))
    return qs


# How much end-user traffic an alert affects: a dimension of its own, apart from whether it needs action
# now. A real error spike in a batch job is actionable but touches no user; a slow checkout is both.
# Each level is a situation the model can match on its own, lowest first (questions.json, user_impact).
IMPACT_LEVELS = tuple(level["name"] for level in Q["user_impact"]["levels"])
IMPACT = Score(instructions=Q["user_impact"]["question"],
               criteria=[level["description"] for level in Q["user_impact"]["levels"]])


async def judge(client, alert: dict, recent: list[dict], rollouts: list[dict], condition: str,
                limit: asyncio.Semaphore, same_event: bool = True,
                states: bool = False, impact: bool = False) -> dict:
    """Score one alert given its context: {"actionable": p, "same_event": p or None, "user_impact": ...}.
    With same_event=False the same-event question is not asked. With impact=True the user-impact Score is
    asked in the same request; its answer is {"score": expected level, "level": most likely level,
    "probabilities": [p per level]}, otherwise None. Every call asks Jev: nothing is cached.

    "jev" holds what the call cost: its seconds (from the first attempt to the answer, retries included, not
    the wait for a free slot), attempts, input and output tokens as the API reports them, cost at list price
    (the tokens of failed attempts are not reported, so not counted), and request ID."""
    state = build_state(alert, recent, rollouts, condition, states)
    questions = build_questions(condition, has_recent=same_event and condition != "alert" and bool(recent),
                                states=states)
    if impact:
        questions["user_impact"] = IMPACT
    async with limit:
        started = time.perf_counter()
        for attempt in range(4):
            try:
                resp = await client.system_one(state=state, questions=questions)
                break
            except Exception:
                if attempt == 3:
                    raise
                await asyncio.sleep(2 ** attempt)
        seconds = time.perf_counter() - started
    row = {q: (float(a.noul) if a.type == "noul" else
               {"score": float(a.score),
                "probabilities": [float(a.probabilities[i]) for i in range(len(IMPACT_LEVELS))]})
           for q, a in ((q, resp.answers[q]) for q in questions)}
    impact_answer = row.get("user_impact")
    if impact_answer:
        probs = impact_answer["probabilities"]
        impact_answer = {**impact_answer, "level": max(range(len(probs)), key=probs.__getitem__)}
    return {"actionable": row["actionable"], "same_event": row.get("same_event"), "user_impact": impact_answer,
            "jev": call_cost(resp, seconds, attempt + 1)}


def call_cost(resp, seconds: float, attempts: int) -> dict:
    usage = getattr(resp, "usage", None)
    try:
        request_id = resp.request_id
    except Exception:  # the SDK raises when the response had no request-ID header
        request_id = None
    input_tokens = getattr(usage, "input_tokens", None)
    return {"seconds": round(seconds, 3), "attempts": attempts,
            "input_tokens": input_tokens, "output_tokens": getattr(usage, "output_tokens", None),
            "cost_usd": None if input_tokens is None else input_tokens * INPUT_PRICE_PER_MTOK / 1_000_000,
            "request_id": request_id}


def new_client() -> AsyncTypeSafeClient:
    return AsyncTypeSafeClient(api_key=api_key(), model=MODEL)


async def score_alerts(alerts: list[dict], deploys: list[dict], condition: str) -> list[dict]:
    """One dict per alert, in the order given. Context is looked up in the same list."""
    limit = asyncio.Semaphore(CONCURRENCY)
    async with new_client() as client:
        return await asyncio.gather(*(judge(client, alerts[k], *context_for(k, alerts, deploys), condition, limit)
                                      for k in range(len(alerts))))


def load_alerts(path: Path) -> list[dict]:
    payload = json.loads(Path(path).read_text())
    alerts = payload["alerts"] if isinstance(payload, dict) else payload
    return sorted(alerts, key=lambda a: a["startsAt"])


def deploy_labels(deploy: dict) -> dict:
    """The labels a deploy names: its own "labels" (the receiver), or "service" (the simulated data)."""
    return deploy["labels"] if "labels" in deploy else {"service": deploy["service"]}


def deploy_note(alert: dict, rollouts: list[dict]) -> str:
    """A fact for the annotation column, looked up in code rather than judged. A deploy is the alert's
    own when every label it names has the same value on the alert."""
    labels = alert.get("labels", {})
    own = [d for d in rollouts if all(labels.get(k) == v for k, v in deploy_labels(d).items())]
    if not own:
        return ""
    d = own[-1]
    what = ", ".join(f"{k}={v}" for k, v in sorted(deploy_labels(d).items()))
    return f"{what} {d['version']} deployed {minutes_before(when(alert['startsAt']), d['at']):.0f}m before"


def print_ranked(alerts: list[dict], deploys: list[dict], scores: list[dict]):
    rows = sorted(zip(scores, range(len(alerts))), key=lambda r: -r[0]["actionable"])
    print(f"{'#':>3}  {'act':>4}  {'same':>4}  {'fired':<9} {'alert':<24} {'service':<14} {'env':<8} detail")
    for rank, (s, k) in enumerate(rows, 1):
        a = alerts[k]
        same = "   -" if s["same_event"] is None else f"{s['same_event']:.2f}"
        note = deploy_note(a, context_for(k, alerts, deploys)[1])
        detail = a["annotations"]["description"] + (f"  [{note}]" if note else "")
        print(f"{rank:>3}  {s['actionable']:.2f}  {same}  {when(a['startsAt']).strftime('%a %H:%M'):<9} "
              f"{a['labels']['alertname']:<24} {a['labels'].get('service', ''):<14} {a['labels'].get('env', ''):<8} {detail}")
    print(f"\n{len(alerts)} alerts in, {len(alerts)} alerts out: ranked, none suppressed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("score", help="score data/alerts.json under all three conditions")
    rank = sub.add_parser("rank", help="rank the alerts in an Alertmanager webhook payload")
    rank.add_argument("alerts", type=Path)
    rank.add_argument("--deploys", type=Path)
    args = parser.parse_args()

    if not api_key():
        sys.exit("TYPESAFE_API_KEY is not set. Set it in the environment or in a .env file in the project root.")

    if args.command == "score":
        alerts = load_alerts(DATA / "alerts.json")
        deploys = json.loads((DATA / "deploys.json").read_text())
        out = {"model": MODEL, "fingerprints": [a["fingerprint"] for a in alerts]}
        for condition in CONDITIONS:
            out[condition] = asyncio.run(score_alerts(alerts, deploys, condition))
            print(f"{condition}: scored {len(alerts)} alerts")
        (DATA / "scores.json").write_text(json.dumps(out))
    else:
        alerts = load_alerts(args.alerts)
        deploys = json.loads(args.deploys.read_text()) if args.deploys else []
        condition = "alert+recent+deploys" if deploys else "alert+recent"
        print_ranked(alerts, deploys, asyncio.run(score_alerts(alerts, deploys, condition)))


if __name__ == "__main__":
    main()
