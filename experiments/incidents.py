"""Group alerts into incidents on top of Alertmanager's grouping.

Grouping only changes how alerts are shown: every alert is listed under its incident, none is dropped.

    python -m experiments.incidents                                 # data/alerts.json with data/deploys.json
    python -m experiments.incidents webhook.json --group-by namespace --deploys deploys.json

1. Alertmanager groups, in code: alerts with the same --group-by labels (the group_by of your
   Alertmanager route; or set ALERT_TRIAGE_GROUP_BY) form a group while they keep arriving less than
   GROUP_GAP apart. There is no default for your own alerts: label names differ between setups. Live,
   the receiver uses each webhook's groupKey instead. The simulated alerts have no resolve times, so
   the gap stands in for a group closing.
2. Splits, Jev, off by default: an alert that lands in an open group gets one Noul per earlier alert of
   that group (same cause?). If none reaches SPLIT_AT, it starts a new group: a second problem on the
   same service.
3. Merges, Jev: each group gets one Noul per other group that was firing in the 15 minutes before
   it started (same cause?). It joins the incident of its best match at MERGE_AT or
   above. A group that joined below CHAIN_AT cannot be joined through, so weak links do not chain.
   Links from RELATED_AT up to MERGE_AT are kept and shown as "possibly related" incidents.
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from typesafe_sdk import Noul, NoulCriteria

from alert_triage.jev_questions import EXPERIMENTS

from alert_triage.triage import (CONCURRENCY, DATA, MAX_RECENT, MODEL, RECENT_WINDOW, ROOT, api_key,
                    build_state, context_for, describe, load_alerts, new_client, when)

GROUP_BY = ("service", "env")  # the labels of the bundled simulated alerts; main() sets it from --group-by
GROUP_GAP = timedelta(minutes=15)
MAX_GROUP_ALERTS = 8  # alerts of a group shown to Jev when comparing groups
# On the 300 simulated alerts (pair precision / recall): Alertmanager groups alone 0.42 / 0.48. Splits at
# 0.3 made both worse (0.41 / 0.41), so they are off; set SPLIT_AT above 0 to try them. Merges at 0.5
# reached 0.31 / 0.72, with one 17-alert incident; at 0.6, 0.43 / 0.54; at 0.7, 0.44 / 0.46.
SPLIT_AT = 0
MERGE_AT = 0.6
CHAIN_AT = 0.8      # no effect at MERGE_AT 0.6 or above on this data
RELATED_AT = 0.3    # merge links from here up to MERGE_AT are shown as "possibly related": 19-38% of them
                    # were the same event, against 3-10% below 0.2


async def ask(client, state: dict, questions: dict, limit: asyncio.Semaphore) -> dict:
    """Each Noul answer as a probability. Asked fresh every time."""
    async with limit:
        for attempt in range(4):
            try:
                resp = await client.system_one(state=state, questions=questions)
                break
            except Exception:
                if attempt == 3:
                    raise
                await asyncio.sleep(2 ** attempt)
    return {q: float(a.noul) for q, a in resp.answers.items()}


def join(links: list[list[tuple[int, float]]], at: float, chain_at: float = 1.01) -> tuple[list[int], list[float | None]]:
    """For items in order, each with (earlier item, P(same cause)) pairs: head[i] is the first item of
    i's cluster, match[i] the probability it joined on (None if it opened one). An item that joined
    below chain_at is a leaf: later items cannot join through it."""
    head, match, joinable = [], [], []
    for i, scored in enumerate(links):
        open_links = [(j, p) for j, p in scored if joinable[j]]
        j, p = max(open_links, key=lambda s: s[1], default=(None, 0.0))
        joined = j is not None and p >= at
        head.append(head[j] if joined else i)
        match.append(p if joined else None)
        joinable.append(not joined or p >= chain_at)
    return head, match


# --- 1. Alertmanager groups ---

def group_key(alert: dict) -> tuple:
    return tuple(alert["labels"].get(label) for label in GROUP_BY)


def label_groups(alerts: list[dict]) -> list[int]:
    """label[k] is the index of the first alert of alerts[k]'s Alertmanager group."""
    open_groups, label = {}, []
    for k, a in enumerate(alerts):
        t = when(a["startsAt"])
        g = open_groups.get(group_key(a))
        if g is None or t - g[1] > GROUP_GAP:
            g = open_groups[group_key(a)] = [k, t]
        g[1] = t
        label.append(g[0])
    return label


# --- 2. splits ---

def link_questions(n: int) -> dict:
    # Of the links this scored at 0.5 or above when asked over every recent alert, 165 joined different
    # events and 107 the same one. A v2 wording ("overlap in time is not a shared cause", plus the
    # rollout-in-between rule) cut false links to 92 but true ones to 77, so it was dropped.
    q = EXPERIMENTS["same_cause_as_alert"]
    return {f"alert_{j:02d}": Noul(instructions=q["question"].replace("{n}", f"{j:02d}"),
                                  criteria=NoulCriteria(true=q["yes"], false=q["no"]))
            for j in range(1, n + 1)}


async def split_scores(client, k: int, alerts: list[dict], deploys: list[dict], label: list[int],
                       limit) -> list[tuple[int, float]]:
    """(earlier alert of the same Alertmanager group, P(same cause)); empty when alerts[k] opened it."""
    earlier = [j for j in range(k) if label[j] == label[k]][-MAX_RECENT:]
    if not earlier:
        return []
    state = build_state(alerts[k], [alerts[j] for j in earlier], context_for(k, alerts, deploys)[1],
                        "alert+recent+deploys")
    answers = await ask(client, state, link_questions(len(earlier)), limit)
    return [(j, answers[f"alert_{n:02d}"]) for n, j in enumerate(earlier, 1)]


def split_groups(splits: list[list[tuple[int, float]]], split_at: float) -> tuple[list[int], dict[int, float]]:
    """group[k] is the first alert of alerts[k]'s group after splitting. split_off maps each alert that
    started a group of its own inside an open Alertmanager group to its best match there."""
    group, split_off = [], {}
    for k, scored in enumerate(splits):
        j, p = max(scored, key=lambda s: s[1], default=(None, 0.0))
        if j is not None and p >= split_at:
            group.append(group[j])
        else:
            group.append(k)
            if j is not None:
                split_off[k] = p
    return group, split_off


# --- 3. merges ---

def group_view(members: list[int], alerts: list[dict]) -> dict:
    return {f"alert_{n:02d}": {**describe(alerts[k]), "fired_at_utc": when(alerts[k]["startsAt"]).strftime("%a %H:%M")}
            for n, k in enumerate(members[:MAX_GROUP_ALERTS], 1)}


def merge_questions(n: int) -> dict:
    q = EXPERIMENTS["same_cause_as_group"]
    return {f"group_{j:02d}": Noul(instructions=q["question"].replace("{n}", f"{j:02d}"),
                                  criteria=NoulCriteria(true=q["yes"], false=q["no"]))
            for j in range(1, n + 1)}


def merge_candidates(g: int, heads: list[int], members: dict[int, list[int]], alerts: list[dict]) -> list[int]:
    """Earlier groups with other group labels still firing in the window before group g started."""
    t = when(alerts[heads[g]]["startsAt"])
    return [i for i in range(g)
            if group_key(alerts[heads[i]]) != group_key(alerts[heads[g]])
            and t - when(alerts[members[heads[i]][-1]]["startsAt"]) <= RECENT_WINDOW][-MAX_RECENT:]


async def merge_scores(client, g: int, heads: list[int], members: dict[int, list[int]], alerts: list[dict],
                       deploys: list[dict], limit) -> list[tuple[int, float]]:
    """(earlier group, P(same cause)). Offline, every group is shown whole; live, it would be asked
    again as a group grows."""
    candidates = merge_candidates(g, heads, members, alerts)
    if not candidates:
        return []
    state = merge_state([alerts[k] for k in members[heads[g]]],
                        [[alerts[k] for k in members[heads[i]]] for i in candidates],
                        context_for(heads[g], alerts, deploys)[1])
    answers = await ask(client, state, merge_questions(len(candidates)), limit)
    return [(i, answers[f"group_{n:02d}"]) for n, i in enumerate(candidates, 1)]


def merge_state(group: list[dict], others: list[list[dict]], rollouts: list[dict]) -> dict:
    """What Jev sees when asked whether `group` shares a cause with each of `others`. live_incidents.py asks
    the same questions as alerts arrive."""
    return {
        "group": group_view(list(range(len(group))), group),
        "other_groups": {f"group_{n:02d}": group_view(list(range(len(o))), o) for n, o in enumerate(others, 1)},
        "recent_deploys": {f"deploy_{n:02d}": {"service": d["service"], "version": d["version"], "change": d["summary"],
                                               "deployed_at_utc": when(d["at"]).strftime("%a %H:%M")}
                           for n, d in enumerate(rollouts, 1)},
    }


# --- output ---

def print_incidents(alerts: list[dict], incidents: list[dict]):
    for n, inc in enumerate(incidents, 1):
        first = alerts[inc["members"][0]]
        print(f"\n#{n:<3} {when(first['startsAt']).strftime('%a %H:%M')}  {len(inc['members'])} alert(s) in "
              f"{len(inc['groups'])} group(s)")
        if inc["related"]:
            print("     possibly related: " + ", ".join(f"#{r['incident']} ({r['p']:.2f})" for r in inc["related"]))
        for g in inc["groups"]:
            how = []
            if g["split_off"] is not None:
                how.append(f"split off its open group, best match {g['split_off']:.2f}")
            if g["merged"] is not None:
                how.append(f"merged at {g['merged']:.2f}")
            print(f"     {'/'.join(str(v) for v in g['key'])}" + (f"  ({'; '.join(how)})" if how else ""))
            for k in g["members"]:
                a = alerts[k]
                print(f"       {when(a['startsAt']).strftime('%H:%M')}  {a['labels']['alertname']:<24} "
                      f"{a['annotations'].get('description') or a['annotations'].get('summary', '')}")
    print(f"\n{len(alerts)} alerts in {len(incidents)} incidents; every alert listed, none suppressed.")


async def run(alerts: list[dict], deploys: list[dict], split_at: float = SPLIT_AT,
              merge_at: float = MERGE_AT) -> list[dict]:
    """split_at=0 keeps Alertmanager's groups whole; merge_at above 1 never merges them."""
    limit = asyncio.Semaphore(CONCURRENCY)
    label = label_groups(alerts)
    async with new_client() as client:
        splits = (await asyncio.gather(*(split_scores(client, k, alerts, deploys, label, limit)
                                         for k in range(len(alerts))))
                  if split_at > 0 else [[] for _ in alerts])
        group, split_off = split_groups(splits, split_at) if split_at > 0 else (label, {})
        heads = sorted(set(group))
        members = {h: [k for k in range(len(alerts)) if group[k] == h] for h in heads}
        merges = (await asyncio.gather(*(merge_scores(client, g, heads, members, alerts, deploys, limit)
                                         for g in range(len(heads))))
                  if merge_at <= 1 else [[] for _ in heads])
        incident, merged = join(merges, merge_at, CHAIN_AT)
        order = sorted(set(incident))
        number = {h: n for n, h in enumerate(order, 1)}  # incident numbers as printed
        related = {n: {} for n in number.values()}
        for g, scored in enumerate(merges):
            for i, p in scored:
                a, b = number[incident[g]], number[incident[i]]
                if a != b and p >= RELATED_AT:
                    related[a][b] = related[b][a] = max(p, related[a].get(b, 0.0))
        incidents = []
        for h in order:
            gs = [g for g in range(len(heads)) if incident[g] == h]
            incidents.append({
                "members": sorted(k for g in gs for k in members[heads[g]]),
                "groups": [{"key": group_key(alerts[heads[g]]), "members": members[heads[g]],
                            "split_off": split_off.get(heads[g]), "merged": merged[g]} for g in gs],
                "related": [{"incident": m, "p": round(p, 3)} for m, p in sorted(related[number[h]].items())],
            })
    return incidents


def main():
    global GROUP_BY
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("alerts", type=Path, nargs="?", default=DATA / "alerts.json")
    parser.add_argument("--deploys", type=Path, default=None)
    parser.add_argument("--group-by", default=os.environ.get("ALERT_TRIAGE_GROUP_BY"),
                        help="the group_by labels of your Alertmanager route, comma-separated "
                             "(default: ALERT_TRIAGE_GROUP_BY)")
    parser.add_argument("--out", type=Path, default=DATA / "incidents.json")
    args = parser.parse_args()
    if args.group_by:
        GROUP_BY = tuple(label.strip() for label in args.group_by.split(",") if label.strip())
    elif args.alerts.resolve() != (DATA / "alerts.json").resolve():
        sys.exit("Which labels group these alerts? Pass the group_by of your Alertmanager route, e.g. "
                 "--group-by namespace, or set ALERT_TRIAGE_GROUP_BY.")
    if not api_key():
        sys.exit("TYPESAFE_API_KEY is not set. Set it in the environment or in a .env file in the project root.")

    deploys_path = args.deploys or (DATA / "deploys.json" if args.alerts == DATA / "alerts.json" else None)
    alerts = load_alerts(args.alerts)
    deploys = json.loads(deploys_path.read_text()) if deploys_path else []
    incidents = asyncio.run(run(alerts, deploys))
    print_incidents(alerts, incidents)
    out = {"model": MODEL, "incidents": [{**inc, "fingerprints": [alerts[k]["fingerprint"] for k in inc["members"]]}
                                         for inc in incidents]}
    args.out.write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
