#!/usr/bin/env python3
"""Break the shop on purpose and write down what was done.

    python3 chaos/chaos.py list
    python3 chaos/chaos.py run redis_outage
    python3 chaos/chaos.py run bad_deploy --service payments
    python3 chaos/chaos.py random --minutes 30
    python3 chaos/chaos.py suite                     # every scenario once, shuffled: about 30 minutes
    python3 chaos/chaos.py storm                     # every real fault once, with unrelated noise on top: ~25 min
    python3 chaos/chaos.py storm --at-once 3         # the same, three faults at a time: ~10 min
    python3 chaos/chaos.py storm --at-once 6         # every real fault at once: ~5 min
    python3 chaos/chaos.py storm --quick             # all at once, held 2 min, with staging and batch noise: ~5 min
    python3 chaos/chaos.py heal                      # clear every fault

Every fault is appended to data/chaos-log.jsonl with its start, end, target and whether it
is a real problem or noise. That log is the answer key for the alerts the fault fires.

Runs on the host (inside WSL), standard library only, Python 3.8 or later.
"""

import argparse
import json
import random
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOG = HERE.parent / "data" / "chaos-log.jsonl"
PORTS = {"frontend": 8001, "checkout": 8002, "payments": 8003, "cart": 8004, "search": 8005, "search-index": 8006}
# The noise-only services: nothing calls them, so no real fault reaches them and score_live.py counts
# every alert from them as noise.
NOISE_PORTS = {"checkout-staging": 8007, "batch-reports": 8008}
CALLERS = {"payments": ["checkout"], "cart": ["checkout"], "redis-cart": ["cart"], "checkout": ["frontend"],
           "search": ["frontend"], "search-index": ["search"]}


def blast_radius(service):
    """The service and everything that calls it, directly or not: where its fault can show up."""
    seen, todo = [], [service]
    while todo:
        s = todo.pop()
        if s not in seen:
            seen.append(s)
            todo += CALLERS.get(s, [])
    return seen
TRIAGE = "http://127.0.0.1:9099"


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def post(url, body):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read()


def fault(service, **settings):
    post("http://127.0.0.1:%d/chaos" % (PORTS.get(service) or NOISE_PORTS[service]), settings)


def compose(*args):
    subprocess.run(["docker", "compose", "-f", str(HERE / "docker-compose.yml"), *args], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def deploy(service, summary):
    version = "v%d.%d.%d" % (random.randint(1, 4), random.randint(0, 60), random.randint(0, 9))
    try:
        post(TRIAGE + "/deploys", {"labels": {"service": service}, "version": version, "summary": summary})
    except OSError as exc:
        print("  could not tell triage about the deploy: %s" % exc)
    return version


# Each scenario takes the target service and how long to hold the fault, and returns a note for the log.

def bad_deploy(service, hold):
    """A rollout that makes the service fail 40% of its requests."""
    version = deploy(service, "refactor request handlers")
    time.sleep(20)
    fault(service, error_rate=0.4)
    time.sleep(hold)
    fault(service)
    return "deployed %s, then 40%% errors" % version


def slow_dependency(service, hold):
    """The service answers 2.5 seconds late; its callers slow down with it."""
    fault(service, latency=2.5)
    time.sleep(hold)
    fault(service)
    return "2.5s added latency"


def redis_outage(service, hold):
    """Redis behind cart is stopped; cart, checkout and frontend fail in turn."""
    compose("stop", "redis-cart")
    time.sleep(hold)
    compose("start", "redis-cart")
    return "redis-cart stopped"


def slow_burn(service, hold):
    """8% of requests fail, just over the 5% threshold, and it does not go away. Looks mild; is real."""
    fault(service, error_rate=0.08)
    time.sleep(max(hold, 180))
    fault(service)
    return "8% errors, sustained"


def memory_leak(service, hold):
    """The service leaks 4 MB a second until it hits its 300 MB limit and is killed and restarted."""
    fault(service, leak_mb=4)
    time.sleep(min(hold, 90))
    try:
        fault(service)
    except OSError:  # mid-restart after the kill; the restart cleared the leak anyway
        pass
    return "leaked 4MB/s"


def service_down(service, hold):
    """The service's container is stopped."""
    compose("stop", service)
    time.sleep(hold)
    compose("start", service)
    return "container stopped"


def healthy_deploy(service, hold):
    """A normal rollout: a restart and two minutes of warm-up CPU. Nothing is wrong."""
    version = deploy(service, "dependency bumps")
    compose("restart", service)
    time.sleep(5)
    fault(service, cpu=True)
    time.sleep(min(hold, 130))
    fault(service)
    return "deployed %s, warm-up CPU" % version


def latency_flap(service, hold):
    """p99 sits just over the threshold for two minutes, then recovers by itself."""
    fault(service, latency=1.05)
    time.sleep(min(hold, 130))
    fault(service)
    return "1.05s latency, briefly"


def cpu_spike(service, hold):
    """Two minutes of busy CPU with no effect on requests."""
    fault(service, cpu=True)
    time.sleep(min(hold, 130))
    fault(service)
    return "busy CPU, briefly"


def staging_errors(service, hold):
    """The staging copy fails 60% of requests: the same page-severity error alert as production, from staging."""
    fault(service, error_rate=0.6)
    time.sleep(min(hold, 130))
    fault(service)
    return "60% errors in staging"


def batch_cpu(service, hold):
    """The batch job runs a core hot: a CPU alert on something no user waits for."""
    fault(service, cpu=True)
    time.sleep(min(hold, 130))
    fault(service)
    return "batch job busy"


# name -> (function, actionable, services it can target)
SCENARIOS = {
    "bad_deploy": (bad_deploy, True, ["payments", "checkout", "cart", "search", "search-index"]),
    "slow_dependency": (slow_dependency, True, ["payments", "cart", "search-index"]),
    "redis_outage": (redis_outage, True, ["redis-cart"]),
    "memory_leak": (memory_leak, True, ["payments", "cart", "search"]),
    "service_down": (service_down, True, ["payments", "search-index"]),
    "slow_burn": (slow_burn, True, ["payments", "cart", "search-index"]),
    "healthy_deploy": (healthy_deploy, False, list(PORTS)),
    "latency_flap": (latency_flap, False, ["search", "payments"]),
    "cpu_spike": (cpu_spike, False, list(PORTS)),
    "staging_errors": (staging_errors, False, ["checkout-staging"]),
    "batch_cpu": (batch_cpu, False, ["batch-reports"]),
}


def run(name, service=None, hold=150):
    function, actionable, targets = SCENARIOS[name]
    service = service or random.choice(targets)
    start = now()
    print("%s  %s on %s (%s)" % (start, name, service, "real fault" if actionable else "noise"), flush=True)
    try:
        note = function(service, hold)
    finally:  # clear this fault only: another one may be running beside it
        try:
            if service in PORTS or service in NOISE_PORTS:
                fault(service)
        except OSError:
            pass
        compose("start", service)
    LOG.parent.mkdir(exist_ok=True)
    with open(LOG, "a") as f:
        f.write(json.dumps({"scenario": name, "service": service, "actionable": actionable,
                            "affects": blast_radius(service) if actionable or name == "latency_flap" else [service],
                            "start": start, "end": now(), "note": note}) + "\n")


def pick_targets(names):
    """A different service for each scenario, from the ones it can target, or None if there is no way to.
    Two faults on one service would overwrite each other: POST /chaos replaces the service's settings."""
    for _ in range(200):
        taken = {}
        for name in sorted(names, key=lambda n: len(SCENARIOS[n][2])):
            free = [t for t in SCENARIOS[name][2] if t not in taken.values()]
            if not free:
                break
            taken[name] = random.choice(free)
        else:
            return taken
    return None


def storm(hold, at_once=1, stagger=20):
    """Every real fault once, each with a piece of unrelated noise started 30 seconds into it.
    The noise goes on a service outside the fault's blast radius, so its alerts can be told apart.

    at_once runs that many real faults side by side, stagger seconds apart, each on its own service. Their
    noise goes on a shop service outside all of their blast radii; when none is left, on the staging copy
    (60% errors) or the batch job (busy CPU), which nothing calls."""
    real_ones = [name for name, scenario in SCENARIOS.items() if scenario[1]]
    random.shuffle(real_ones)
    batches = [real_ones[i:i + at_once] for i in range(0, len(real_ones), at_once)]
    for n, batch in enumerate(batches):
        targets = pick_targets(batch)
        if targets is None:  # cannot happen with the shipped scenarios; run them one by one instead
            batches[n + 1:n + 1] = [[name] for name in batch]
            continue
        reach = {s for name in batch for s in blast_radius(targets[name])}
        busy = set(targets.values())
        threads = []
        for k, name in enumerate(batch):
            if k:
                time.sleep(stagger)
            threads.append(threading.Thread(target=run, args=(name, targets[name], hold)))
            threads[-1].start()
        time.sleep(30)
        for _ in batch:  # one piece of noise per real fault, while there is somewhere to put it
            free = [s for s in PORTS if s not in reach and s not in busy]
            spare = [s for s in NOISE_PORTS if s not in busy]
            if free:
                target = random.choice(free)
                name = random.choice(["cpu_spike", "healthy_deploy"])
            elif spare:
                target = random.choice(spare)
                name = {"checkout-staging": "staging_errors", "batch-reports": "batch_cpu"}[target]
            else:
                break
            busy.add(target)
            threads.append(threading.Thread(target=run, args=(name, target, hold)))
            threads[-1].start()
        for t in threads:
            t.join()
        if n < len(batches) - 1:
            time.sleep(100)  # long enough for the alerts to resolve, so batches do not overlap


def heal(quiet=False):
    for service in [*PORTS, *NOISE_PORTS]:
        try:
            fault(service)
        except OSError:
            pass
    compose("start", "redis-cart", *PORTS)
    if not quiet:
        print("all faults cleared")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    sub.add_parser("heal")
    one = sub.add_parser("run")
    one.add_argument("scenario", choices=SCENARIOS)
    one.add_argument("--service")
    one.add_argument("--hold", type=int, default=150, help="seconds to keep the fault (default 150)")
    many = sub.add_parser("random")
    many.add_argument("--minutes", type=float, default=30)
    many.add_argument("--seed", type=int)
    suite = sub.add_parser("suite")
    suite.add_argument("--rounds", type=int, default=1)
    suite.add_argument("--hold", type=int, default=120)
    suite.add_argument("--seed", type=int)
    stormy = sub.add_parser("storm")
    stormy.add_argument("--hold", type=int, default=150)
    stormy.add_argument("--at-once", type=int, default=1,
                        help="real faults to run side by side (default 1; 6 runs them all at once)")
    stormy.add_argument("--stagger", type=int, default=20, help="seconds between faults that run side by side")
    stormy.add_argument("--quick", action="store_true",
                        help="all 6 at once, 10 s apart, held 120 s, with noise beside them: about 5 minutes")
    stormy.add_argument("--seed", type=int)
    args = parser.parse_args()

    if args.command == "list":
        for name, (function, actionable, targets) in SCENARIOS.items():
            print("%-16s %-10s %s  [%s]" % (name, "real fault" if actionable else "noise",
                                           function.__doc__.strip(), ", ".join(targets)))
    elif args.command == "heal":
        heal()
    elif args.command == "run":
        run(args.scenario, args.service, args.hold)
    elif args.command == "storm":
        random.seed(args.seed)
        if args.quick:
            storm(120, 6, 10)
        else:
            storm(args.hold, max(1, args.at_once), args.stagger)
    elif args.command == "suite":
        random.seed(args.seed)
        for _ in range(args.rounds):
            for name in random.sample(list(SCENARIOS), len(SCENARIOS)):
                run(name, hold=args.hold)
                time.sleep(100)  # long enough for the alerts to resolve, so faults do not overlap
    else:
        random.seed(args.seed)
        deadline = time.time() + args.minutes * 60
        while time.time() < deadline:
            run(random.choice(list(SCENARIOS)))
            time.sleep(random.uniform(60, 150))  # let alerts resolve before the next one


if __name__ == "__main__":
    main()
