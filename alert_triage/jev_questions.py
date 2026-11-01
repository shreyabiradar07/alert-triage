"""The questions alert-triage asks Jev, loaded from two files:

    questions.json              Q: what the receiver asks about every alert (urgency, user impact)
    experiment_questions.json   EXPERIMENTS: questions only the offline experiments ask

Edit either file to try your own wording, or point ALERT_TRIAGE_QUESTIONS / ALERT_TRIAGE_EXPERIMENT_QUESTIONS
at another one. Both are checked on load, so a missing part stops the program with a message saying which.

Keys starting with "_" are notes for the reader and are not sent to Jev. Nothing is cached: every alert
is asked fresh, so an edit applies to every alert scored after a restart.
"""

import json
import os
from pathlib import Path

HERE = Path(__file__).parent
PATH = Path(os.environ.get("ALERT_TRIAGE_QUESTIONS") or HERE / "questions.json")
EXPERIMENTS_PATH = Path(os.environ.get("ALERT_TRIAGE_EXPERIMENT_QUESTIONS") or HERE.parent / "experiments" / "experiment_questions.json")

REQUIRED = {
    ("urgency",): ("question", "context_rules", "yes", "no", "no_when_recent_alerts_are_sent"),
    ("urgency", "context_rules"): ("recent_alerts", "recent_deploys", "alert_states"),
    ("user_impact",): ("question", "levels"),
}
EXPERIMENT_NAMES = ("same_event_as_any_recent_alert", "same_cause_as_alert", "same_cause_as_group")


def check(q: dict, path: Path = PATH):
    def fail(what):
        raise SystemExit(f"{path}: {what}")
    for parts, keys in REQUIRED.items():
        part = q
        for step in parts:
            part = part.get(step) if isinstance(part, dict) else None
        if not isinstance(part, dict):
            fail(f"missing section {'.'.join(parts)}")
        for key in keys:
            if key not in part:
                fail(f"missing {'.'.join(parts + (key,))}")
    levels = q["user_impact"]["levels"]
    if not isinstance(levels, list) or len(levels) < 2 or not all({"name", "description"} <= set(l) for l in levels):
        fail("user_impact.levels must be a list of at least 2 {\"name\", \"description\"} entries, lowest first")


def check_experiments(e: dict, path: Path = EXPERIMENTS_PATH):
    def fail(what):
        raise SystemExit(f"{path}: {what}")
    for name in EXPERIMENT_NAMES:
        if not isinstance(e.get(name), dict) or not {"question", "yes", "no"} <= set(e[name]):
            fail(f"{name} needs question, yes and no")
        if name != "same_event_as_any_recent_alert" and "{n}" not in e[name]["question"]:
            fail(f"{name}.question must contain {{n}}")


Q = json.loads(PATH.read_text(encoding="utf-8"))
check(Q)
EXPERIMENTS = json.loads(EXPERIMENTS_PATH.read_text(encoding="utf-8"))
check_experiments(EXPERIMENTS)
