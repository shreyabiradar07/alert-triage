"""itbench.py demo: a recorded scenario becomes webhooks a running receiver can show. With a stub in place of Jev.

    python -m unittest tests.test_itbench
"""

import contextlib
import io
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from alert_triage.live_incidents import LiveIncidents
from alert_triage.receiver import Store, make_handler
from benchmark import itbench


def snapshot(alerts):
    return json.dumps([{"state": "firing", "labels": {"alertname": a, "namespace": name, "service": name},
                        "annotations": {}, "activeAt": at} for a, name, at in alerts])


def scenario(root):
    """Two minutes of alerts: A and B fire, then B resolves while C starts in another namespace."""
    folder = root / "Scenario-1"
    (folder / "alerts").mkdir(parents=True)
    (folder / "ground_truth.yaml").write_text(
        "groups: [{id: g, namespace: shop, filter: [shop]}]\nalerts: [{id: A, group_id: g}]\n")
    a = ("A", "shop", "2025-12-15T17:30:00.5Z")
    b = ("B", "shop", "2025-12-15T17:30:01Z")
    c = ("C", "other", "2025-12-15T17:31:00Z")
    (folder / "alerts" / "alerts_at_2025-12-15T17-30-09.1.json").write_text(snapshot([a, b]))
    (folder / "alerts" / "alerts_at_2025-12-15T17-31-09.1.json").write_text(snapshot([a, c]))
    return folder


class WebhooksTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.folder = scenario(self.root)

    def test_new_alerts_and_resolves_arrive_by_group_with_moved_times(self):
        steps = list(itbench.webhooks(self.folder, ["namespace"], timedelta(days=1)))
        first = steps[0][1]
        self.assertEqual([(p["groupLabels"], [a["labels"]["alertname"] for a in p["alerts"]]) for p in first],
                         [({"namespace": "shop"}, ["A", "B"])])
        self.assertEqual(first[0]["alerts"][1]["startsAt"], "2025-12-16T17:30:01Z")
        self.assertNotIn("_arrived", first[0]["alerts"][0])
        later = {p["groupLabels"]["namespace"]: p["alerts"] for p in steps[1][1]}
        self.assertEqual([(a["labels"]["alertname"], a["status"]) for a in later["shop"]], [("B", "resolved")])
        self.assertEqual(later["shop"][0]["endsAt"], "2025-12-16T17:31:09Z")
        self.assertEqual([a["labels"]["alertname"] for a in later["other"]], ["C"])

    def test_demo_fills_a_running_receiver_and_prints_the_answer_key(self):
        async def scorer(jobs):
            return [{"actionable": 0.9 if a["labels"]["alertname"] == "A" else 0.2, "same_event": None,
                     "user_impact": None} for a, _, _ in jobs]

        store, live = Store(), LiveIncidents()
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store, scorer, live))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        output = io.StringIO()
        with mock.patch.object(itbench, "RAW", self.root), contextlib.redirect_stdout(output):
            itbench.demo("Scenario-1", ["namespace"], 100000, f"http://127.0.0.1:{server.server_port}")
        status = {e["alert"]["labels"]["alertname"]: e["alert"]["status"] for e in store.entries.values()}
        self.assertEqual(status, {"A": "firing", "B": "resolved", "C": "firing"})
        self.assertEqual(len(live.view()), 2)
        newest = max(datetime.fromisoformat(e["alert"]["startsAt"].replace("Z", "+00:00")) for e in store.entries.values())
        self.assertLess(abs(datetime.now(timezone.utc) - newest), timedelta(minutes=2))
        self.assertIn("1. strict  0.90  A", output.getvalue())

    def test_unknown_scenario_says_what_to_do(self):
        with mock.patch.object(itbench, "RAW", self.root), self.assertRaises(SystemExit) as caught:
            itbench.demo("Scenario-99", ["namespace"], 30, "http://127.0.0.1:1")
        self.assertIn("fetch", str(caught.exception))

    def test_missing_receiver_says_how_to_start_it(self):
        with mock.patch.object(itbench, "RAW", self.root), self.assertRaises(SystemExit) as caught:
            with contextlib.redirect_stdout(io.StringIO()):
                itbench.demo("Scenario-1", ["namespace"], 100000, "http://127.0.0.1:1")
        self.assertIn("python -m alert_triage.receiver", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
