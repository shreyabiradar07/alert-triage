"""Receiver and store behaviour, with a stub in place of Jev.

    python -m unittest tests.test_receiver
"""

import asyncio
import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

from alert_triage import triage
from alert_triage.live_incidents import LiveIncidents
from alert_triage.receiver import Store, as_html, as_json, band, make_handler, priority, receive


def alert(name, minute, service="payments", status="firing", fingerprint=None):
    return {"status": status, "labels": {"alertname": name, "service": service, "env": "prod"},
            "annotations": {"description": f"{name} detail"},
            "startsAt": f"2026-10-05T10:{minute:02d}:00Z", "endsAt": "0001-01-01T00:00:00Z",
            "fingerprint": fingerprint or f"{name}-{service}"}


class Stub:
    """Scores by alert name and records the context each alert was scored with."""

    def __init__(self, fail=False):
        self.jobs, self.fail = [], fail

    async def __call__(self, jobs):
        if self.fail:
            raise RuntimeError("jev is down")
        self.jobs += jobs
        return [{"actionable": 0.95 if a["labels"]["alertname"] == "HighErrorRate" else 0.2, "same_event": None}
                for a, _, _ in jobs]


def names(ranked):
    return [r["alert"]["labels"]["alertname"] for r in ranked]


class StoreTest(unittest.TestCase):
    def test_repeat_is_scored_once_and_resolve_updates(self):
        store, stub = Store(), Stub()
        a = alert("HighErrorRate", 0)
        self.assertEqual(receive(store, {"alerts": [a]}, stub), 1)
        self.assertEqual(receive(store, {"alerts": [dict(a)]}, stub), 0)
        self.assertEqual(receive(store, {"alerts": [{**a, "status": "resolved"}]}, stub), 0)
        self.assertEqual(len(stub.jobs), 1)
        (entry,) = store.ranked()
        self.assertEqual((entry["notifications"], entry["alert"]["status"]), (3, "resolved"))

    def test_context_is_the_15_minutes_before_across_payloads(self):
        store, stub = Store(), Stub()
        receive(store, {"alerts": [alert("TargetDown", 0)]}, stub)
        receive(store, {"alerts": [alert("HighLatencyP99", 10)]}, stub)
        store.add_deploy({"labels": {"service": "payments"}, "version": "v1", "summary": "", "at": "2026-10-05T10:05:00Z"})
        store.add_deploy({"labels": {"service": "cart"}, "version": "v2", "summary": "", "at": "2026-10-05T08:00:00Z"})
        receive(store, {"alerts": [alert("HighErrorRate", 20)]}, stub)
        _, recent, rollouts = stub.jobs[-1]
        self.assertEqual(names([{"alert": r} for r in recent]), ["HighLatencyP99"])
        self.assertEqual([d["version"] for d in rollouts], ["v1"])

    def test_alerts_in_one_payload_see_earlier_ones(self):
        store, stub = Store(), Stub()
        receive(store, {"alerts": [alert("HighErrorRate", 3), alert("TargetDown", 1)]}, stub)
        contexts = {a["labels"]["alertname"]: [r["labels"]["alertname"] for r in recent] for a, recent, _ in stub.jobs}
        self.assertEqual(contexts, {"TargetDown": [], "HighErrorRate": ["TargetDown"]})

    def test_failed_scoring_still_lists_the_alert_first(self):
        store = Store()
        receive(store, {"alerts": [alert("HighErrorRate", 0)]}, Stub())
        receive(store, {"alerts": [alert("HighCPU", 1)]}, Stub(fail=True))
        ranked = as_json(store)
        self.assertEqual(names(ranked), ["HighCPU", "HighErrorRate"])
        self.assertIsNone(ranked[0]["actionable"])
        self.assertIn("jev is down", ranked[0]["note"])

    def test_ranking_firing_by_score_then_resolved(self):
        store, stub = Store(), Stub()
        receive(store, {"alerts": [alert("HighCPU", 0), alert("HighErrorRate", 1),
                                   alert("HighErrorRate", 2, service="cart", status="resolved")]}, stub)
        ranked = as_json(store)
        self.assertEqual([(r["alert"]["labels"]["alertname"], r["alert"]["status"], r["band"]) for r in ranked],
                         [("HighErrorRate", "firing", "look first"), ("HighCPU", "firing", "probably noise"),
                          ("HighErrorRate", "resolved", "look first")])

    def test_annotations_are_facts_from_the_store(self):
        store, stub = Store(), Stub()
        store.add_deploy({"labels": {"service": "payments"}, "version": "v7", "summary": "", "at": "2026-10-05T10:36:00Z"})
        receive(store, {"groupKey": "p", "alerts": [alert("HighCPU", 0, fingerprint="cpu"),
                                                    alert("HighCPU", 20, fingerprint="cpu")]}, stub)
        receive(store, {"groupKey": "p", "alerts": [alert("HighCPU", 40, fingerprint="cpu"), alert("HighErrorRate", 41)]}, stub)
        receive(store, {"groupKey": "c", "alerts": [alert("HighErrorRate", 41, service="cart")]}, stub)
        notes = {(r["alert"]["labels"]["alertname"], r["alert"]["labels"]["service"], r["alert"]["startsAt"][14:16]):
                 r["annotations"] for r in as_json(store)}
        self.assertEqual(notes[("HighCPU", "payments", "40")],
                         ["service=payments v7 deployed 4m before", "fired 2 times before in the last 24h",
                          "also firing in its group: HighCPU, HighErrorRate"])
        self.assertEqual(notes[("HighErrorRate", "cart", "41")], [])

    def test_notes_assume_no_label_names(self):
        """Alerts labelled job= (no service label) still get the deploy and group notes."""
        def job_alert(name, minute):
            return {"status": "firing", "labels": {"alertname": name, "job": "payments", "instance": "p-1"},
                    "annotations": {}, "startsAt": f"2026-10-05T10:{minute:02d}:00Z", "fingerprint": name}
        store, stub = Store(), Stub()
        store.add_deploy({"labels": {"job": "payments"}, "version": "v3", "summary": "", "at": "2026-10-05T10:00:00Z"})
        store.add_deploy({"labels": {"job": "cart"}, "version": "v9", "summary": "", "at": "2026-10-05T10:01:00Z"})
        receive(store, {"groupKey": "{}:{job=\"payments\"}", "groupLabels": {"job": "payments"},
                        "alerts": [job_alert("HighCPU", 2), job_alert("HighErrorRate", 3)]}, stub)
        notes = {r["alert"]["labels"]["alertname"]: r["annotations"] for r in as_json(store)}
        self.assertEqual(notes["HighErrorRate"], ["job=payments v3 deployed 3m before", "also firing in its group: HighCPU"])

    def test_bands(self):
        self.assertEqual([band(p)[0] for p in (0.95, 0.9, 0.85, 0.6, 0.49, 0.0)],
                         ["look first", "look first", "likely real", "unsure", "probably noise", "probably noise"])

    def test_user_impact_is_shown_by_its_level_name(self):
        async def scorer(jobs):
            return [{"actionable": 0.9, "same_event": None,
                     "user_impact": {"score": 2.6, "level": 3, "probabilities": [0.0, 0.1, 0.2, 0.7]}}
                    for _ in jobs]
        store = Store()
        receive(store, {"alerts": [alert("HighErrorRate", 1)]}, scorer)
        impact = as_json(store)[0]["user_impact"]
        self.assertEqual((impact["name"], impact["score"]), ("users broadly affected", 2.6))
        self.assertIn("users broadly affected", as_html(store))

    def test_sorted_by_urgency_times_user_impact(self):
        """An urgent alert that hits no users goes below a less urgent one that hits many."""
        answers = {"HighCPU": (0.9, 0.0), "HighErrorRate": (0.7, 3.0)}

        async def scorer(jobs):
            return [{"actionable": answers[a["labels"]["alertname"]][0], "same_event": None,
                     "user_impact": {"score": answers[a["labels"]["alertname"]][1],
                                     "level": round(answers[a["labels"]["alertname"]][1]),
                                     "probabilities": [0.25] * 4}} for a, _, _ in jobs]
        store, live = Store(), LiveIncidents()
        for name, service in (("HighCPU", "batch-reports"), ("HighErrorRate", "checkout")):
            receive(store, {"groupKey": service, "groupLabels": {"service": service},
                            "alerts": [alert(name, 1, service=service)]}, scorer, live)
        self.assertEqual(names(as_json(store)), ["HighErrorRate", "HighCPU"])
        page = as_html(store, live)
        self.assertLess(page.index('<span class="chip">service=checkout</span>'),
                        page.index('<span class="chip">service=batch-reports</span>'))

    def test_card_is_headed_by_its_most_important_alert(self):
        """The first alert to fire is not the headline when a later one in the group matters more."""
        answers = {"HighCPU": (0.6, 1.0), "HighErrorRate": (0.9, 3.0)}

        async def scorer(jobs):
            return [{"actionable": answers[a["labels"]["alertname"]][0], "same_event": None,
                     "user_impact": {"score": answers[a["labels"]["alertname"]][1],
                                     "level": round(answers[a["labels"]["alertname"]][1]),
                                     "probabilities": [0.25] * 4}} for a, _, _ in jobs]
        store, live = Store(), LiveIncidents()
        receive(store, {"groupKey": "p", "groupLabels": {"service": "payments"},
                        "alerts": [alert("HighCPU", 1), alert("HighErrorRate", 2)]}, scorer, live)
        page = as_html(store, live)
        self.assertLess(page.index("<h3>HighErrorRate</h3>"), page.index('<div class="k">Also in this group · 1 more</div>'))
        self.assertLess(page.index('<div class="k">Also in this group · 1 more</div>'), page.index("<b>HighCPU</b>"))

    def test_every_section_is_drawn_even_when_empty(self):
        page = as_html(Store(), LiveIncidents())
        for section in ("attention", "noise", "resolved", "all"):
            self.assertIn(f'id="{section}"', page)
        self.assertIn("No firing incidents rated as noise.", page)

    def test_alert_without_impact_is_sorted_by_urgency_alone(self):
        self.assertEqual(priority({"actionable": 0.8}), 0.8)
        self.assertAlmostEqual(priority({"actionable": 0.8, "user_impact": {"score": 1.5, "level": 2}}), 0.4)
        self.assertIsNone(priority(None))

    def test_alert_scored_without_user_impact_still_lists(self):
        store = Store()
        receive(store, {"alerts": [alert("HighErrorRate", 1)]}, Stub())
        self.assertIsNone(as_json(store)[0]["user_impact"])
        self.assertIn("HighErrorRate", as_html(store))


class JudgeTest(unittest.TestCase):
    """triage.judge with a fake Jev client: the user-impact Score is asked and read back."""

    def test_impact_score_is_asked_in_the_same_request_and_parsed(self):
        asked = []

        class Answer(SimpleNamespace):
            pass

        class Client:
            async def system_one(self, state, questions):
                asked.append(sorted(questions))
                return SimpleNamespace(answers={
                    "actionable": Answer(type="noul", noul=0.8),
                    "user_impact": Answer(type="score", score=0.3, probabilities={0: 0.75, 1: 0.2, 2: 0.05, 3: 0.0})},
                    usage=SimpleNamespace(input_tokens=812, output_tokens=41), request_id="req-1")

        got = asyncio.run(triage.judge(Client(), alert("HighCPU", 1, service="batch-reports"), [], [],
                                       triage.CONDITIONS[-1], asyncio.Semaphore(1),
                                       same_event=False, states=True, impact=True))
        self.assertEqual(asked, [["actionable", "user_impact"]])
        self.assertEqual(got["actionable"], 0.8)
        self.assertEqual((got["user_impact"]["level"], got["user_impact"]["score"]), (0, 0.3))
        jev = got["jev"]
        self.assertEqual((jev["input_tokens"], jev["output_tokens"], jev["attempts"], jev["request_id"]),
                         (812, 41, 1, "req-1"))
        self.assertAlmostEqual(jev["cost_usd"], 812 * 0.042 / 1_000_000)
        self.assertGreaterEqual(jev["seconds"], 0)

    def test_cost_is_shown_on_the_page_and_in_json(self):
        async def scorer(jobs):
            return [{"actionable": 0.9, "same_event": None, "user_impact": None,
                     "jev": {"seconds": 1.42, "attempts": 1, "input_tokens": 1_000_000, "output_tokens": 41,
                             "cost_usd": 0.042, "request_id": "req-1"}} for _ in jobs]
        store, live = Store(), LiveIncidents()
        receive(store, {"groupKey": "p", "alerts": [alert("HighErrorRate", 1)]}, scorer, live)
        self.assertEqual(as_json(store)[0]["jev"]["input_tokens"], 1_000_000)
        page = as_html(store, live)
        self.assertIn("Jev 1.4 s · 1,000,000 in / 41 out tokens · $0.042000", page)
        for stat in ("<b>1</b>", "<b>1.4 s</b>", "<b>1,000,000</b>", "<b>41</b>", "<b>$0.0420</b>"):
            self.assertIn(stat, page)

    def test_totals_survive_pruning_and_a_repeat_is_not_counted(self):
        async def scorer(jobs):
            return [{"actionable": 0.9, "same_event": None, "user_impact": None,
                     "jev": {"seconds": 1.0, "attempts": 1, "input_tokens": 500, "output_tokens": 10,
                             "cost_usd": 0.000021, "request_id": None}} for _ in jobs]
        store, old = Store(), alert("HighCPU", 0)
        receive(store, {"alerts": [old]}, scorer)
        receive(store, {"alerts": [dict(old)]}, scorer)
        receive(store, {"alerts": [{**alert("HighErrorRate", 1), "startsAt": "2026-10-07T10:00:00Z"}]}, scorer)
        self.assertEqual(len(store.entries), 1)
        self.assertEqual({k: store.usage_totals()[k] for k in ("calls", "input_tokens", "output_tokens")},
                         {"calls": 2, "input_tokens": 1000, "output_tokens": 20})
        self.assertAlmostEqual(store.usage_totals()["cost_usd"], 0.000042)


class HttpTest(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(Store(), Stub()))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def post(self, path, body):
        req = urllib.request.Request(self.url + path, json.dumps(body).encode())
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())

    def test_webhook_round_trip(self):
        self.post("/deploys", {"labels": {"service": "payments"}, "version": "v9", "at": "2026-10-05T09:58:00Z"})
        got = self.post("/alerts", {"version": "4", "groupKey": "ignored",
                                    "alerts": [alert("HighErrorRate", 0), alert("<script>", 1)]})
        self.assertEqual(got, {"received": 2, "new": 2})
        listed = json.loads(urllib.request.urlopen(self.url + "/alerts.json").read())
        self.assertEqual((listed[0]["actionable"], listed[0]["band"]), (0.95, "look first"))
        self.assertIn("service=payments v9 deployed 2m before", listed[0]["annotations"])
        page = urllib.request.urlopen(self.url + "/").read().decode()
        self.assertIn("look first", page)
        self.assertIn("also firing in its group", page)
        self.assertEqual(page.count("<script>"), 1)  # the page's own reload script, not the alert's name
        self.assertIn("&lt;script&gt;", page)

    def test_bad_payload_is_rejected(self):
        for body in ({"alerts": "nope"}, {"alerts": [{"labels": {}}]}, {"alerts": [{"startsAt": "yesterday"}]}):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.post("/alerts", body)
            self.assertEqual(caught.exception.code, 400)
        for body in ({"service": "payments", "version": "v1"}, {"labels": {}}, {"labels": {"job": 3}}):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.post("/deploys", body)
            self.assertEqual(caught.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
