"""Live incidents: one per Alertmanager group, keyed by the webhook's groupKey.

    python -m unittest tests.test_live_incidents
"""

import json
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer

from alert_triage.live_incidents import LiveIncidents, payload_group
from alert_triage.receiver import Store, make_handler, receive
from tests.test_receiver import Stub, alert


def payload(group_labels: dict, *alerts):
    """A webhook payload as Alertmanager sends it for one group."""
    return {"groupKey": "{}:" + json.dumps(group_labels, sort_keys=True), "groupLabels": group_labels,
            "alerts": list(alerts)}


class PayloadGroupTest(unittest.TestCase):
    def test_uses_alertmanagers_group_key_and_labels_whatever_they_are(self):
        body = payload({"namespace": "boutique"}, alert("KubePodCrashLooping", 1))
        self.assertEqual(payload_group(body), (body["groupKey"], {"namespace": "boutique"}))

    def test_without_a_group_key_falls_back_to_the_labels_all_alerts_share(self):
        key, labels = payload_group({"alerts": [alert("HighErrorRate", 1), alert("HighLatencyP99", 2)]})
        self.assertEqual(labels, {"service": "payments", "env": "prod"})


class LiveIncidentsTest(unittest.TestCase):
    def test_same_group_key_joins_and_another_key_opens_an_incident(self):
        live = LiveIncidents()
        for body in (payload({"job": "payments"}, alert("HighErrorRate", 1)),
                     payload({"job": "payments"}, alert("HighLatencyP99", 2)),
                     payload({"job": "cart"}, alert("HighCPU", 3, service="cart"))):
            live.receive(body["alerts"], payload_group(body))
        view = live.view()
        self.assertEqual(len(view), 2)
        payments = next(i for i in view if i["group"] == "job=payments")
        self.assertEqual(payments["alertnames"], ["HighErrorRate", "HighLatencyP99"])

    def test_a_quiet_group_closes_and_the_next_alert_opens_a_new_one(self):
        live = LiveIncidents()
        for body in (payload({"service": "payments"}, alert("HighErrorRate", 1)),
                     payload({"service": "payments"}, alert("HighErrorRate", 30, fingerprint="again"))):
            live.receive(body["alerts"], payload_group(body))
        self.assertEqual(len(live.view()), 2)

    def test_receive_passes_the_payloads_group_through(self):
        live, store = LiveIncidents(), Store()
        receive(store, payload({"namespace": "boutique"}, alert("KubePodCrashLooping", 1)), Stub(), live)
        self.assertEqual(live.view()[0]["group"], "namespace=boutique")

    def test_incidents_endpoint(self):
        live = LiveIncidents()
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(Store(), Stub(), live))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            body = json.dumps(payload({"service": "cart", "env": "prod"}, alert("RedisDown", 1, service="cart"))).encode()
            urllib.request.urlopen(urllib.request.Request(f"{base}/alerts", body, {"Content-Type": "application/json"}))
            incidents = json.loads(urllib.request.urlopen(f"{base}/incidents.json").read())
            self.assertEqual((incidents[0]["group"], incidents[0]["alertnames"]), ("env=prod, service=cart", ["RedisDown"]))
            self.assertIn("Probably noise", urllib.request.urlopen(base).read().decode())
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
