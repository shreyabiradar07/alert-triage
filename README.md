# alert-triage

**Rank alerts with a System 1 model like Jev. Never suppress one.**

When an incident hits, alerts arrive in a flood, and they all look equally important. Which one do you
read first?

alert-triage is a webhook that sits beside Alertmanager and asks [Jev](https://docs.typesafe.ai) two things
about each new alert: how urgent is it, and how many users does it affect? You get a page with the alerts that matter at the
top and the likely noise at the bottom. Your pager keeps working exactly as it does today.

## Layout

| Folder | What it is |
| --- | --- |
| `alert_triage/` | The tool: the receiver and page, the Jev calls, and the questions it asks |
| `chaos/` | Demo 1: a shop to break, with a real Prometheus and Alertmanager |
| `benchmark/` | Demo 2: ITBench-Lite, replayed and scored |
| `experiments/` | Simulated alerts and the grouping experiment; the tool does not depend on their results |
| `tests/` | Unit tests |

Run the commands below from the project root.

## Setup

Needs Python 3.10 or later. Put your key in `.env` (see `.env.example`). Without one, alerts are listed
unscored. Every new alert is one paid Jev call; nothing is cached.

Linux or macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows (PowerShell):

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Activate the environment in each new terminal. `python` then means the same thing on every system.

## Demo 1: break a live shop

Real faults fire real alerts through Prometheus and Alertmanager. Needs Docker with Compose and about
2-3 GB of free memory. These steps are the same on Linux and macOS. On Windows, run them in a WSL
terminal, starting from the project folder (`cd "/mnt/c/path/to/alert-triage"`).

1. Start the stack:

   ```bash
   docker compose -f chaos/docker-compose.yml up -d --build
   ```

2. Open http://localhost:9099.

3. Break something. Alerts appear 1-3 minutes after a fault starts.

   ```bash
   python3 chaos/chaos.py storm --quick      # 6 real faults at once, with noise: ~5 min
   ```

   Or run one fault with `python3 chaos/chaos.py run redis_outage`. `python3 chaos/chaos.py list` shows them all.

4. Optional: score the ranking against what was injected:

   ```bash
   python3 chaos/score_live.py
   ```

5. Clean up:

   ```bash
   python3 chaos/chaos.py heal
   docker compose -f chaos/docker-compose.yml down
   ```

   On Windows, this PowerShell command then gives WSL's memory back:

   ```powershell
   wsl --shutdown
   ```

Prometheus is on `:9090` and Alertmanager on `:9093`.

## Demo 2: replay a real incident

Plays one of IBM Research's recorded SRE incidents ([ITBench-Lite](https://huggingface.co/datasets/ibm-research/ITBench-Lite),
Apache-2.0) onto the page. No Docker needed, and much lighter than Demo 1. With the environment from Setup
active, these commands are the same on Linux, macOS and Windows.

1. Download the incidents once (~13 MB):

   ```bash
   python -m benchmark.itbench fetch
   ```

2. Start the receiver and leave it running:

   ```bash
   python -m alert_triage.receiver
   ```

3. Open http://localhost:9099.

4. In a second terminal, play an incident. It takes about a minute and makes about 26 Jev calls.

   ```bash
   python -m benchmark.itbench demo Scenario-81
   ```

   The terminal ends with the top five alerts checked against IBM's answer key.

5. Stop the receiver with Ctrl+C. Restart it before the next scenario, or they mix on the page.

### Run the full benchmark

Scores every scenario, without using the page. This makes about 1,500 Jev calls.

```bash
python -m benchmark.itbench run --group-by namespace
python -m benchmark.itbench score
```

`run` overwrites the previous results for the same `--group-by`.

## Try it on your own alerts

Rank a saved Alertmanager webhook payload in the terminal, with no receiver or Docker. Each alert is one
Jev call. This ranks by urgency alone; the page also uses user impact.

```bash
python -m alert_triage.triage rank webhook.json
```

## Use it with your Alertmanager

```bash
python -m alert_triage.receiver --host 0.0.0.0     # default host is 127.0.0.1, port 9099
```

```yaml
route:
  routes:
    - receiver: alert-triage     # first route
      continue: true             # the alert still reaches your pager
receivers:
  - name: alert-triage
    webhook_configs:
      - url: http://<receiver-host>:9099/alerts
```

| Endpoint | |
| --- | --- |
| `POST /alerts` | Alertmanager webhook |
| `POST /deploys` | `{"labels": {"job": "payments"}, "version", "summary", "at"}`, using your alert label names |
| `GET /` | the page, refreshed every 15 seconds |
| `GET /alerts.json` | ranked alerts, with each Jev call's time, tokens and cost |
| `GET /incidents.json` | incidents: one per Alertmanager group |

- Sections: needs attention (a firing alert at least 50% urgent, or unscored), probably noise (all firing
  alerts under 50%), resolved.
- Nothing is muted, and no label names are assumed.
- There is no authentication, and alerts are held in memory for 24 hours. Keep it on an internal network.
- Cost is an estimate: input tokens × $0.042 per million for `jev-1.13.0`. Output is free.

## Customise the questions

Edit `alert_triage/questions.json` and restart the receiver. Keep the backticked field names such as `alert`. To keep
several versions, point `ALERT_TRIAGE_QUESTIONS` at another file.

## Offline experiments

Used to test ideas during development. The demos and the receiver don't need them.

They run on 300 simulated alerts with known labels, and test whether context helps. Each alert is scored
with only its own details, then with recent alerts, then with recent deploys too. The report compares ranking
and calibration (is a 0.9 right nine times in ten?) against the `severity` label and hand-written rules.

```bash
python -m experiments.simulate          # create the alerts; run this first
python -m alert_triage.triage score     # score them: 900 Jev calls
python -m experiments.evaluate          # write report.html
python -m experiments.incidents         # grouping experiment
python -m experiments.replay            # or post the alerts to a running receiver
```

## Tests

```bash
python -m unittest discover -s tests -t .
```