"""questions.json and experiment_questions.json: the shipped files load, the questions are built from them,
bad files are refused.

    python -m unittest tests.test_questions
"""

import copy
import json
import unittest

from alert_triage import jev_questions, triage
from alert_triage.jev_questions import EXPERIMENTS, Q, check, check_experiments


class QuestionsFileTest(unittest.TestCase):
    def test_live_file_holds_only_what_the_receiver_asks(self):
        self.assertEqual({k for k in Q if not k.startswith("_")}, {"urgency", "user_impact"})

    def test_the_question_is_built_from_the_file(self):
        qs = triage.build_questions(triage.CONDITIONS[-1], has_recent=False, states=True)
        rules = Q["urgency"]["context_rules"]
        self.assertEqual(qs["actionable"].instructions,
                         " ".join([Q["urgency"]["question"], rules["recent_alerts"], rules["recent_deploys"],
                                   rules["alert_states"]]))
        self.assertEqual(triage.IMPACT_LEVELS, tuple(level["name"] for level in Q["user_impact"]["levels"]))

    def test_a_missing_part_is_refused_with_its_name(self):
        broken = copy.deepcopy(Q)
        del broken["urgency"]["context_rules"]["alert_states"]
        with self.assertRaises(SystemExit) as caught:
            check(broken)
        self.assertIn("urgency.context_rules.alert_states", str(caught.exception))

    def test_impact_needs_at_least_two_levels(self):
        broken = copy.deepcopy(Q)
        broken["user_impact"]["levels"] = broken["user_impact"]["levels"][:1]
        with self.assertRaises(SystemExit):
            check(broken)

    def test_experiment_questions_need_their_placeholder(self):
        broken = copy.deepcopy(EXPERIMENTS)
        broken["same_cause_as_group"]["question"] = "Same cause?"
        with self.assertRaises(SystemExit) as caught:
            check_experiments(broken)
        self.assertIn("{n}", str(caught.exception))

    def test_shipped_files_are_valid_json_with_notes(self):
        for path in (jev_questions.PATH, jev_questions.EXPERIMENTS_PATH):
            self.assertIn("_about", json.loads(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
