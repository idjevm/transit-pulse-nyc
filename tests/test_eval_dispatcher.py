"""Tests for the offline dispatcher-decision eval harness (PR: disruption
simulator & forecast).

The harness has no third-party dependency; it is imported directly from scripts/.
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))

import eval_dispatcher_decisions as ev  # noqa: E402

SAMPLE = os.path.join(REPO, "tests", "fixtures", "dispatcher_decisions_sample.jsonl")


@pytest.mark.parametrize("alert_type,headway,expected", [
    ("BUNCHING", 60, "HOLD TRAIN"),
    ("BUNCHING", 89, "HOLD TRAIN"),
    ("BUNCHING", 90, "MONITOR"),
    ("BUNCHING", 149, "MONITOR"),
    ("GAP", 1201, "GAP FILL"),
    ("GAP", 1200, "MONITOR"),
    ("GAP", 950, "MONITOR"),
    ("", 500, "MONITOR"),
    ("bunching", 60, "HOLD TRAIN"),  # case-insensitive
])
def test_expected_action_matches_rules(alert_type, headway, expected):
    assert ev.expected_action(alert_type, headway) == expected


def test_evaluate_all_correct():
    records = [
        {"alert_type": "BUNCHING", "headway_seconds": 60, "action": "HOLD TRAIN"},
        {"alert_type": "GAP", "headway_seconds": 1500, "action": "GAP FILL"},
        {"alert_type": "GAP", "headway_seconds": 1000, "action": "MONITOR"},
    ]
    report = ev.evaluate(records)
    assert report["total"] == 3
    assert report["action_wrong"] == 0
    assert report["action_accuracy"] == 1.0


def test_evaluate_detects_wrong_action():
    records = [
        {"route_id": "N", "stop_id": "R16", "alert_type": "BUNCHING",
         "headway_seconds": 60, "action": "MONITOR"},  # should be HOLD TRAIN
    ]
    report = ev.evaluate(records)
    assert report["action_wrong"] == 1
    m = report["mismatches"][0]
    assert m["expected"] == "HOLD TRAIN"
    assert m["actual"] == "MONITOR"


def test_evaluate_skips_records_without_fields():
    records = [{"action": "MONITOR"}, {"alert_type": "GAP"}]  # missing headway/type
    report = ev.evaluate(records)
    assert report["total"] == 0


def test_llm_agreement_rate():
    records = [
        {"alert_type": "GAP", "headway_seconds": 1000, "action": "MONITOR", "llm_action": "MONITOR"},
        {"alert_type": "GAP", "headway_seconds": 1000, "action": "MONITOR", "llm_action": "GAP FILL"},
    ]
    report = ev.evaluate(records)
    assert report["llm_present"] == 2
    assert report["llm_agreement"] == 1
    assert report["llm_agreement_rate"] == 0.5


def test_bundled_sample_passes_and_scores_llm():
    records = ev.load_jsonl(SAMPLE)
    report = ev.evaluate(records)
    # every deterministic action in the shipped fixture is correct
    assert report["action_wrong"] == 0
    # the fixture deliberately includes one LLM over-reaction (4/5 agree)
    assert report["llm_present"] == 5
    assert report["llm_agreement"] == 4


def test_main_returns_zero_on_bundled_sample(capsys):
    assert ev.main(["prog", SAMPLE]) == 0
    out = capsys.readouterr().out
    assert "PASS" in out


def test_main_returns_one_on_bad_file(tmp_path):
    bad = tmp_path / "bad.jsonl"
    bad.write_text(
        '{"alert_type": "BUNCHING", "headway_seconds": 60, "action": "MONITOR"}\n',
        encoding="utf-8",
    )
    assert ev.main(["prog", str(bad)]) == 1
