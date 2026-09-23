"""Offline eval harness for the in-Flink dispatcher's decisions.

The dispatcher (flink/06) writes two action columns per alert:
  - `action`     : deterministic, computed in SQL from (alert_type, headway_seconds)
  - `llm_action` : parsed from the LLM's free-text "Action:" line

This harness scores a batch of decision records against the ground-truth action
rules (the same thresholds encoded in flink/06's CASE and the agent prompt):

  BUNCHING & headway < 90s   -> HOLD TRAIN
  BUNCHING (90-150s)         -> MONITOR
  GAP      & headway > 1200s -> GAP FILL
  GAP      (900-1200s)       -> MONITOR
  otherwise                  -> MONITOR

The deterministic `action` MUST always match the rule — any mismatch is a real
correctness regression and fails the run (exit 1). The LLM's `llm_action` is
scored for agreement only (informational): the LLM may reasonably differ, and the
deterministic column is what the dashboard and the ops webhook act on.

Input is JSON Lines (one decision object per line). Export a sample with the
Confluent CLI, e.g.:
  confluent kafka topic consume mta_dispatcher_decisions --value-format avro \\
    --from-beginning --print-key=false > decisions.jsonl

Run:
  python scripts/eval_dispatcher_decisions.py decisions.jsonl
  python scripts/eval_dispatcher_decisions.py            # uses the bundled sample
"""

from __future__ import annotations

import json
import os
import sys

BUNCHING_HOLD_BELOW = 90      # BUNCHING under this -> HOLD TRAIN, else MONITOR
GAP_FILL_ABOVE = 1200         # GAP over this -> GAP FILL, else MONITOR

_DEFAULT_SAMPLE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "dispatcher_decisions_sample.jsonl",
)


def expected_action(alert_type: str, headway_seconds: int) -> str:
    """Ground-truth action for an alert, mirroring flink/06's deterministic CASE."""
    at = (alert_type or "").upper().strip()
    if at == "BUNCHING":
        return "HOLD TRAIN" if headway_seconds < BUNCHING_HOLD_BELOW else "MONITOR"
    if at == "GAP":
        return "GAP FILL" if headway_seconds > GAP_FILL_ABOVE else "MONITOR"
    return "MONITOR"


def _norm(action: str) -> str:
    return (action or "").upper().strip()


def evaluate(records: list) -> dict:
    """Score decision records. Returns a report with deterministic-action accuracy
    (must be 100%) and informational LLM agreement."""
    total = 0
    action_correct = 0
    mismatches = []
    llm_present = 0
    llm_agree = 0

    for r in records:
        alert_type = r.get("alert_type")
        headway = r.get("headway_seconds")
        if alert_type is None or headway is None:
            continue
        total += 1
        exp = expected_action(alert_type, int(headway))
        actual = _norm(r.get("action"))
        if actual == exp:
            action_correct += 1
        else:
            mismatches.append({
                "route_id": r.get("route_id"),
                "stop_id": r.get("stop_id"),
                "alert_type": alert_type,
                "headway_seconds": int(headway),
                "expected": exp,
                "actual": actual or "(empty)",
            })
        llm = r.get("llm_action")
        if llm:
            llm_present += 1
            if _norm(llm) == exp:
                llm_agree += 1

    return {
        "total": total,
        "action_correct": action_correct,
        "action_wrong": len(mismatches),
        "action_accuracy": (action_correct / total) if total else 1.0,
        "mismatches": mismatches,
        "llm_present": llm_present,
        "llm_agreement": llm_agree,
        "llm_agreement_rate": (llm_agree / llm_present) if llm_present else None,
    }


def load_jsonl(path: str) -> list:
    records = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except ValueError:
                continue
    return records


def _format_report(report: dict) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("Dispatcher decision eval")
    lines.append("-" * 60)
    lines.append(f"records scored         : {report['total']}")
    lines.append(
        f"deterministic action   : {report['action_correct']}/{report['total']} correct "
        f"({report['action_accuracy'] * 100:.1f}%)"
    )
    if report["llm_present"]:
        rate = report["llm_agreement_rate"] * 100
        lines.append(
            f"llm agreement (info)   : {report['llm_agreement']}/{report['llm_present']} "
            f"({rate:.1f}%)"
        )
    else:
        lines.append("llm agreement (info)   : no llm_action present")
    if report["mismatches"]:
        lines.append("-" * 60)
        lines.append("ACTION MISMATCHES (deterministic column disagrees with the rule):")
        for m in report["mismatches"]:
            lines.append(
                f"  {m['route_id']} @ {m['stop_id']} {m['alert_type']} "
                f"{m['headway_seconds']}s: expected {m['expected']}, got {m['actual']}"
            )
    lines.append("=" * 60)
    return "\n".join(lines)


def main(argv: list) -> int:
    path = argv[1] if len(argv) > 1 else _DEFAULT_SAMPLE
    if not os.path.exists(path):
        print(f"error: no such file: {path}", file=sys.stderr)
        return 2
    records = load_jsonl(path)
    if not records:
        print(f"error: no decision records found in {path}", file=sys.stderr)
        return 2
    report = evaluate(records)
    print(_format_report(report))
    if report["action_wrong"] > 0:
        print("FAIL: deterministic action does not match the rules.", file=sys.stderr)
        return 1
    print("PASS: every deterministic action matches the rules.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
