"""Structural assertions on the Flink SQL DAG (PR: flink-correctness-and-alerts).

These don't run Flink — they lock in the correctness properties the SQL is
supposed to have, so an accidental edit (dropping a watermark, flipping the
dispatcher back to append, breaking the deterministic action) fails a fast test
instead of only showing up as duplicate LLM calls or non-deterministic decisions
on live infrastructure.

Also guards a provisioner landmine: deploy/provision.sh strips full-line `--`
comments, flattens newlines to spaces, then splits on `;`. A *trailing* inline
`--` comment would therefore swallow the rest of a statement. We assert the
auto-submitted files never use one.
"""

import os
import re

FLINK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "flink")


def _read(name: str) -> str:
    with open(os.path.join(FLINK_DIR, name), encoding="utf-8") as fh:
        return fh.read()


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text)


# ---- 04: headway alerts carry an event-time watermark ----------------------

def test_headway_alerts_has_watermark_on_arrival_time():
    sql = _collapse_ws(_read("04_headway_alerts.sql"))
    # The rowtime property is what makes job 06's first-row dedup append-only.
    assert re.search(
        r"WATERMARK\s+FOR\s+`?arrival_time`?\s+AS\s+`?arrival_time`?\s*-\s*INTERVAL\s+'5'\s+MINUTE",
        sql,
    ), "mta_headway_alerts must declare a watermark on arrival_time"


# ---- 06: effectively-once dispatcher ---------------------------------------

def test_dispatcher_decisions_is_upsert_with_alert_identity_pk():
    sql = _collapse_ws(_read("06_dispatcher_agent.sql"))
    assert "'changelog.mode' = 'upsert'" in sql, "decisions table must be upsert, not append"
    assert re.search(
        r"PRIMARY\s+KEY\s*\(\s*`route_id`\s*,\s*`direction`\s*,\s*`stop_id`\s*,\s*`curr_trip`\s*,\s*`prev_trip`\s*\)\s*NOT\s+ENFORCED",
        sql,
    ), "PK must be the alert identity so reprocessing overwrites, not duplicates"


def test_dispatcher_dedups_alerts_first_row_by_rowtime():
    sql = _collapse_ws(_read("06_dispatcher_agent.sql"))
    # First-row dedup (ORDER BY the arrival_time rowtime ASC, keep rn = 1) is the
    # append-only pattern that fires the agent exactly once per distinct alert.
    assert re.search(
        r"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION\s+BY\s+route_id\s*,\s*direction\s*,\s*stop_id\s*,\s*curr_trip\s*,\s*prev_trip\s+ORDER\s+BY\s+arrival_time\s+ASC\s*\)",
        sql,
    ), "must first-row dedup alerts on the alert identity, ordered by arrival_time ASC"
    assert re.search(r"WHERE\s+rn\s*=\s*1", sql), "dedup must keep rn = 1"


def test_dispatcher_action_is_deterministic_and_matches_prompt_rules():
    sql = _collapse_ws(_read("06_dispatcher_agent.sql"))
    # The control action is computed in SQL, not parsed from the model. Thresholds
    # must match the prompt's ACTION RULES exactly.
    assert re.search(
        r"WHEN\s+a\.alert_type\s*=\s*'BUNCHING'\s+AND\s+a\.headway_seconds\s*<\s*90\s+THEN\s+'HOLD TRAIN'",
        sql,
    ), "BUNCHING under 90s must deterministically map to HOLD TRAIN"
    assert re.search(
        r"WHEN\s+a\.alert_type\s*=\s*'GAP'\s+AND\s+a\.headway_seconds\s*>\s*1200\s+THEN\s+'GAP FILL'",
        sql,
    ), "GAP over 1200s must deterministically map to GAP FILL"
    assert re.search(r"END\s+AS\s+action", sql), "deterministic CASE must land in the action column"


def test_dispatcher_keeps_llm_action_for_audit():
    sql = _collapse_ws(_read("06_dispatcher_agent.sql"))
    # The model's own suggestion is retained alongside the deterministic action.
    assert re.search(r"Action:.*?AS\s+llm_action", sql), "parsed model action must be kept as llm_action"
    assert "AI_RUN_AGENT(" in sql, "the in-Flink agent call must still run"
    assert re.search(r"AS\s+raw_response", sql), "raw model response must be retained"


# ---- 09: the optional active-alerts view uses real columns -----------------

def test_service_alerts_active_view_uses_real_columns():
    sql = _read("09_service_alerts.sql")
    # The mta_service_alerts table declares these columns...
    for real_col in ("`event_id`", "`affected`", "`status_label`", "`date`"):
        assert real_col in sql
    # ...and the (commented) mta_active_alerts example must reference them, never
    # the phantom columns the earlier draft used.
    view_region = sql[sql.index("mta_active_alerts"):]
    for phantom in ("route_id", "`status`", "updated_at"):
        assert phantom not in view_region, f"active-alerts view must not reference phantom column {phantom}"


# ---- provisioner landmine: no trailing inline comments in submitted files --

AUTO_SUBMITTED = [
    "01_create_tables.sql",
    "02_arrival_estimates.sql",
    "03_headway.sql",
    "04_headway_alerts.sql",
    "06_dispatcher_agent.sql",
    "07_bus_positions.sql",
    "08_headway_forecast.sql",
]


def test_auto_submitted_files_have_no_trailing_inline_comments():
    # provision.sh removes full-line `--` comments, flattens newlines to spaces,
    # then splits on `;`. A trailing inline `--` on a code line would comment out
    # everything after it once flattened. Full-line comments are safe (stripped).
    for name in AUTO_SUBMITTED:
        for lineno, raw in enumerate(_read(name).splitlines(), start=1):
            stripped = raw.strip()
            if stripped.startswith("--") or not stripped:
                continue
            assert "--" not in raw, f"{name}:{lineno} has a trailing inline `--` comment"
