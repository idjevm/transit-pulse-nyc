"""Tests for the interactive-agent backend (PR: dashboard-copilot-ux).

Covers the truthful model label, the reasoning-model truncation handling that was
leaving the copilot panes blank, and that every agent response reports which model
answered. No LLM SDK is required: the backend is resolved through _resolve_backend,
which the tests monkeypatch with fakes.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import agents  # noqa: E402


# ---- fakes -----------------------------------------------------------------

class _FinishReason:
    def __init__(self, name):
        self.name = name


class _Part:
    def __init__(self, text):
        self.text = text


class _Content:
    def __init__(self, parts):
        self.parts = parts


class _Candidate:
    def __init__(self, parts, finish):
        self.content = _Content(parts)
        self.finish_reason = _FinishReason(finish)


class _GeminiResp:
    def __init__(self, text=None, parts=None, finish="STOP"):
        self.text = text
        self.candidates = [_Candidate(parts or [], finish)]


class _GeminiClient:
    def __init__(self, resp):
        self.models = self
        self._resp = resp

    def generate_content(self, model, contents, config):
        return self._resp


class _ClaudeBlock:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _ClaudeMsg:
    def __init__(self, blocks):
        self.content = blocks


class _ClaudeClient:
    def __init__(self, msg):
        self.messages = self
        self._msg = msg

    def create(self, model, max_tokens, system, messages):
        return self._msg


# ---- model label -----------------------------------------------------------

def test_pretty_model_labels():
    assert agents._pretty_model("google", "gemini-2.5-pro") == "Gemini 2.5 Pro"
    assert agents._pretty_model("google", "models/gemini-2.0-flash") == "Gemini 2.0 Flash"
    assert agents._pretty_model("anthropic", "claude-haiku-4-5-20251001") == "Claude Haiku 4.5"
    assert agents._pretty_model("", "") == "not configured"


def test_active_model_not_configured(monkeypatch):
    for var in ("GEMINI_API_KEY", "GOOGLEAI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    info = agents.active_model()
    assert info["provider"] is None
    assert info["label"] == "not configured"


# ---- truncation / extraction helpers ---------------------------------------

def test_finish_reason_reads_candidate():
    assert agents._finish_reason(_GeminiResp(finish="MAX_TOKENS")) == "MAX_TOKENS"
    assert agents._finish_reason(object()) == ""


def test_extract_gemini_text_falls_back_to_parts():
    assert agents._extract_gemini_text(_GeminiResp(text="hi")) == "hi"
    assert agents._extract_gemini_text(_GeminiResp(parts=[_Part("a"), _Part("b")])) == "ab"
    assert agents._extract_gemini_text(_GeminiResp(parts=[])) == ""


# ---- _ask across backends --------------------------------------------------

def test_ask_gemini_truncation_returns_clear_error(monkeypatch):
    resp = _GeminiResp(text=None, parts=[], finish="MAX_TOKENS")
    monkeypatch.setattr(agents, "_resolve_backend",
                        lambda: ("google", _GeminiClient(resp), "gemini-2.5-pro"))
    monkeypatch.setattr(agents, "_gemini_config", lambda system, max_tokens: None)
    text, err, model = agents._ask("sys", "user")
    assert text is None
    assert "reasoning" in err.lower() or "budget" in err.lower()
    assert model == "Gemini 2.5 Pro"


def test_ask_gemini_success(monkeypatch):
    resp = _GeminiResp(text="Take the 4 train.", finish="STOP")
    monkeypatch.setattr(agents, "_resolve_backend",
                        lambda: ("google", _GeminiClient(resp), "gemini-2.5-pro"))
    monkeypatch.setattr(agents, "_gemini_config", lambda system, max_tokens: None)
    text, err, model = agents._ask("sys", "user")
    assert err is None
    assert text == "Take the 4 train."
    assert model == "Gemini 2.5 Pro"


def test_ask_claude_success(monkeypatch):
    msg = _ClaudeMsg([_ClaudeBlock("Hold the trailing train.")])
    monkeypatch.setattr(agents, "_resolve_backend",
                        lambda: ("anthropic", _ClaudeClient(msg), "claude-haiku-4-5-20251001"))
    text, err, model = agents._ask("sys", "user")
    assert err is None
    assert text == "Hold the trailing train."
    assert model == "Claude Haiku 4.5"


def test_ask_none_configured(monkeypatch):
    monkeypatch.setattr(agents, "_resolve_backend", lambda: (None, None, None))
    text, err, model = agents._ask("sys", "user")
    assert text is None
    assert "not configured" in err.lower()
    assert model is None


# ---- every agent response reports the model --------------------------------

def test_rider_advisor_reports_model(monkeypatch):
    monkeypatch.setattr(agents, "_ask", lambda *a, **k: ("advice", None, "Gemini 2.5 Pro"))
    result = agents.rider_advisor({}, "Union Sq", "Astoria")
    assert result == {"ok": True, "answer": "advice", "model": "Gemini 2.5 Pro"}


def test_operator_insight_reports_model_on_error(monkeypatch):
    monkeypatch.setattr(agents, "_ask", lambda *a, **k: (None, "boom", "Claude Haiku 4.5"))
    result = agents.operator_insight({})
    assert result["ok"] is False
    assert result["error"] == "boom"
    assert result["model"] == "Claude Haiku 4.5"
