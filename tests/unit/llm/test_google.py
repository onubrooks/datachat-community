"""Tests for GoogleProvider helper behavior."""

from types import SimpleNamespace

from backend.llm.google import GoogleProvider


class _FakeCandidate:
    def __init__(self, finish_reason):
        self.finish_reason = finish_reason


class _FakeResponse:
    def __init__(self, text="", finish_reason=None):
        self.text = text
        self.candidates = [_FakeCandidate(finish_reason)]


def test_extract_finish_reason_maps_length_like_values():
    provider = GoogleProvider(api_key="dummy", model="gemini-2.5-flash")
    response = _FakeResponse(text="{}", finish_reason="MAX_TOKENS")

    assert provider._extract_finish_reason(response) == "length"


def test_extract_finish_reason_maps_content_filter_values():
    provider = GoogleProvider(api_key="dummy", model="gemini-2.5-flash")
    response = _FakeResponse(text="", finish_reason="SAFETY")

    assert provider._extract_finish_reason(response) == "content_filter"


def test_extract_finish_reason_defaults_to_stop_for_unknown():
    provider = GoogleProvider(api_key="dummy", model="gemini-2.5-flash")
    response = _FakeResponse(text="ok", finish_reason="FINISH_REASON_UNSPECIFIED")

    assert provider._extract_finish_reason(response) == "stop"


def test_extract_response_text_handles_non_string_payload():
    provider = GoogleProvider(api_key="dummy", model="gemini-2.5-flash")
    response = SimpleNamespace(text=None, candidates=[])

    assert provider._extract_response_text(response) == ""
