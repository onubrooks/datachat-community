"""Unit tests for query pattern matching behavior."""

from backend.utils.pattern_matcher import QueryPatternMatcher, QueryPatternType


def test_matcher_does_not_treat_top_n_aggregation_as_sample_rows():
    matcher = QueryPatternMatcher()
    patterns = matcher.match(
        "Show total deposits, withdrawals, and net flow by segment for the last 8 weeks, "
        "then identify the top 2 segments driving week-over-week net flow decline."
    )
    types = {pattern.pattern_type for pattern in patterns}
    assert QueryPatternType.SAMPLE_ROWS not in types
    assert QueryPatternType.AGGREGATION in types


def test_matcher_detects_sample_rows_for_top_n_rows_phrase():
    matcher = QueryPatternMatcher()
    patterns = matcher.match("Show top 5 rows from public.transactions")
    types = {pattern.pattern_type for pattern in patterns}
    assert QueryPatternType.SAMPLE_ROWS in types
