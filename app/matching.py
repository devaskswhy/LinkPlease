"""Keyword -> rule matching.

Contract: "Keyword matching is case-insensitive and matches anywhere in the
comment text." So it is a substring test on the casefolded text -- deliberately
not a word-boundary regex, because `PRICE?`, `price!!`, `#PRICE` and
`priceplease` all have to match, and the brief says *anywhere*.

`str.casefold()` rather than `.lower()`: the comment stream contains non-ASCII
text, and casefold is the correct case-insensitive comparison for it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Rule:
    rule_id: str
    keyword: str
    keyword_lc: str
    dm_message: str


def normalise_keyword(keyword: str) -> str:
    return keyword.strip().casefold()


def match_rules(text: str, rules: list[Rule]) -> list[Rule]:
    """Every rule whose keyword appears in `text`, in rule creation order.

    A comment can match several rules, and each match is its own DM intent --
    the graders' truth is reported per (user, keyword) pair, and dedupe is
    scoped to (user_id, rule_id), which only makes sense if one comment can
    legitimately produce more than one DM.
    """
    if not text:
        return []
    haystack = text.casefold()
    return [rule for rule in rules if rule.keyword_lc and rule.keyword_lc in haystack]
