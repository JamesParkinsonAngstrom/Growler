from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import pyarrow as pa
import pyarrow.compute as pc


@dataclass
class Range:
    low: Any = None
    low_inclusive: bool = True
    high: Any = None
    high_inclusive: bool = True
    unbounded_low: bool = False
    unbounded_high: bool = False

    def contains(self, value: Any) -> bool:
        if not self.unbounded_low and self.low is not None:
            if self.low_inclusive:
                if value < self.low:
                    return False
            else:
                if value <= self.low:
                    return False
        if not self.unbounded_high and self.high is not None:
            if self.high_inclusive:
                if value > self.high:
                    return False
            else:
                if value >= self.high:
                    return False
        return True


@dataclass
class ValueSet:
    kind: str
    ranges: list[Range] | None = None
    values: list[Any] | None = None
    null_allowed: bool = False
    white_list: bool = True
    all_or_none: Optional[bool] = None

    def contains(self, value: Any) -> bool:
        if value is None:
            return self.null_allowed
        if self.kind == "all_or_none":
            return bool(self.all_or_none)
        if self.kind == "equatable":
            inside = value in (self.values or [])
            return inside if self.white_list else not inside
        if self.kind == "sorted_range":
            return any(r.contains(value) for r in (self.ranges or []))
        return True


def parse_constraints(constraints: Optional[dict]) -> dict[str, ValueSet]:
    if not constraints:
        return {}
    summary = constraints.get("summary") or {}
    out: dict[str, ValueSet] = {}
    for column, vset_json in summary.items():
        out[column] = _parse_value_set(vset_json)
    return out


def _parse_value_set(vset: dict) -> ValueSet:
    type_tag = vset.get("@type", "")
    null_allowed = bool(vset.get("nullAllowed", False))
    if "AllOrNoneValueSet" in type_tag:
        return ValueSet(
            kind="all_or_none",
            null_allowed=null_allowed,
            all_or_none=bool(vset.get("all", False)),
        )
    if "EquatableValueSet" in type_tag:
        values = _extract_values(vset.get("valueBlock"))
        white_list = bool(vset.get("whiteList", True))
        return ValueSet(
            kind="equatable",
            values=values,
            white_list=white_list,
            null_allowed=null_allowed,
        )
    if "SortedRangeSet" in type_tag:
        ranges: list[Range] = []
        for r in vset.get("ranges", []) or []:
            ranges.append(_parse_range(r))
        return ValueSet(kind="sorted_range", ranges=ranges, null_allowed=null_allowed)
    return ValueSet(kind="all_or_none", null_allowed=null_allowed, all_or_none=True)


def _parse_range(r: dict) -> Range:
    low_mark = r.get("low") or {}
    high_mark = r.get("high") or {}
    return Range(
        low=low_mark.get("value"),
        low_inclusive=low_mark.get("bound") in (None, "EXACTLY", "INCLUSIVE"),
        high=high_mark.get("value"),
        high_inclusive=high_mark.get("bound") in (None, "EXACTLY", "INCLUSIVE"),
        unbounded_low=low_mark.get("valueSet") == "ABOVE" and low_mark.get("value") is None,
        unbounded_high=high_mark.get("valueSet") == "BELOW" and high_mark.get("value") is None,
    )


def _extract_values(block: Optional[dict]) -> list[Any]:
    if not block:
        return []
    rows = block.get("rows") or []
    if rows:
        return list(rows)
    return []


class ConstraintsEvaluator:
    def __init__(self, parsed: dict[str, ValueSet]):
        self.parsed = parsed

    def partition_predicate(self) -> Callable[[dict[str, str]], bool]:
        def _pred(pvals: dict[str, str]) -> bool:
            for col, vset in self.parsed.items():
                value = pvals.get(col)
                if value is None:
                    if not vset.null_allowed:
                        return False
                    continue
                if not vset.contains(_best_effort_cast(value)):
                    return False
            return True

        return _pred

    def filter_table(self, table: pa.Table) -> pa.Table:
        if not self.parsed:
            return table
        mask = None
        for col, vset in self.parsed.items():
            if col not in table.column_names:
                continue
            col_mask = _column_mask(table[col], vset)
            mask = col_mask if mask is None else pc.and_(mask, col_mask)
        if mask is None:
            return table
        return table.filter(mask)


def _column_mask(column: pa.ChunkedArray, vset: ValueSet):
    if vset.kind == "equatable" and vset.values:
        values_arr = pa.array(vset.values)
        mask = pc.is_in(column, value_set=values_arr)
        if not vset.white_list:
            mask = pc.invert(mask)
        if vset.null_allowed:
            mask = pc.or_(mask, pc.is_null(column))
        return mask
    if vset.kind == "sorted_range" and vset.ranges:
        mask = None
        for r in vset.ranges:
            piece = _range_mask(column, r)
            mask = piece if mask is None else pc.or_(mask, piece)
        if vset.null_allowed:
            mask = pc.or_(mask, pc.is_null(column))
        return mask
    if vset.kind == "all_or_none":
        if vset.all_or_none:
            return pc.is_valid(column) if not vset.null_allowed else pa.array([True] * len(column))
        return pa.array([False] * len(column))
    return pa.array([True] * len(column))


def _range_mask(column, r: Range):
    mask = pa.array([True] * len(column))
    if not r.unbounded_low and r.low is not None:
        lo = pa.scalar(r.low)
        mask = pc.and_(mask, pc.greater_equal(column, lo) if r.low_inclusive else pc.greater(column, lo))
    if not r.unbounded_high and r.high is not None:
        hi = pa.scalar(r.high)
        mask = pc.and_(mask, pc.less_equal(column, hi) if r.high_inclusive else pc.less(column, hi))
    return mask


def _best_effort_cast(value: str) -> Any:
    try:
        if value.lstrip("-").isdigit():
            return int(value)
        return float(value) if "." in value and not value.endswith("-") else value
    except Exception:
        return value
