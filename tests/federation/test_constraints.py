import pyarrow as pa

from growler.federation.constraints import ConstraintsEvaluator, parse_constraints
from tests.federation.fixtures import eq_value_set, range_value_set


def test_equatable_predicate_matches():
    parsed = parse_constraints(
        {
            "@type": "Constraints",
            "summary": {"region": eq_value_set(["us", "eu"])},
        }
    )
    assert set(parsed.keys()) == {"region"}
    pred = ConstraintsEvaluator(parsed).partition_predicate()
    assert pred({"region": "us"})
    assert pred({"region": "eu"})
    assert not pred({"region": "apac"})


def test_range_predicate_inclusive():
    parsed = parse_constraints(
        {
            "@type": "Constraints",
            "summary": {"id": range_value_set(low=10, low_inclusive=True, high=20, high_inclusive=True)},
        }
    )
    pred = ConstraintsEvaluator(parsed).partition_predicate()
    assert pred({"id": "10"})
    assert pred({"id": "20"})
    assert not pred({"id": "21"})
    assert not pred({"id": "9"})


def test_range_predicate_exclusive_high():
    parsed = parse_constraints(
        {
            "@type": "Constraints",
            "summary": {"id": range_value_set(low=10, low_inclusive=True, high=20, high_inclusive=False)},
        }
    )
    pred = ConstraintsEvaluator(parsed).partition_predicate()
    assert pred({"id": "19"})
    assert not pred({"id": "20"})


def test_filter_table_equatable():
    parsed = parse_constraints(
        {"@type": "Constraints", "summary": {"region": eq_value_set(["us"])}}
    )
    ev = ConstraintsEvaluator(parsed)
    t = pa.table({"region": ["us", "eu", "us"], "id": [1, 2, 3]})
    out = ev.filter_table(t)
    assert out["region"].to_pylist() == ["us", "us"]
    assert out["id"].to_pylist() == [1, 3]


def test_filter_table_range():
    parsed = parse_constraints(
        {"@type": "Constraints", "summary": {"id": range_value_set(low=2, high=3, high_inclusive=True, low_inclusive=True)}}
    )
    ev = ConstraintsEvaluator(parsed)
    t = pa.table({"id": [1, 2, 3, 4]})
    out = ev.filter_table(t)
    assert out["id"].to_pylist() == [2, 3]


def test_empty_constraints_pass_through():
    parsed = parse_constraints({"summary": {}})
    ev = ConstraintsEvaluator(parsed)
    t = pa.table({"id": [1, 2]})
    out = ev.filter_table(t)
    assert out.equals(t)
    pred = ev.partition_predicate()
    assert pred({"anything": "at-all"})
