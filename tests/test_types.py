import pyarrow as pa
import pytest

from growler.errors import InvalidType
from growler.types import is_valid_type, to_arrow, type_matches, validate_type


def test_primitives_valid():
    for t in ["bool", "int32", "int64", "float32", "float64", "string", "binary", "date", "timestamp_us", "timestamp_ns"]:
        assert is_valid_type(t)


def test_decimal_valid():
    assert is_valid_type("decimal(10,2)")
    assert is_valid_type("decimal(38, 0)")


def test_list_valid():
    assert is_valid_type("list<int64>")
    assert is_valid_type("list<string>")
    assert is_valid_type("list<struct>")


def test_invalid():
    assert not is_valid_type("uint64")
    assert not is_valid_type("timestamp")
    with pytest.raises(InvalidType):
        validate_type("bogus")


def test_to_arrow_primitives():
    assert to_arrow("int64") == pa.int64()
    assert to_arrow("string") == pa.string()
    assert to_arrow("timestamp_us") == pa.timestamp("us", tz="UTC")


def test_to_arrow_decimal():
    assert to_arrow("decimal(10,2)") == pa.decimal128(10, 2)


def test_to_arrow_list():
    assert to_arrow("list<int64>") == pa.list_(pa.int64())


def test_type_matches_primitives():
    assert type_matches(pa.int64(), "int64")
    assert not type_matches(pa.int32(), "int64")
    assert type_matches(pa.string(), "string")
    assert type_matches(pa.large_string(), "string")


def test_type_matches_timestamp_unit():
    assert type_matches(pa.timestamp("us", tz="UTC"), "timestamp_us")
    assert not type_matches(pa.timestamp("ns", tz="UTC"), "timestamp_us")


def test_type_matches_decimal():
    assert type_matches(pa.decimal128(10, 2), "decimal(10,2)")
    assert not type_matches(pa.decimal128(10, 3), "decimal(10,2)")


def test_type_matches_list():
    assert type_matches(pa.list_(pa.int64()), "list<int64>")
    assert type_matches(pa.list_(pa.struct([pa.field("x", pa.int64())])), "list<struct>")
