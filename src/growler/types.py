from __future__ import annotations

import re

import pyarrow as pa

from growler.errors import InvalidType


_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
_LIST_RE = re.compile(r"^list<(.+)>$")

_PRIMITIVES = {
    "bool",
    "int32",
    "int64",
    "float32",
    "float64",
    "string",
    "binary",
    "date",
    "timestamp_us",
    "timestamp_ns",
}


def is_valid_type(type_str: str) -> bool:
    if type_str in _PRIMITIVES:
        return True
    if _DECIMAL_RE.match(type_str):
        return True
    m = _LIST_RE.match(type_str)
    if m:
        inner = m.group(1)
        if inner == "struct":
            return True
        return is_valid_type(inner)
    if type_str == "struct":
        return True
    return False


def validate_type(type_str: str) -> str:
    if not is_valid_type(type_str):
        raise InvalidType(f"Unsupported type: {type_str!r}")
    return type_str


def to_arrow(type_str: str) -> pa.DataType:
    if type_str == "bool":
        return pa.bool_()
    if type_str == "int32":
        return pa.int32()
    if type_str == "int64":
        return pa.int64()
    if type_str == "float32":
        return pa.float32()
    if type_str == "float64":
        return pa.float64()
    if type_str == "string":
        return pa.string()
    if type_str == "binary":
        return pa.binary()
    if type_str == "date":
        return pa.date32()
    if type_str == "timestamp_us":
        return pa.timestamp("us", tz="UTC")
    if type_str == "timestamp_ns":
        return pa.timestamp("ns", tz="UTC")
    m = _DECIMAL_RE.match(type_str)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        return pa.decimal128(p, s)
    m = _LIST_RE.match(type_str)
    if m:
        inner = m.group(1)
        if inner == "struct":
            return pa.list_(pa.struct([]))
        return pa.list_(to_arrow(inner))
    if type_str == "struct":
        return pa.struct([])
    raise InvalidType(f"Cannot map to Arrow: {type_str!r}")


def type_matches(pa_type: pa.DataType, type_str: str) -> bool:
    if type_str == "bool":
        return pa.types.is_boolean(pa_type)
    if type_str == "int32":
        return pa.types.is_int32(pa_type)
    if type_str == "int64":
        return pa.types.is_int64(pa_type)
    if type_str == "float32":
        return pa.types.is_float32(pa_type)
    if type_str == "float64":
        return pa.types.is_float64(pa_type)
    if type_str == "string":
        return pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type)
    if type_str == "binary":
        return pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type)
    if type_str == "date":
        return pa.types.is_date(pa_type)
    if type_str == "timestamp_us":
        return pa.types.is_timestamp(pa_type) and pa_type.unit == "us"
    if type_str == "timestamp_ns":
        return pa.types.is_timestamp(pa_type) and pa_type.unit == "ns"
    m = _DECIMAL_RE.match(type_str)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        return pa.types.is_decimal(pa_type) and pa_type.precision == p and pa_type.scale == s
    m = _LIST_RE.match(type_str)
    if m:
        if not pa.types.is_list(pa_type) and not pa.types.is_large_list(pa_type):
            return False
        inner = m.group(1)
        if inner == "struct":
            return pa.types.is_struct(pa_type.value_type)
        return type_matches(pa_type.value_type, inner)
    if type_str == "struct":
        return pa.types.is_struct(pa_type)
    return False


def reject_int96_timestamps(arrow_schema: pa.Schema) -> None:
    pass
