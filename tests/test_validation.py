from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from growler import Column, Schema
from growler.errors import SchemaMismatch
from growler.validation import (
    extract_leaf_paths,
    extract_stats,
    validate_file_schema,
)


def test_extract_leaf_paths_flat():
    s = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    paths = extract_leaf_paths(s)
    assert set(paths.keys()) == {"a", "b"}


def test_extract_leaf_paths_nested_struct():
    s = pa.schema(
        [
            pa.field(
                "user",
                pa.struct(
                    [pa.field("name", pa.string()), pa.field("age", pa.int64())]
                ),
            )
        ]
    )
    paths = extract_leaf_paths(s)
    assert "user" in paths
    assert "user.name" in paths
    assert "user.age" in paths


def test_extract_leaf_paths_list_of_struct():
    s = pa.schema(
        [
            pa.field(
                "addrs",
                pa.list_(pa.struct([pa.field("city", pa.string())])),
            )
        ]
    )
    paths = extract_leaf_paths(s)
    assert "addrs" in paths
    assert "addrs.city" in paths


def _schema() -> Schema:
    return Schema(
        next_column_id=3,
        columns=[
            Column(id=1, path="a", type="int64", nullable=False),
            Column(id=2, path="b", type="string"),
        ],
    )


def test_validate_passes_when_superset():
    s = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("extra", pa.int64()),
        ]
    )
    mapping = validate_file_schema(s, _schema())
    assert mapping == {1: "a", 2: "b"}


def test_validate_fails_on_missing():
    s = pa.schema([pa.field("a", pa.int64())])
    with pytest.raises(SchemaMismatch) as ei:
        validate_file_schema(s, _schema())
    assert ei.value.details["missing"] == "b"


def test_validate_fails_on_type_mismatch():
    s = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())])
    with pytest.raises(SchemaMismatch) as ei:
        validate_file_schema(s, _schema())
    assert ei.value.details["expected"] == "int64"


def test_extract_stats_primitive(tmp_path: Path):
    t = pa.table({"a": pa.array([1, 2, 3, None], type=pa.int64()), "b": pa.array(["x", "y", "z", None])})
    p = tmp_path / "f.parquet"
    pq.write_table(t, p)
    pf = pq.ParquetFile(p)
    stats = extract_stats(pf, {1: "a", 2: "b"})
    assert stats[1]["min"] == 1
    assert stats[1]["max"] == 3
    assert stats[1]["null_count"] == 1
