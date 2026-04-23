from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from growler.errors import ColumnExists, ColumnNotFound, SchemaMismatch


def _write(path: Path, table: pa.Table) -> str:
    pq.write_table(table, path)
    return str(path)


def test_create_table(handler):
    v = handler.create_table(
        "t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "name", "type": "string"},
            {"path": "event_date", "type": "date", "nullable": False},
        ],
        partition_paths=["event_date"],
    )
    assert v == 1
    loaded = handler.store.load("t")
    assert loaded.pointer.partition_spec == [3]
    assert len(loaded.pointer.schema.columns) == 3


def test_add_files_valid(handler, tmp_parquet_dir):
    handler.create_table(
        "t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    f1 = _write(
        tmp_parquet_dir / "a.parquet",
        pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "region": ["us", "us", "us"]}),
    )
    v = handler.add_files(
        "t",
        [{"s3_uri": f1, "partition_values": {"region": "us"}, "watermark": 100}],
    )
    assert v == 2
    loaded = handler.store.load("t")
    assert loaded.pointer.watermarks["region=us"] == 100


def test_add_files_rejects_missing_column(handler, tmp_parquet_dir):
    handler.create_table(
        "t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "name", "type": "string"},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    f = _write(
        tmp_parquet_dir / "bad.parquet",
        pa.table({"id": pa.array([1], type=pa.int64()), "region": ["us"]}),
    )
    with pytest.raises(SchemaMismatch):
        handler.add_files("t", [{"s3_uri": f, "partition_values": {"region": "us"}}])


def test_add_column(handler):
    handler.create_table("t", [{"path": "id", "type": "int64", "nullable": False}])
    col_id = handler.add_column("t", "email", "string", nullable=True)
    assert col_id == 2
    loaded = handler.store.load("t")
    assert loaded.pointer.schema.by_path("email").type == "string"


def test_add_column_duplicate(handler):
    handler.create_table("t", [{"path": "id", "type": "int64", "nullable": False}])
    with pytest.raises(ColumnExists):
        handler.add_column("t", "id", "int64")


def test_drop_column(handler):
    handler.create_table(
        "t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "name", "type": "string"},
        ],
    )
    handler.drop_column("t", "name")
    loaded = handler.store.load("t")
    assert not loaded.pointer.schema.has_path("name")


def test_drop_column_missing(handler):
    handler.create_table("t", [{"path": "id", "type": "int64", "nullable": False}])
    with pytest.raises(ColumnNotFound):
        handler.drop_column("t", "nope")


def test_rename_column(handler):
    handler.create_table("t", [{"path": "full_name", "type": "string"}])
    handler.rename_column("t", "full_name", "name")
    loaded = handler.store.load("t")
    assert loaded.pointer.schema.by_path("name").id == 1


def test_rename_column_conflict(handler):
    handler.create_table(
        "t",
        [
            {"path": "full_name", "type": "string"},
            {"path": "name", "type": "string"},
        ],
    )
    with pytest.raises(ColumnExists):
        handler.rename_column("t", "full_name", "name")


def test_watermark_tracking(handler, tmp_parquet_dir):
    handler.create_table(
        "t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    f = _write(
        tmp_parquet_dir / "a.parquet",
        pa.table({"id": pa.array([1], type=pa.int64()), "region": ["us"]}),
    )
    handler.add_files("t", [{"s3_uri": f, "partition_values": {"region": "us"}, "watermark": 100}])
    handler.add_files("t", [{"s3_uri": f, "partition_values": {"region": "us"}, "watermark": 50}])
    loaded = handler.store.load("t")
    assert loaded.pointer.watermarks["region=us"] == 100
    assert handler.get_watermark("t", {"region": "us"}) == 100
