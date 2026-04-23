from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from growler import Column, MetadataHandler, Schema
from growler.metadata_handler import build_arrow_schema, eq_predicate


def test_build_arrow_schema_flat():
    schema = Schema(
        next_column_id=3,
        columns=[
            Column(id=1, path="id", type="int64", nullable=False),
            Column(id=2, path="name", type="string"),
        ],
    )
    s = build_arrow_schema(schema)
    assert s.field("id").type == pa.int64()
    assert s.field("name").type == pa.string()


def test_build_arrow_schema_nested_struct():
    schema = Schema(
        next_column_id=5,
        columns=[
            Column(id=1, path="user", type="struct"),
            Column(id=2, path="user.name", type="string"),
            Column(id=3, path="user.age", type="int64"),
        ],
    )
    s = build_arrow_schema(schema)
    user = s.field("user").type
    assert pa.types.is_struct(user)
    assert user.field("name").type == pa.string()


def test_build_arrow_schema_list_of_struct():
    schema = Schema(
        next_column_id=4,
        columns=[
            Column(id=1, path="addrs", type="list<struct>"),
            Column(id=2, path="addrs.city", type="string"),
        ],
    )
    s = build_arrow_schema(schema)
    field = s.field("addrs")
    assert pa.types.is_list(field.type)
    assert pa.types.is_struct(field.type.value_type)
    assert field.type.value_type.field("city").type == pa.string()


def test_get_partitions_and_splits(handler, tmp_parquet_dir):
    handler.create_table(
        "default/events",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    f_us = tmp_parquet_dir / "us.parquet"
    f_eu = tmp_parquet_dir / "eu.parquet"
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int64()), "region": ["us"]}), f_us)
    pq.write_table(pa.table({"id": pa.array([2], type=pa.int64()), "region": ["eu"]}), f_eu)
    handler.add_files(
        "default/events",
        [
            {"s3_uri": str(f_us), "partition_values": {"region": "us"}},
            {"s3_uri": str(f_eu), "partition_values": {"region": "eu"}},
        ],
    )
    md = MetadataHandler(handler.store, catalog={"default": ["events"]})
    assert md.do_list_schema_names() == ["default"]
    assert md.do_list_tables("default") == ["events"]
    t = md.do_get_table("default", "events")
    assert t["partition_columns"] == ["region"]
    parts = md.get_partitions("default", "events")
    assert len(parts) == 2
    splits = md.do_get_splits("default", "events", predicate=eq_predicate(region="us"))
    assert len(splits) == 1
    assert splits[0]["s3_uri"] == str(f_us)
