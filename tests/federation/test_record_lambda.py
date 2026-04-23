from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from growler import MetadataHandler, RecordHandler
from growler.federation import (
    MetadataFederationLambda,
    RecordFederationLambda,
    decode_block,
    encode_schema,
)
from tests.federation import fixtures as fx


def _prepare(handler, tmp_parquet_dir: Path):
    handler.create_table(
        "default/t",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "name", "type": "string"},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    p = tmp_parquet_dir / "a.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": ["a", "b", "c"],
                "region": ["us", "us", "us"],
            }
        ),
        p,
    )
    handler.add_files(
        "default/t",
        [{"s3_uri": str(p), "partition_values": {"region": "us"}}],
    )


def test_ping(handler, tmp_parquet_dir):
    _prepare(handler, tmp_parquet_dir)
    lam = RecordFederationLambda(RecordHandler(handler.store))
    resp = lam.handle(fx.ping())
    assert resp["@type"] == "PingResponse"


def test_read_records_roundtrip(handler, tmp_parquet_dir):
    _prepare(handler, tmp_parquet_dir)
    md_lam = MetadataFederationLambda(MetadataHandler(handler.store, catalog={"default": ["t"]}))
    layout = md_lam.handle(fx.get_table_layout("default", "t"))
    splits_resp = md_lam.handle(fx.get_splits("default", "t", layout["partitions"]))
    split = splits_resp["splits"][0]
    requested_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    event = fx.read_records(
        "default",
        "t",
        split,
        requested_schema_b64=encode_schema(requested_schema),
    )
    rec_lam = RecordFederationLambda(RecordHandler(handler.store))
    resp = rec_lam.handle(event)
    assert resp["@type"] == "ReadRecordsResponse"
    schema, batch = decode_block(resp["records"])
    assert set(schema.names) == {"id", "name"}
    assert batch.num_rows == 3
    assert batch.column("name").to_pylist() == ["a", "b", "c"]


def test_read_records_with_equatable_constraint(handler, tmp_parquet_dir):
    _prepare(handler, tmp_parquet_dir)
    md_lam = MetadataFederationLambda(MetadataHandler(handler.store, catalog={"default": ["t"]}))
    layout = md_lam.handle(fx.get_table_layout("default", "t"))
    splits_resp = md_lam.handle(fx.get_splits("default", "t", layout["partitions"]))
    split = splits_resp["splits"][0]
    requested_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    event = fx.read_records(
        "default",
        "t",
        split,
        requested_schema_b64=encode_schema(requested_schema),
        constraints={
            "@type": "Constraints",
            "summary": {"name": fx.eq_value_set(["b"])},
        },
    )
    rec_lam = RecordFederationLambda(RecordHandler(handler.store))
    resp = rec_lam.handle(event)
    _, batch = decode_block(resp["records"])
    assert batch.column("name").to_pylist() == ["b"]
    assert batch.column("id").to_pylist() == [2]


def test_unknown_request(handler, tmp_parquet_dir):
    _prepare(handler, tmp_parquet_dir)
    rec_lam = RecordFederationLambda(RecordHandler(handler.store))
    resp = rec_lam.handle({"@type": "UnknownRequest"})
    assert resp["@type"] == "FederationException"
