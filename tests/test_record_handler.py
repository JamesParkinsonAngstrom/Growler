from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from growler import MetadataHandler, RecordHandler


def test_read_split_end_to_end(handler, tmp_parquet_dir):
    handler.create_table(
        "default/events",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
            {"path": "user", "type": "struct"},
            {"path": "user.name", "type": "string"},
        ],
        partition_paths=["region"],
    )
    user_type = pa.struct([pa.field("name", pa.string())])
    t = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "region": ["us", "us", "us"],
            "user": pa.array(
                [{"name": "a"}, None, {"name": "c"}],
                type=user_type,
            ),
        }
    )
    f = tmp_parquet_dir / "a.parquet"
    pq.write_table(t, f)
    handler.add_files(
        "default/events",
        [{"s3_uri": str(f), "partition_values": {"region": "us"}}],
    )
    md = MetadataHandler(handler.store, catalog={"default": ["events"]})
    splits = md.do_get_splits("default", "events")
    assert len(splits) == 1
    record = RecordHandler(handler.store)
    batches = list(record.read_split(splits[0]))
    assert batches
    out = pa.Table.from_batches(batches)
    rows = out["user"].to_pylist()
    assert rows[1] is None
    assert rows[0] == {"name": "a"}


def test_requested_paths_filtering(handler, tmp_parquet_dir):
    handler.create_table(
        "default/t",
        [
            {"path": "a", "type": "int64", "nullable": False},
            {"path": "b", "type": "string"},
            {"path": "c", "type": "int64"},
        ],
    )
    pa_t = pa.table({"a": pa.array([1], type=pa.int64()), "b": ["x"], "c": pa.array([10], type=pa.int64())})
    f = tmp_parquet_dir / "f.parquet"
    pq.write_table(pa_t, f)
    handler.add_files("default/t", [{"s3_uri": str(f), "partition_values": {}}])
    md = MetadataHandler(handler.store, catalog={"default": ["t"]})
    splits = md.do_get_splits("default", "t")
    record = RecordHandler(handler.store)
    batches = list(record.read_split(splits[0], requested_paths=["a", "c"]))
    out = pa.Table.from_batches(batches)
    assert set(out.column_names) == {"a", "c"}
