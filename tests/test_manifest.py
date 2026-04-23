from growler import Column, Schema
from growler.manifest import all_files, empty_tree, insert_file, walk_leaves, walk_partitions


def _schema() -> Schema:
    return Schema(
        next_column_id=3,
        columns=[
            Column(id=1, path="event_date", type="date", nullable=False),
            Column(id=2, path="region", type="string", nullable=False),
        ],
    )


def _file(date: str, region: str, name: str) -> dict:
    return {
        "s3_uri": f"s3://b/{name}.parquet",
        "partition_values": {"event_date": date, "region": region},
        "row_count": 1,
        "size_bytes": 100,
        "column_mapping": {},
        "stats": {},
    }


def test_unpartitioned_insert():
    tree = empty_tree()
    insert_file(tree, {"partition_values": {}, "s3_uri": "s3://b/a.parquet"}, [], _schema())
    assert tree["type"] == "leaf"
    assert len(tree["files"]) == 1


def test_nested_partitions_insert_and_walk():
    schema = _schema()
    tree = empty_tree()
    insert_file(tree, _file("2026-04-20", "us", "a"), [1, 2], schema)
    insert_file(tree, _file("2026-04-20", "us", "b"), [1, 2], schema)
    insert_file(tree, _file("2026-04-20", "eu", "c"), [1, 2], schema)
    insert_file(tree, _file("2026-04-21", "us", "d"), [1, 2], schema)
    files = list(all_files(tree))
    assert len(files) == 4
    partitions = list(walk_partitions(tree, schema))
    assert len(partitions) == 3


def test_pruning_predicate():
    schema = _schema()
    tree = empty_tree()
    insert_file(tree, _file("2026-04-20", "us", "a"), [1, 2], schema)
    insert_file(tree, _file("2026-04-20", "eu", "b"), [1, 2], schema)
    insert_file(tree, _file("2026-04-21", "us", "c"), [1, 2], schema)

    def pred(col_id: int, path: str, value: str) -> bool:
        if path == "event_date":
            return value == "2026-04-20"
        return True

    partitions = list(walk_partitions(tree, schema, pred))
    dates = {p["partition_values"]["event_date"] for p in partitions}
    assert dates == {"2026-04-20"}


def test_missing_partition_raises():
    import pytest

    schema = _schema()
    tree = empty_tree()
    with pytest.raises(ValueError):
        insert_file(tree, {"s3_uri": "s", "partition_values": {"event_date": "x"}}, [1, 2], schema)
