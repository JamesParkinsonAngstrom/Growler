from growler import Column, Pointer, Schema
from growler.errors import ColumnNotFound
import pytest


def _make_pointer() -> Pointer:
    schema = Schema(
        next_column_id=6,
        columns=[
            Column(id=1, path="user_id", type="int64", nullable=False),
            Column(id=2, path="user.name", type="string"),
            Column(id=3, path="user.addresses", type="list<struct>"),
            Column(id=4, path="user.addresses.city", type="string"),
            Column(id=5, path="event_date", type="date", nullable=False),
        ],
    )
    return Pointer(
        version=1,
        updated_at="2026-04-21T00:00:00Z",
        updated_by="test",
        schema=schema,
        partition_spec=[5],
    )


def test_roundtrip_json():
    p = _make_pointer()
    s = p.to_json()
    p2 = Pointer.from_json(s)
    assert p2.to_dict() == p.to_dict()


def test_schema_lookups():
    schema = _make_pointer().schema
    assert schema.by_id(2).path == "user.name"
    assert schema.by_path("user.addresses.city").id == 4
    assert schema.has_path("event_date")
    with pytest.raises(ColumnNotFound):
        schema.by_id(999)
    with pytest.raises(ColumnNotFound):
        schema.by_path("nope")


def test_leaf_paths():
    schema = _make_pointer().schema
    assert "user.name" in schema.leaf_paths()
    assert "event_date" in schema.leaf_paths()


def test_copy_is_deep():
    p = _make_pointer()
    p2 = p.copy()
    p2.schema.columns[0].path = "changed"
    assert p.schema.columns[0].path == "user_id"


def test_type_validation_on_column_init():
    from growler.errors import InvalidType

    with pytest.raises(InvalidType):
        Column(id=1, path="bad", type="uint64")
