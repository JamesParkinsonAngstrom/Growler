import pyarrow as pa

from growler.null_projection import project_nulls_downward


def test_flat_unchanged():
    t = pa.table({"a": [1, 2, None], "b": ["x", None, "z"]})
    out = project_nulls_downward(t)
    assert out["a"].to_pylist() == [1, 2, None]
    assert out["b"].to_pylist() == ["x", None, "z"]


def test_struct_null_propagates_to_children():
    s = pa.array(
        [{"name": "a", "age": 1}, None, {"name": "c", "age": 3}],
        type=pa.struct([pa.field("name", pa.string()), pa.field("age", pa.int64())]),
    )
    t = pa.table({"user": s})
    out = project_nulls_downward(t)
    rows = out["user"].to_pylist()
    assert rows[0] == {"name": "a", "age": 1}
    assert rows[1] is None
    assert rows[2] == {"name": "c", "age": 3}


def test_nested_struct_null_propagates():
    inner = pa.struct([pa.field("city", pa.string())])
    outer = pa.struct([pa.field("addr", inner), pa.field("name", pa.string())])
    arr = pa.array(
        [
            {"addr": {"city": "NYC"}, "name": "a"},
            None,
            {"addr": None, "name": "c"},
        ],
        type=outer,
    )
    t = pa.table({"u": arr})
    out = project_nulls_downward(t)
    rows = out["u"].to_pylist()
    assert rows[0]["addr"]["city"] == "NYC"
    assert rows[1] is None
    assert rows[2]["addr"] is None


def test_list_is_not_descended():
    list_type = pa.list_(pa.struct([pa.field("city", pa.string())]))
    arr = pa.array(
        [
            [{"city": "A"}, {"city": "B"}],
            None,
            [],
        ],
        type=list_type,
    )
    t = pa.table({"addrs": arr})
    out = project_nulls_downward(t)
    rows = out["addrs"].to_pylist()
    assert rows[0] == [{"city": "A"}, {"city": "B"}]
    assert rows[1] is None
    assert rows[2] == []
