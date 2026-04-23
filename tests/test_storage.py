import pytest

from growler import InMemoryPointerStore, Pointer, Schema
from growler.errors import ConcurrentUpdate, TableNotFound


def test_create_load():
    store = InMemoryPointerStore()
    p = Pointer(version=1, schema=Schema())
    loaded = store.create("t", p)
    assert loaded.etag
    back = store.load("t")
    assert back.pointer.version == 1


def test_create_duplicate_raises():
    store = InMemoryPointerStore()
    store.create("t", Pointer(version=1, schema=Schema()))
    with pytest.raises(ConcurrentUpdate):
        store.create("t", Pointer(version=1, schema=Schema()))


def test_load_missing():
    store = InMemoryPointerStore()
    with pytest.raises(TableNotFound):
        store.load("nope")


def test_cas_success():
    store = InMemoryPointerStore()
    store.create("t", Pointer(version=1, schema=Schema()))
    cur = store.load("t")
    updated = cur.pointer.copy()
    updated.version = 2
    result = store.cas_write("t", updated, cur.etag)
    assert result.pointer.version == 2
    assert result.etag != cur.etag


def test_cas_conflict():
    store = InMemoryPointerStore()
    store.create("t", Pointer(version=1, schema=Schema()))
    a = store.load("t")
    b = store.load("t")
    a_updated = a.pointer.copy()
    a_updated.version = 2
    store.cas_write("t", a_updated, a.etag)
    b_updated = b.pointer.copy()
    b_updated.version = 2
    with pytest.raises(ConcurrentUpdate):
        store.cas_write("t", b_updated, b.etag)
