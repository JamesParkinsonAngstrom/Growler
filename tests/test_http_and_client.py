import json
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from growler import Client
from growler.errors import ColumnExists, ConcurrentUpdate, SchemaMismatch, TableNotFound
from growler.http import dispatch, lambda_handler_factory


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _DispatchSession:
    def __init__(self, handler):
        self.handler = handler

    def _split(self, url):
        marker = "/tables/"
        idx = url.index(marker)
        return url[idx:]

    def post(self, url, json=None, timeout=None):
        status, body = dispatch(self.handler, "POST", self._split(url), json or {})
        return _FakeResponse(status, body)

    def get(self, url, params=None, timeout=None):
        status, body = dispatch(self.handler, "GET", self._split(url), None, params or {})
        return _FakeResponse(status, body)


def test_dispatch_create_add_files_schema(handler, tmp_parquet_dir):
    session = _DispatchSession(handler)
    c = Client("http://x", "events", session=session)
    v = c.create_table(
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    assert v == 1
    f = tmp_parquet_dir / "a.parquet"
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int64()), "region": ["us"]}), f)
    v2 = c.add_files([{"s3_uri": str(f), "partition_values": {"region": "us"}, "watermark": 10}])
    assert v2 == 2
    wm = c.get_watermark({"region": "us"})
    assert wm == 10
    col_id = c.add_column("email", "string")
    assert col_id == 3


def test_dispatch_errors_map_to_typed_exceptions(handler, tmp_parquet_dir):
    session = _DispatchSession(handler)
    c = Client("http://x", "events", session=session)
    with pytest.raises(TableNotFound):
        c.add_files([])
    c.create_table([{"path": "id", "type": "int64", "nullable": False}])
    with pytest.raises(ColumnExists):
        c.add_column("id", "int64")


def test_schema_mismatch_propagates(handler, tmp_parquet_dir):
    session = _DispatchSession(handler)
    c = Client("http://x", "events", session=session)
    c.create_table(
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
        ],
        partition_paths=["region"],
    )
    f = tmp_parquet_dir / "bad.parquet"
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), f)
    with pytest.raises(SchemaMismatch):
        c.add_files([{"s3_uri": str(f), "partition_values": {"region": "us"}}])


def test_lambda_handler_factory(handler):
    lam = lambda_handler_factory(handler)
    event = {
        "httpMethod": "POST",
        "path": f"/tables/{quote('default/t', safe='')}/create",
        "body": json.dumps({"columns": [{"path": "id", "type": "int64", "nullable": False}]}),
    }
    resp = lam(event)
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["version"] == 1
