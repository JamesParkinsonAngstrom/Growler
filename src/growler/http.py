from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import unquote

from growler.errors import (
    ColumnExists,
    ColumnNotFound,
    ConcurrentUpdate,
    GrowlerError,
    SchemaMismatch,
    TableNotFound,
)
from growler.write_handler import WriteHandler


_TABLE_PREFIX = re.compile(r"^/tables/(?P<table>[^/]+)")


def _status_for(err: GrowlerError) -> int:
    if isinstance(err, SchemaMismatch):
        return 409
    if isinstance(err, ConcurrentUpdate):
        return 409
    if isinstance(err, ColumnExists):
        return 409
    if isinstance(err, ColumnNotFound):
        return 404
    if isinstance(err, TableNotFound):
        return 404
    return 400


def dispatch(
    handler: WriteHandler,
    method: str,
    path: str,
    body: Optional[dict] = None,
    query: Optional[dict] = None,
) -> tuple[int, dict]:
    m = _TABLE_PREFIX.match(path)
    if not m:
        return 404, {"error": "not_found"}
    table = unquote(m.group("table"))
    suffix = path[m.end():]
    body = body or {}
    query = query or {}
    try:
        if method == "POST" and suffix == "/create":
            version = handler.create_table(
                table,
                body.get("columns", []),
                body.get("partition_paths", []) or None,
            )
            return 200, {"version": version}
        if method == "POST" and suffix == "/add_files":
            version = handler.add_files(table, body.get("files", []))
            return 200, {"version": version}
        if method == "POST" and suffix == "/schema/add_column":
            col_id = handler.add_column(
                table,
                body["path"],
                body["type"],
                bool(body.get("nullable", True)),
            )
            return 200, {"column_id": col_id, "version": _latest_version(handler, table)}
        if method == "POST" and suffix == "/schema/drop_column":
            version = handler.drop_column(table, body["path"])
            return 200, {"version": version}
        if method == "POST" and suffix == "/schema/rename_column":
            version = handler.rename_column(table, body["old_path"], body["new_path"])
            return 200, {"version": version}
        if method == "GET" and suffix == "/watermark":
            part = query.get("partition", "")
            pvals = _parse_partition_query(part)
            wm = handler.get_watermark(table, pvals)
            return 200, {"watermark": wm}
    except GrowlerError as e:
        return _status_for(e), e.to_dict()
    return 404, {"error": "not_found"}


def _latest_version(handler: WriteHandler, table: str) -> int:
    return handler.store.load(table).pointer.version


def _parse_partition_query(s: str) -> dict:
    if not s:
        return {}
    out = {}
    for part in s.split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k] = v
    return out


def lambda_handler_factory(handler: WriteHandler):
    def _lambda(event, context=None):
        method = event.get("httpMethod", "GET")
        path = event.get("path", "/")
        body_raw = event.get("body") or "{}"
        body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
        query = event.get("queryStringParameters") or {}
        status, resp = dispatch(handler, method, path, body, query)
        return {
            "statusCode": status,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(resp, default=str),
        }

    return _lambda
