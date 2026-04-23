from __future__ import annotations

from typing import Optional
from urllib.parse import quote

import requests

from growler.errors import (
    ColumnExists,
    ColumnNotFound,
    ConcurrentUpdate,
    GrowlerError,
    SchemaMismatch,
    TableNotFound,
)


class Client:
    def __init__(
        self,
        base_url: str,
        table: str,
        session: Optional[requests.Session] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.table = table
        self.session = session or requests.Session()
        self.timeout = timeout

    def _url(self, suffix: str) -> str:
        return f"{self.base_url}/tables/{quote(self.table, safe='')}{suffix}"

    def _post(self, suffix: str, body: dict) -> dict:
        r = self.session.post(self._url(suffix), json=body, timeout=self.timeout)
        return _decode(r)

    def _get(self, suffix: str, params: Optional[dict] = None) -> dict:
        r = self.session.get(self._url(suffix), params=params or {}, timeout=self.timeout)
        return _decode(r)

    def create_table(self, columns: list[dict], partition_paths: Optional[list[str]] = None) -> int:
        return self._post("/create", {"columns": columns, "partition_paths": partition_paths or []})["version"]

    def add_files(self, files: list[dict]) -> int:
        return self._post("/add_files", {"files": files})["version"]

    def add_column(self, path: str, type_: str, nullable: bool = True) -> int:
        return self._post(
            "/schema/add_column",
            {"path": path, "type": type_, "nullable": nullable},
        )["column_id"]

    def drop_column(self, path: str) -> int:
        return self._post("/schema/drop_column", {"path": path})["version"]

    def rename_column(self, old_path: str, new_path: str) -> int:
        return self._post(
            "/schema/rename_column",
            {"old_path": old_path, "new_path": new_path},
        )["version"]

    def get_watermark(self, partition_values: dict) -> Optional[int]:
        key = ",".join(f"{k}={v}" for k, v in sorted(partition_values.items()))
        resp = self._get("/watermark", {"partition": key})
        return resp.get("watermark")


def _decode(r: requests.Response) -> dict:
    if r.status_code == 200:
        return r.json()
    try:
        body = r.json()
    except Exception:
        r.raise_for_status()
        raise GrowlerError(f"Unexpected status {r.status_code}")
    err = body.get("error")
    if err == "schema_mismatch":
        raise SchemaMismatch(body.get("message", "schema mismatch"), **body.get("details", {}))
    if err == "concurrent_update":
        raise ConcurrentUpdate()
    if err == "column_exists":
        raise ColumnExists(body.get("path", ""))
    if err == "column_not_found":
        raise ColumnNotFound(body.get("key", ""))
    if err == "table_not_found":
        raise TableNotFound(body.get("table", ""))
    raise GrowlerError(body.get("message") or str(body))
