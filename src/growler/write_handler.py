from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

import pyarrow.parquet as pq

from growler.errors import ColumnExists, ColumnNotFound, TableNotFound
from growler.manifest import all_files, empty_tree, insert_file
from growler.pointer import Column, Pointer, Schema
from growler.storage import PointerStore
from growler.types import validate_type
from growler.validation import (
    extract_leaf_paths,
    extract_stats,
    reject_disallowed_encodings,
    validate_file_schema,
)


ParquetOpener = Callable[[str], pq.ParquetFile]
SizeLookup = Callable[[str], int]


def _default_opener(s3_uri: str) -> pq.ParquetFile:
    return pq.ParquetFile(s3_uri)


def _default_size(s3_uri: str) -> int:
    import os

    if s3_uri.startswith("s3://"):
        raise RuntimeError("Provide a size_lookup for S3 URIs")
    return os.path.getsize(s3_uri)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WriteHandler:
    def __init__(
        self,
        store: PointerStore,
        updated_by: str = "write-lambda",
        parquet_opener: Optional[ParquetOpener] = None,
        size_lookup: Optional[SizeLookup] = None,
        max_retries: int = 3,
    ):
        self.store = store
        self.updated_by = updated_by
        self.parquet_opener = parquet_opener or _default_opener
        self.size_lookup = size_lookup or _default_size
        self.max_retries = max_retries

    def create_table(
        self,
        table: str,
        columns: list[dict],
        partition_paths: Optional[list[str]] = None,
    ) -> int:
        schema = Schema(next_column_id=1, columns=[])
        for c in columns:
            validate_type(c["type"])
            schema.columns.append(
                Column(
                    id=schema.next_column_id,
                    path=c["path"],
                    type=c["type"],
                    nullable=c.get("nullable", True),
                )
            )
            schema.next_column_id += 1
        partition_spec: list[int] = []
        for p in partition_paths or []:
            col = schema.by_path(p)
            partition_spec.append(col.id)
        pointer = Pointer(
            version=1,
            updated_at=_now_iso(),
            updated_by=self.updated_by,
            schema=schema,
            partition_spec=partition_spec,
            watermarks={},
            manifest_tree=empty_tree(),
        )
        loaded = self.store.create(table, pointer)
        return loaded.pointer.version

    def add_files(self, table: str, files: list[dict]) -> int:
        for attempt in range(self.max_retries):
            loaded = self.store.load(table)
            pointer = loaded.pointer
            validated_entries: list[dict] = []
            for f in files:
                s3_uri = f["s3_uri"]
                pq_file = self.parquet_opener(s3_uri)
                reject_disallowed_encodings(pq_file.schema)
                mapping = validate_file_schema(pq_file.schema_arrow, pointer.schema)
                stats = extract_stats(pq_file, mapping)
                pvals = _normalize_partition_values(f.get("partition_values", {}), pointer)
                entry = {
                    "s3_uri": s3_uri,
                    "partition_values": pvals,
                    "row_count": pq_file.metadata.num_rows if pq_file.metadata else 0,
                    "size_bytes": self.size_lookup(s3_uri),
                    "column_mapping": {str(k): v for k, v in mapping.items()},
                    "stats": {str(k): v for k, v in stats.items()},
                }
                validated_entries.append(entry)
            new_pointer = pointer.copy()
            for entry in validated_entries:
                insert_file(
                    new_pointer.manifest_tree,
                    entry,
                    new_pointer.partition_spec,
                    new_pointer.schema,
                )
            for f in files:
                wm = f.get("watermark")
                if wm is None:
                    continue
                key = _watermark_key(f.get("partition_values", {}))
                current = new_pointer.watermarks.get(key, 0)
                new_pointer.watermarks[key] = max(current, int(wm))
            new_pointer.version = pointer.version + 1
            new_pointer.updated_at = _now_iso()
            new_pointer.updated_by = self.updated_by
            try:
                result = self.store.cas_write(table, new_pointer, loaded.etag)
                return result.pointer.version
            except Exception as e:
                from growler.errors import ConcurrentUpdate

                if isinstance(e, ConcurrentUpdate) and attempt < self.max_retries - 1:
                    continue
                raise
        raise RuntimeError("unreachable")

    def add_column(self, table: str, path: str, type_: str, nullable: bool = True) -> int:
        validate_type(type_)
        for attempt in range(self.max_retries):
            loaded = self.store.load(table)
            pointer = loaded.pointer
            if pointer.schema.has_path(path):
                raise ColumnExists(path)
            new_pointer = pointer.copy()
            col_id = new_pointer.schema.next_column_id
            new_pointer.schema.next_column_id += 1
            new_pointer.schema.columns.append(
                Column(id=col_id, path=path, type=type_, nullable=nullable)
            )
            new_pointer.version = pointer.version + 1
            new_pointer.updated_at = _now_iso()
            new_pointer.updated_by = self.updated_by
            try:
                self.store.cas_write(table, new_pointer, loaded.etag)
                return col_id
            except Exception as e:
                from growler.errors import ConcurrentUpdate

                if isinstance(e, ConcurrentUpdate) and attempt < self.max_retries - 1:
                    continue
                raise
        raise RuntimeError("unreachable")

    def drop_column(self, table: str, path: str) -> int:
        for attempt in range(self.max_retries):
            loaded = self.store.load(table)
            pointer = loaded.pointer
            if not pointer.schema.has_path(path):
                raise ColumnNotFound(path)
            new_pointer = pointer.copy()
            new_pointer.schema.columns = [
                c for c in new_pointer.schema.columns if c.path != path
            ]
            dropped = next((c for c in pointer.schema.columns if c.path == path), None)
            if dropped is not None and dropped.id in new_pointer.partition_spec:
                new_pointer.partition_spec = [
                    i for i in new_pointer.partition_spec if i != dropped.id
                ]
            new_pointer.version = pointer.version + 1
            new_pointer.updated_at = _now_iso()
            new_pointer.updated_by = self.updated_by
            try:
                self.store.cas_write(table, new_pointer, loaded.etag)
                return new_pointer.version
            except Exception as e:
                from growler.errors import ConcurrentUpdate

                if isinstance(e, ConcurrentUpdate) and attempt < self.max_retries - 1:
                    continue
                raise
        raise RuntimeError("unreachable")

    def rename_column(self, table: str, old_path: str, new_path: str) -> int:
        for attempt in range(self.max_retries):
            loaded = self.store.load(table)
            pointer = loaded.pointer
            if not pointer.schema.has_path(old_path):
                raise ColumnNotFound(old_path)
            if pointer.schema.has_path(new_path):
                raise ColumnExists(new_path)
            new_pointer = pointer.copy()
            for c in new_pointer.schema.columns:
                if c.path == old_path:
                    c.path = new_path
                    break
            new_pointer.version = pointer.version + 1
            new_pointer.updated_at = _now_iso()
            new_pointer.updated_by = self.updated_by
            try:
                self.store.cas_write(table, new_pointer, loaded.etag)
                return new_pointer.version
            except Exception as e:
                from growler.errors import ConcurrentUpdate

                if isinstance(e, ConcurrentUpdate) and attempt < self.max_retries - 1:
                    continue
                raise
        raise RuntimeError("unreachable")

    def get_watermark(self, table: str, partition_values: dict) -> Optional[int]:
        loaded = self.store.load(table)
        return loaded.pointer.watermarks.get(_watermark_key(partition_values))


def _normalize_partition_values(pvals: dict, pointer: Pointer) -> dict:
    out = {}
    for col_id in pointer.partition_spec:
        col = pointer.schema.by_id(col_id)
        if col.path in pvals:
            out[col.path] = str(pvals[col.path])
        elif str(col.id) in pvals:
            out[col.path] = str(pvals[str(col.id)])
        else:
            raise ValueError(f"Missing partition value for {col.path}")
    return out


def _watermark_key(pvals: dict) -> str:
    items = sorted(pvals.items())
    return ",".join(f"{k}={v}" for k, v in items)
