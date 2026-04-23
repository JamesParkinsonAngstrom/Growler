from __future__ import annotations

from typing import Callable, Iterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from growler.null_projection import project_nulls_downward
from growler.pointer import Schema
from growler.storage import PointerStore


ParquetReader = Callable[[str, Optional[list[str]]], pa.Table]


def _default_reader(s3_uri: str, columns: Optional[list[str]]) -> pa.Table:
    return pq.read_table(s3_uri, columns=columns)


class RecordHandler:
    def __init__(
        self,
        store: PointerStore,
        parquet_reader: Optional[ParquetReader] = None,
        batch_size: int = 65536,
    ):
        self.store = store
        self.parquet_reader = parquet_reader or _default_reader
        self.batch_size = batch_size

    def read_split(
        self,
        split: dict,
        requested_paths: Optional[list[str]] = None,
    ) -> Iterator[pa.RecordBatch]:
        table_key = split["table"]
        loaded = self.store.load(table_key)
        schema = loaded.pointer.schema
        column_mapping = {int(k): v for k, v in split.get("column_mapping", {}).items()}
        current_paths = requested_paths or [c.path for c in schema.columns]
        top_level_to_read = _top_level_set(current_paths, schema, column_mapping)
        table = self.parquet_reader(split["s3_uri"], sorted(top_level_to_read) or None)
        table = _rename_top_level(table, schema, column_mapping)
        table = project_nulls_downward(table)
        table = _select_current_paths(table, current_paths, schema)
        for batch in table.to_batches(max_chunksize=self.batch_size):
            yield batch


def _top_level_set(
    requested_paths: list[str],
    schema: Schema,
    column_mapping: dict[int, str],
) -> set[str]:
    out: set[str] = set()
    for path in requested_paths:
        col = schema.by_path(path)
        file_path = column_mapping.get(col.id, path)
        out.add(file_path.split(".", 1)[0])
    return out


def _rename_top_level(table: pa.Table, schema: Schema, column_mapping: dict[int, str]) -> pa.Table:
    file_top_to_current_top: dict[str, str] = {}
    for col in schema.columns:
        file_path = column_mapping.get(col.id, col.path)
        file_top = file_path.split(".", 1)[0]
        current_top = col.path.split(".", 1)[0]
        if file_top != current_top and file_top in table.column_names:
            file_top_to_current_top[file_top] = current_top
    if not file_top_to_current_top:
        return table
    new_names = [file_top_to_current_top.get(n, n) for n in table.column_names]
    return table.rename_columns(new_names)


def _select_current_paths(
    table: pa.Table,
    requested_paths: list[str],
    schema: Schema,
) -> pa.Table:
    wanted_top = {p.split(".", 1)[0] for p in requested_paths}
    keep = [n for n in table.column_names if n in wanted_top]
    if len(keep) == len(table.column_names):
        return table
    return table.select(keep)
