from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from growler import InMemoryPointerStore, WriteHandler


@pytest.fixture
def tmp_parquet_dir(tmp_path: Path) -> Path:
    d = tmp_path / "parquet"
    d.mkdir()
    return d


@pytest.fixture
def store() -> InMemoryPointerStore:
    return InMemoryPointerStore()


@pytest.fixture
def handler(store: InMemoryPointerStore) -> WriteHandler:
    def _size(uri: str) -> int:
        return Path(uri).stat().st_size

    return WriteHandler(
        store=store,
        updated_by="test",
        parquet_opener=lambda uri: pq.ParquetFile(uri),
        size_lookup=_size,
    )


def write_parquet(path: Path, table: pa.Table) -> str:
    pq.write_table(table, path)
    return str(path)
