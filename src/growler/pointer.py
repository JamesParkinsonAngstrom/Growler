from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field, asdict
from typing import Any

from growler.errors import ColumnNotFound
from growler.types import validate_type


FORMAT_VERSION = 1


@dataclass
class Column:
    id: int
    path: str
    type: str
    nullable: bool = True

    def __post_init__(self):
        validate_type(self.type)


@dataclass
class Schema:
    next_column_id: int = 1
    columns: list[Column] = field(default_factory=list)

    def by_id(self, column_id: int) -> Column:
        for c in self.columns:
            if c.id == column_id:
                return c
        raise ColumnNotFound(column_id)

    def by_path(self, path: str) -> Column:
        for c in self.columns:
            if c.path == path:
                return c
        raise ColumnNotFound(path)

    def has_path(self, path: str) -> bool:
        return any(c.path == path for c in self.columns)

    def leaf_paths(self) -> set[str]:
        return {c.path for c in self.columns}


@dataclass
class ManifestLeaf:
    type: str = "leaf"
    files: list[dict] = field(default_factory=list)


@dataclass
class ManifestInternal:
    type: str = "internal"
    partition_column_id: int | None = None
    children: dict[str, Any] = field(default_factory=dict)


@dataclass
class Pointer:
    format_version: int = FORMAT_VERSION
    version: int = 0
    updated_at: str = ""
    updated_by: str = ""
    schema: Schema = field(default_factory=Schema)
    partition_spec: list[int] = field(default_factory=list)
    watermarks: dict[str, int] = field(default_factory=dict)
    manifest_tree: dict = field(default_factory=lambda: asdict(ManifestLeaf()))

    def to_dict(self) -> dict:
        return {
            "format_version": self.format_version,
            "version": self.version,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "schema": {
                "next_column_id": self.schema.next_column_id,
                "columns": [asdict(c) for c in self.schema.columns],
            },
            "partition_spec": list(self.partition_spec),
            "watermarks": dict(self.watermarks),
            "manifest_tree": copy.deepcopy(self.manifest_tree),
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, d: dict) -> "Pointer":
        schema_d = d.get("schema", {})
        schema = Schema(
            next_column_id=schema_d.get("next_column_id", 1),
            columns=[Column(**c) for c in schema_d.get("columns", [])],
        )
        return cls(
            format_version=d.get("format_version", FORMAT_VERSION),
            version=d.get("version", 0),
            updated_at=d.get("updated_at", ""),
            updated_by=d.get("updated_by", ""),
            schema=schema,
            partition_spec=list(d.get("partition_spec", [])),
            watermarks=dict(d.get("watermarks", {})),
            manifest_tree=copy.deepcopy(d.get("manifest_tree", asdict(ManifestLeaf()))),
        )

    @classmethod
    def from_json(cls, s: str) -> "Pointer":
        return cls.from_dict(json.loads(s))

    def copy(self) -> "Pointer":
        return Pointer.from_dict(self.to_dict())
