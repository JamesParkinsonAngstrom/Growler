from __future__ import annotations

import json
from typing import Any, Optional

import pyarrow as pa

from growler.errors import GrowlerError, TableNotFound
from growler.federation.constraints import ConstraintsEvaluator, parse_constraints
from growler.federation.serde import encode_schema, partitions_block_from_dicts
from growler.federation.wire import (
    error_envelope,
    make_split,
    ok_envelope,
    req_type,
    table_name,
)
from growler.metadata_handler import MetadataHandler


SOURCE_TYPE = "growler"


class MetadataFederationLambda:
    def __init__(
        self,
        metadata: MetadataHandler,
        catalog_name: str = "growler",
        source_type: str = SOURCE_TYPE,
    ):
        self.metadata = metadata
        self.catalog_name = catalog_name
        self.source_type = source_type

    def handle(self, event: dict) -> dict:
        try:
            rt = req_type(event)
            if rt == "PingRequest":
                return self._ping(event)
            if rt == "ListSchemasRequest":
                return self._list_schemas(event)
            if rt == "ListTablesRequest":
                return self._list_tables(event)
            if rt == "GetTableRequest":
                return self._get_table(event)
            if rt == "GetTableLayoutRequest":
                return self._get_table_layout(event)
            if rt == "GetSplitsRequest":
                return self._get_splits(event)
            return error_envelope(f"Unknown request type: {rt}", "UnsupportedOperationException")
        except TableNotFound as e:
            return error_envelope(f"Table not found: {e.table}", "TableNotFoundException")
        except GrowlerError as e:
            return error_envelope(str(e), "InternalError")

    def lambda_handler(self, event: dict, context: Any = None) -> dict:
        return self.handle(event)

    def _ping(self, event: dict) -> dict:
        return ok_envelope(
            "PingResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            sourceType=self.source_type,
            capabilities=0,
            serDeVersion=2,
            queryId=event.get("queryId", ""),
        )

    def _list_schemas(self, event: dict) -> dict:
        return ok_envelope(
            "ListSchemasResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            schemas=self.metadata.do_list_schema_names(),
        )

    def _list_tables(self, event: dict) -> dict:
        schema = event.get("schemaName") or event.get("schema") or ""
        tables = [
            {"@type": "TableName", "schemaName": schema, "tableName": t}
            for t in self.metadata.do_list_tables(schema)
        ]
        return ok_envelope(
            "ListTablesResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            tables=tables,
        )

    def _get_table(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        info = self.metadata.do_get_table(schema_name, table)
        return ok_envelope(
            "GetTableResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            tableName={"@type": "TableName", "schemaName": schema_name, "tableName": table},
            schema={"@type": "Schema", "schema": encode_schema(info["arrow_schema"])},
            partitionColumns=info["partition_columns"],
        )

    def _get_table_layout(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        constraints_json = event.get("constraints") or {}
        parsed = parse_constraints(constraints_json)
        evaluator = ConstraintsEvaluator(parsed)
        predicate = evaluator.partition_predicate() if parsed else None
        parts = self.metadata.get_partitions(schema_name, table, predicate)
        info = self.metadata.do_get_table(schema_name, table)
        partition_columns = info["partition_columns"]
        partition_types = _partition_types(info["arrow_schema"], partition_columns)
        rows = [p["partition_values"] for p in parts]
        block = partitions_block_from_dicts(list(zip(partition_columns, partition_types)), rows)
        return ok_envelope(
            "GetTableLayoutResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            tableName={"@type": "TableName", "schemaName": schema_name, "tableName": table},
            partitions=block,
        )

    def _get_splits(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        partitions_block = event.get("partitions") or None
        continuation_token = event.get("continuationToken")
        from growler.federation.serde import partitions_from_block

        if partitions_block:
            partition_rows = partitions_from_block(partitions_block)
            eligible = set(_tuple_key(r) for r in partition_rows)
        else:
            eligible = None
        splits = self.metadata.do_get_splits(schema_name, table)
        if eligible is not None:
            splits = [s for s in splits if _tuple_key(s.get("partition_values", {})) in eligible]
        start = int(continuation_token) if continuation_token else 0
        page_size = 1000
        page = splits[start : start + page_size]
        next_token = str(start + page_size) if start + page_size < len(splits) else None
        split_objs = [
            make_split(
                properties={
                    "s3_uri": s["s3_uri"],
                    "table": s["table"],
                    "column_mapping": json.dumps(s.get("column_mapping", {})),
                    "partition_values": json.dumps(s.get("partition_values", {})),
                    "row_count": str(s.get("row_count", 0)),
                }
            )
            for s in page
        ]
        resp = ok_envelope(
            "GetSplitsResponse",
            catalog_name=event.get("catalogName", self.catalog_name),
            splits=split_objs,
        )
        if next_token is not None:
            resp["continuationToken"] = next_token
        return resp


def _tuple_key(pvals: dict[str, str]) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in pvals.items()))


def _partition_types(arrow_schema: pa.Schema, partition_columns: list[str]) -> list[pa.DataType]:
    types: list[pa.DataType] = []
    for p in partition_columns:
        try:
            t = arrow_schema.field(p).type
            if pa.types.is_date(t) or pa.types.is_timestamp(t) or pa.types.is_decimal(t):
                types.append(pa.string())
            else:
                types.append(pa.string() if not pa.types.is_primitive(t) else t)
        except KeyError:
            types.append(pa.string())
    return types
