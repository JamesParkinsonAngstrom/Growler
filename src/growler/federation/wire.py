from __future__ import annotations

from typing import Any, Optional


REQUEST_TYPES = {
    "PingRequest",
    "ListSchemasRequest",
    "ListTablesRequest",
    "GetTableRequest",
    "GetTableLayoutRequest",
    "GetSplitsRequest",
    "ReadRecordsRequest",
}


def req_type(event: dict) -> str:
    t = event.get("@type") or ""
    if "." in t:
        t = t.rsplit(".", 1)[-1]
    return t


def ok_envelope(
    response_type: str,
    catalog_name: Optional[str] = None,
    request_type: Optional[str] = None,
    **fields: Any,
) -> dict:
    resp: dict[str, Any] = {"@type": response_type}
    if catalog_name is not None:
        resp["catalogName"] = catalog_name
    if request_type is not None:
        resp["requestType"] = request_type
    resp.update(fields)
    return resp


def error_envelope(message: str, error_code: str = "InternalError") -> dict:
    return {
        "@type": "FederationException",
        "message": message,
        "errorCode": error_code,
    }


def table_name(name: dict) -> tuple[str, str]:
    return name.get("schemaName", ""), name.get("tableName", "")


def split_properties(split: dict) -> dict[str, str]:
    return dict(split.get("properties") or {})


def make_split(properties: dict[str, str], spill_location: Optional[dict] = None, encryption_key: Optional[dict] = None) -> dict:
    out: dict[str, Any] = {"@type": "Split", "properties": dict(properties)}
    if spill_location is not None:
        out["spillLocation"] = spill_location
    if encryption_key is not None:
        out["encryptionKey"] = encryption_key
    return out
