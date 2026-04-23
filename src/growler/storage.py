from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol
from urllib.parse import urlparse

from growler.errors import ConcurrentUpdate, TableNotFound
from growler.pointer import Pointer


def pointer_key(table: str) -> str:
    return f"{table.strip('/')}/_metadata/pointer.json"


def pointer_s3_uri(bucket: str, table: str) -> str:
    return f"s3://{bucket}/{pointer_key(table)}"


@dataclass
class LoadedPointer:
    pointer: Pointer
    etag: str


class PointerStore(Protocol):
    def load(self, table: str) -> LoadedPointer: ...
    def create(self, table: str, pointer: Pointer) -> LoadedPointer: ...
    def cas_write(self, table: str, pointer: Pointer, etag: str) -> LoadedPointer: ...


class S3PointerStore:
    def __init__(self, bucket: str, client=None):
        import boto3

        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def _key(self, table: str) -> str:
        return pointer_key(table)

    def load(self, table: str) -> LoadedPointer:
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self._key(table))
        except self.s3.exceptions.NoSuchKey:
            raise TableNotFound(table)
        except Exception as e:
            if getattr(e, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                raise TableNotFound(table)
            raise
        body = resp["Body"].read().decode("utf-8")
        etag = resp["ETag"].strip('"')
        return LoadedPointer(pointer=Pointer.from_json(body), etag=etag)

    def create(self, table: str, pointer: Pointer) -> LoadedPointer:
        body = pointer.to_json().encode("utf-8")
        resp = self.s3.put_object(
            Bucket=self.bucket,
            Key=self._key(table),
            Body=body,
            ContentType="application/json",
            IfNoneMatch="*",
        )
        etag = resp["ETag"].strip('"')
        return LoadedPointer(pointer=pointer, etag=etag)

    def cas_write(self, table: str, pointer: Pointer, etag: str) -> LoadedPointer:
        body = pointer.to_json().encode("utf-8")
        try:
            resp = self.s3.put_object(
                Bucket=self.bucket,
                Key=self._key(table),
                Body=body,
                ContentType="application/json",
                IfMatch=etag,
            )
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "ConditionalRequestConflict", "412"):
                raise ConcurrentUpdate()
            raise
        new_etag = resp["ETag"].strip('"')
        return LoadedPointer(pointer=pointer, etag=new_etag)


class InMemoryPointerStore:
    def __init__(self):
        self._data: dict[str, tuple[str, Pointer]] = {}
        self._counter = 0

    def _next_etag(self) -> str:
        self._counter += 1
        return f"etag-{self._counter}"

    def load(self, table: str) -> LoadedPointer:
        if table not in self._data:
            raise TableNotFound(table)
        etag, pointer = self._data[table]
        return LoadedPointer(pointer=pointer.copy(), etag=etag)

    def create(self, table: str, pointer: Pointer) -> LoadedPointer:
        if table in self._data:
            raise ConcurrentUpdate()
        etag = self._next_etag()
        self._data[table] = (etag, pointer.copy())
        return LoadedPointer(pointer=pointer.copy(), etag=etag)

    def cas_write(self, table: str, pointer: Pointer, etag: str) -> LoadedPointer:
        if table not in self._data:
            raise TableNotFound(table)
        current_etag, _ = self._data[table]
        if current_etag != etag:
            raise ConcurrentUpdate()
        new_etag = self._next_etag()
        self._data[table] = (new_etag, pointer.copy())
        return LoadedPointer(pointer=pointer.copy(), etag=new_etag)

    def tables(self) -> list[str]:
        return sorted(self._data.keys())
