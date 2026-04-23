from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
import pyarrow.parquet as pq

from growler.http import lambda_handler_factory
from growler.storage import S3PointerStore
from growler.write_handler import WriteHandler


def _build():
    bucket = os.environ["GROWLER_BUCKET"]
    updated_by = os.environ.get("GROWLER_UPDATED_BY", "write-lambda")
    s3 = boto3.client("s3")

    def size_lookup(s3_uri: str) -> int:
        p = urlparse(s3_uri)
        resp = s3.head_object(Bucket=p.netloc, Key=p.path.lstrip("/"))
        return int(resp["ContentLength"])

    def parquet_opener(s3_uri: str) -> pq.ParquetFile:
        return pq.ParquetFile(s3_uri)

    store = S3PointerStore(bucket=bucket, client=s3)
    handler = WriteHandler(
        store=store,
        updated_by=updated_by,
        parquet_opener=parquet_opener,
        size_lookup=size_lookup,
    )
    return lambda_handler_factory(handler)


lambda_handler = _build()
