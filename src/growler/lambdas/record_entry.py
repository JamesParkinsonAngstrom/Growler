from __future__ import annotations

import os

from growler.federation.record_lambda import RecordFederationLambda
from growler.record_handler import RecordHandler
from growler.storage import S3PointerStore


def _build() -> RecordFederationLambda:
    bucket = os.environ["GROWLER_BUCKET"]
    catalog_name = os.environ.get("GROWLER_CATALOG", "growler")
    store = S3PointerStore(bucket=bucket)
    handler = RecordHandler(store=store)
    return RecordFederationLambda(record=handler, catalog_name=catalog_name)


_instance = _build()


def lambda_handler(event, context=None):
    return _instance.lambda_handler(event, context)
