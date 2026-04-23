from __future__ import annotations

import json
import os

from growler.federation.metadata_lambda import MetadataFederationLambda
from growler.metadata_handler import MetadataHandler
from growler.storage import S3PointerStore


def _build() -> MetadataFederationLambda:
    bucket = os.environ["GROWLER_BUCKET"]
    catalog_name = os.environ.get("GROWLER_CATALOG", "growler")
    catalog = json.loads(os.environ.get("GROWLER_CATALOG_JSON", "{}"))
    store = S3PointerStore(bucket=bucket)
    handler = MetadataHandler(store=store, catalog=catalog)
    return MetadataFederationLambda(metadata=handler, catalog_name=catalog_name)


_instance = _build()


def lambda_handler(event, context=None):
    return _instance.lambda_handler(event, context)
