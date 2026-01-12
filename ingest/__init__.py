import logging
import json
import azure.functions as func
import os
import hmac
import hashlib
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient

SIGNING_KEY = os.environ.get("MISS_DIG_SIGNING_KEY", "").encode("utf-8")
STORAGE_CONN = os.environ["AzureWebJobsStorage"]

BLOB_CONTAINER = "missdig-raw"
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
blob_container = blob_service.get_container_client(BLOB_CONTAINER)
try:
    blob_container.create_container()
except Exception:
    pass


def verify_signature(raw_body: bytes, headers) -> bool:
    sig = headers.get("X-POSR-Webhook-Signature") or headers.get("X-Signature")
    if not sig:
        return True

    sig = sig.replace("sha256=", "").lower()
    calc = hmac.new(SIGNING_KEY, raw_body, hashlib.sha256).hexdigest().lower()
    return hmac.compare_digest(sig, calc)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest hit")

    raw_body = req.get_body()

    if not verify_signature(raw_body, req.headers):
        return func.HttpResponse("Invalid signature", status_code=401)

    try:
        body = json.loads(raw_body)
    except Exception:
        body = {}

    notification_id = body.get("NotificationId") or str(uuid.uuid4())
    event_type = (body.get("Event") or "unknown").lower().replace(" ", "")

    now = datetime.utcnow()
    blob_path = (
        f"{now:%Y/%m/%d}/"
        f"{event_type}/"
        f"{now:%Y%m%d_%H%M%S}_{notification_id}.json"
    )

    blob_container.get_blob_client(blob_path).upload_blob(
        raw_body,
        overwrite=False,
        content_type="application/json"
    )

    logging.info(f"Stored raw blob: {blob_path}")

    return func.HttpResponse(
        json.dumps({"status": "received"}),
        mimetype="application/json",
        status_code=200
    )
