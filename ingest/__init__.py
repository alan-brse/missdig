import logging
import json
import azure.functions as func
import os
import hmac
import hashlib
import uuid

from azure.storage.queue import QueueClient
from azure.data.tables import TableServiceClient

from azure.storage.blob import BlobServiceClient
from datetime import datetime

SIGNING_KEY = os.environ.get("MISS_DIG_SIGNING_KEY", "").encode("utf-8")
TABLE_CONN = os.environ["AzureWebJobsStorage"]
TABLE_NAME = "missdigdedup"

table_service = TableServiceClient.from_connection_string(TABLE_CONN)
table_service.create_table_if_not_exists(TABLE_NAME)
table_client = table_service.get_table_client(TABLE_NAME)

BLOB_CONTAINER = "missdig-raw"
blob_service = BlobServiceClient.from_connection_string(
    os.environ["AzureWebJobsStorage"]
)
blob_container = blob_service.get_container_client(BLOB_CONTAINER)
try:
    blob_container.create_container()
except Exception:
    pass

QUEUE_NAME = "missdig-tickets"
queue_client = QueueClient.from_connection_string(
    conn_str=os.environ["AzureWebJobsStorage"],
    queue_name=QUEUE_NAME
)
try:
    queue_client.create_queue()
except Exception:
    pass


def get_signature(headers):
    for name in ("X-POSR-Webhook-Signature", "X-Signature"):
        val = headers.get(name)
        if val:
            return val
    return None


def verify_signature(raw_body: bytes, headers) -> bool:
    sig_hdr = get_signature(headers)
    if not sig_hdr:
        logging.warning("No signature header present")
        return True  # match AWS behavior

    sig_val = sig_hdr.strip()
    if sig_val.lower().startswith("sha256="):
        recv_hex = sig_val.split("=", 1)[1].strip().lower()
    else:
        recv_hex = sig_val.lower()

    calc_hex = hmac.new(
        SIGNING_KEY,
        raw_body,
        hashlib.sha256
    ).hexdigest().lower()

    logging.debug("HMAC verification performed")

    return hmac.compare_digest(calc_hex, recv_hex)

def write_raw_blob(raw_body: bytes, event_type: str, notification_id: str):
    now = datetime.utcnow()
    event_safe = (event_type or "unknown").lower().replace(" ", "")
    
    blob_path = (
        f"{now:%Y/%m/%d}/"
        f"{event_safe}/"
        f"{now:%Y%m%d_%H%M%S}_{notification_id}.json"
    )

    blob_client = blob_container.get_blob_client(blob_path)

    blob_client.upload_blob(
        raw_body,
        overwrite=False,
        content_type="application/json"
    )

    logging.info(f"Raw webhook stored: {blob_path}")

def check_duplicate(notification_id: str) -> bool:
    #service = TableServiceClient.from_connection_string(TABLE_CONN)
    #service.create_table_if_not_exists(TABLE_NAME)
    #table_client = service.get_table_client(TABLE_NAME)

    try:
        table_client.get_entity(
            partition_key="notif",
            row_key=notification_id
        )
        #table_client.get_entity(partition_key="notif", row_key=notification_id)
        #logging.info("Duplicate notification detected — skipping.")
        return True
    except Exception:
        return False


def mark_processed(notification_id: str, event_type: str):
    #service = TableServiceClient.from_connection_string(TABLE_CONN)
    #service.create_table_if_not_exists(TABLE_NAME)
    #table_client = service.get_table_client(TABLE_NAME)
    
    entity = {
        "PartitionKey": "notif",
        "RowKey": notification_id,
        "Event": event_type
    }
    try:
        table_client.create_entity(entity)
    except Exception:
        pass

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function hit.")

    raw_body = req.get_body()

    # 🔐 Signature verification (fail fast)
    if not verify_signature(raw_body, req.headers):
        return func.HttpResponse("Invalid signature", status_code=401)

    # 📦 Parse JSON (best-effort only)
    try:
        body = json.loads(raw_body)
        logging.info(f"Received JSON")
    except Exception:
        logging.warning("Payload not JSON; storing raw only")
        body = {}

    # 🆔 Safe identifiers
    notification_id = body.get("NotificationId") or str(uuid.uuid4())
    event_type = body.get("Event") or body.get("EventType") or "unknown"

    # 🧱 RAW BLOB STORAGE — ALWAYS
    try:
        write_raw_blob(raw_body, event_type, notification_id)
    except Exception as e:
        logging.error(f"Blob write failed: {e}")

    # 🔁 Deduplication (after raw storage)
    if check_duplicate(notification_id):
        return func.HttpResponse(
            json.dumps({"status": "duplicate", "queued": False}),
            mimetype="application/json",
            status_code=200
        )

    # ✅ Mark processed
    mark_processed(notification_id, event_type)

    # 📬 Queue fan-out
    try:
        queue_client.send_message(
            raw_body.decode("utf-8", errors="ignore")
        )
        logging.info("Message queued successfully.")
    except Exception as e:
        logging.error(f"Queue write error: {e}")

    return func.HttpResponse(
        json.dumps({"status": "received", "queued": True}),
        mimetype="application/json",
        status_code=200
    )




