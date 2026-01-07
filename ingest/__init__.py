import logging
import json
import azure.functions as func
import os
import hmac
import hashlib
import uuid

from azure.storage.queue import QueueClient
from azure.data.tables import TableServiceClient

SIGNING_KEY = os.environ.get("MISS_DIG_SIGNING_KEY", "").encode("utf-8")
TABLE_CONN = os.environ["AzureWebJobsStorage"]
TABLE_NAME = "missdigdedup"


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

    logging.info(f"HMAC calc={calc_hex}")
    logging.info(f"HMAC recv={recv_hex}")

    return hmac.compare_digest(calc_hex, recv_hex)


def check_duplicate(notification_id: str) -> bool:
    table_client = TableServiceClient.from_connection_string(TABLE_CONN).get_table_client(TABLE_NAME)
    table_client.create_table_if_not_exists()

    try:
        table_client.get_entity(partition_key="notif", row_key=notification_id)
        logging.info("Duplicate notification detected — skipping.")
        return True
    except Exception:
        return False


def mark_processed(notification_id: str, event_type: str):
    table_client = TableServiceClient.from_connection_string(TABLE_CONN).get_table_client(TABLE_NAME)
    table_client.create_entity({
        "PartitionKey": "notif",
        "RowKey": notification_id,
        "Event": event_type
    })


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function hit.")

    raw_body = req.get_body()

    if not verify_signature(raw_body, req.headers):
        return func.HttpResponse("Invalid signature", status_code=401)

    try:
        body = json.loads(raw_body)
        logging.info(f"Received JSON: {body}")
    except Exception:
        logging.warning("Payload not JSON; storing raw")
        body = {}

    notification_id = body.get("NotificationId") or str(uuid.uuid4())
    event_type = body.get("Event") or body.get("EventType") or "unknown"

    if check_duplicate(notification_id):
        return func.HttpResponse(
            json.dumps({"status": "duplicate", "queued": False}),
            mimetype="application/json",
            status_code=200
        )

    mark_processed(notification_id, event_type)

    try:
        queue = QueueClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            queue_name="missdig-tickets"
        )
        queue.create_queue()
        queue.send_message(raw_body.decode("utf-8", errors="ignore"))
        logging.info("Message queued successfully.")
    except Exception as e:
        logging.error(f"Queue write error: {e}")

    return func.HttpResponse(
        json.dumps({"status": "received", "queued": True}),
        mimetype="application/json",
        status_code=200
    )
