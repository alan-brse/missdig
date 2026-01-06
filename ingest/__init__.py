import logging
import json
import azure.functions as func
import os
import hmac
import hashlib
import base64

from azure.storage.queue import QueueClient
from azure.data.tables import TableServiceClient

# Load signing key from Azure Function App Settings
SIGNING_KEY = os.environ.get("MISS_DIG_SIGNING_KEY", "").encode("utf-8")
TABLE_CONN = os.environ["AzureWebJobsStorage"]
TABLE_NAME = "missdigdedup"


def verify_signature(raw_body: bytes, signature_header: str) -> bool:
    """
    Validates MISS DIG webhook signature using HMAC-SHA256 + Base64.
    The header arrives as:  'sha256=<hash>'
    """
    if not signature_header or not signature_header.startswith("sha256="):
        logging.error("Missing or invalid signature header format.")
        return False

    sent_hash = signature_header.replace("sha256=", "").strip()

    computed = hmac.new(
        SIGNING_KEY,
        raw_body,
        digestmod=hashlib.sha256
    ).digest()

    computed_b64 = base64.b64encode(computed).decode("utf-8")

    if hmac.compare_digest(computed_b64, sent_hash):
        logging.info("Signature verified successfully ✓")
        return True
    else:
        logging.error(f"Signature mismatch! Sent={sent_hash}, Computed={computed_b64}")
        return False


def check_duplicate(notification_id: str) -> bool:
    """Return True if NotificationId already processed."""
    table_client = TableServiceClient.from_connection_string(TABLE_CONN).get_table_client(TABLE_NAME)
    table_client.create_table_if_not_exists()

    try:
        table_client.get_entity(partition_key="notif", row_key=notification_id)
        logging.info("Duplicate notification detected — skipping processing.")
        return True
    except Exception:
        return False


def mark_processed(notification_id: str, event_type: str):
    """Record NotificationId so future duplicates are ignored."""
    table_client = TableServiceClient.from_connection_string(TABLE_CONN).get_table_client(TABLE_NAME)
    entity = {
        "PartitionKey": "notif",
        "RowKey": notification_id,
        "Event": event_type
    }
    table_client.create_entity(entity)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function hit.")

    raw_body = req.get_body()

    # Retrieve signature header
    signature_header = req.headers.get("X-POSR-Webhook-Signature-Base64")
    
    if signature_header:
        if not verify_signature(raw_body, signature_header):
            return func.HttpResponse("Invalid signature", status_code=401)
    else:
        logging.warning("No signature provided by sender")
    #logging.info(f"Signature header received: {signature_header}")
    #logging.warning(f"ALL HEADERS: {dict(req.headers)}")

    # Verify MISS DIG signature BEFORE processing body
    #if not verify_signature(raw_body, signature_header):
    #    return func.HttpResponse("Invalid signature", status_code=401)
    if not signature_header:
        logging.warning("no signature header present - allowing for now")
    else:
        verify_signature(raw_body,signature_header)
    
    # Parse JSON safely
    try:
        body = json.loads(raw_body)
        logging.info(f"Received JSON: {body}")
    except Exception as e:
        logging.error(f"JSON parse failed: {e}")
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Extract NotificationId
    notification_id = body.get("NotificationId")
    event_type = body.get("Event")

    if not notification_id:
        logging.error("Missing NotificationId — required for deduplication.")
        return func.HttpResponse("Bad Request", status_code=400)

    # DEDUP CHECK BEFORE QUEUEING
    if check_duplicate(notification_id):
        return func.HttpResponse(
            json.dumps({"status": "duplicate", "queued": False}),
            mimetype="application/json",
            status_code=200
        )

    # Mark this notification as processed
    mark_processed(notification_id, event_type)

    # Convert to string for queue
    msg = json.dumps(body)

    # Send to Azure Queue Storage
    try:
        queue = QueueClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            queue_name="missdig-tickets"
        )
        queue.create_queue()
        queue.send_message(msg)
        logging.info("Message queued successfully.")
    except Exception as e:
        logging.error(f"Queue write error: {e}")

    return func.HttpResponse(
        json.dumps({"status": "received", "queued": True}),
        mimetype="application/json",
        status_code=200
    )
