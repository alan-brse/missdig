import logging
import json
import azure.functions as func
import os
import hmac
import hashlib
import base64

from azure.storage.queue import QueueClient

# Load signing key from Azure Function App Settings
SIGNING_KEY = os.environ.get("MISS_DIG_SIGNING_KEY", "").encode("utf-8")

def verify_signature(raw_body: bytes, signature_header: str) -> bool:
    """
    Validates MISS DIG webhook signature using HMAC-SHA256 + Base64.
    The header arrives as:  'sha256=<hash>'
    """
    if not signature_header or not signature_header.startswith("sha256="):
        logging.error("Missing or invalid signature header format.")
        return False

    sent_hash = signature_header.replace("sha256=", "").strip()

    # Compute HMAC SHA256 using your signing key
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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function hit.")

    # Raw body (needed for signature hash calculation)
    raw_body = req.get_body()

    # Retrieve signature header
    signature_header = req.headers.get("X-POSR-Webhook-Signature-Base64")
    logging.info(f"Signature header received: {signature_header}")

    # Verify MISS DIG signature BEFORE processing body
    if not verify_signature(raw_body, signature_header):
        return func.HttpResponse("Invalid signature", status_code=401)

    # Parse JSON safely
    try:
        body = json.loads(raw_body)
        logging.info(f"Received JSON: {body}")
    except Exception as e:
        logging.error(f"JSON parse failed: {e}")
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Convert to string for queue
    msg = json.dumps(body)

    # Send to Azure Queue Storage
    try:
        queue = QueueClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            queue_name="missdig-tickets"
        )
        queue.create_queue()  # Creates or no-op if exists
        queue.send_message(msg)
        logging.info("Message queued successfully.")
    except Exception as e:
        logging.error(f"Queue write error: {e}")

    # Respond fast — MISS DIG requires <5 seconds turnaround
    return func.HttpResponse(
        json.dumps({"status": "received", "queued": True}),
        mimetype="application/json",
        status_code=200
    )
