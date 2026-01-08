import logging
import json
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.storage.queue import QueueClient


QUEUE_NAME = "missdig-normalized-events"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]


def map_event_type(missdig_event: str) -> str:
    mapping = {
        "TICKET CREATION": "TICKET_CREATED",
        "TICKET UPDATE": "TICKET_UPDATED",
        "TICKET CANCELLED": "TICKET_CANCELLED"
    }
    return mapping.get(missdig_event, "UNKNOWN")


def normalize_ticket(raw: dict, blob_uri: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    normalized = {
        "schema_version": "1.0",

        "source": {
            "system": "MISS_DIG",
            "vendor_ticket_id": raw.get("TicketNumber"),
            "notification_id": raw.get("NotificationId")
        },

        "event": {
            "type": map_event_type(raw.get("Event")),
            "occurred_at": raw.get("TimeStamp"),
            "received_at": now
        },

        "ticket": {
            "id": raw.get("TicketNumber"),
            "status": "ACTIVE",
            "priority": "NORMAL",
            "expires_at": raw.get("ExpirationDate")
        },

        "location": {
            "address": {
                "full": raw.get("DigsiteAddress"),
                "city": raw.get("City"),
                "state": raw.get("State"),
                "postal_code": raw.get("Zip")
            },
            "coordinates": {
                "lat": None,
                "lon": None
            }
        },

        "work": {
            "type": "EXCAVATION",
            "description": raw.get("WorkDescription"),
            "start_date": raw.get("DigStartDate"),
            "end_date": raw.get("DigEndDate")
        },

        "utilities": [
            {
                "type": u.get("Type"),
                "owner": u.get("Owner"),
                "status": "NOTIFIED"
            }
            for u in raw.get("Utilities", [])
        ],

        "metadata": {
            "raw_blob_uri": blob_uri,
            "processed_at": now
        }
    }

    return normalized


def main(inputblob: func.InputStream):
    logging.info(f"Processing blob: {inputblob.name}")

    try:
        raw_bytes = inputblob.read()
        raw_json = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        logging.error(f"Failed to read/parse blob {inputblob.name}: {e}")
        raise

    blob_uri = f"{inputblob.uri}"

    normalized = normalize_ticket(raw_json, blob_uri)

    queue = QueueClient.from_connection_string(
        conn_str=STORAGE_CONN,
        queue_name=QUEUE_NAME
    )

    queue.send_message(
        json.dumps(normalized),
        visibility_timeout=0
    )

    logging.info(
        f"Normalized ticket enqueued: {normalized['ticket']['id']}"
    )
