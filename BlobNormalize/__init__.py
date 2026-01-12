import logging
import json
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.storage.blob import BlobServiceClient

STORAGE_CONN = os.environ["AzureWebJobsStorage"]

RAW_CONTAINER = "missdig-raw"
NORM_CONTAINER = "missdig-normalized"


def map_event_type(missdig_event: str) -> str:
    mapping = {
        "TICKET CREATION": "TICKET_CREATED",
        "TICKET UPDATE": "TICKET_UPDATED",
        "TICKET CANCELLED": "TICKET_CANCELLED",
        "MEMBER RESPONSE": "MEMBER_RESPONSE",
    }
    return mapping.get(missdig_event, "UNKNOWN")


def normalize_ticket(raw: dict, blob_uri: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    ticket_id = raw.get("TicketNumber")

    if not ticket_id:
        raise ValueError("Missing TicketNumber")

    normalized = {
        "schema_version": "1.0",

        "source": {
            "system": "MISS_DIG",
            "vendor_ticket_id": ticket_id,
            "notification_id": raw.get("NotificationId"),
        },

        "event": {
            "type": map_event_type(raw.get("Event")),
            "occurred_at": raw.get("TimeStamp"),
            "received_at": now,
        },

        "ticket": {
            "id": ticket_id,
            "status": "ACTIVE",
            "priority": "NORMAL",
            "legal_start_at": raw.get("LegalStartDateTime"),
        },

        "location": {
            "address": {
                "full": raw.get("DigsiteAddress"),
            }
        },

        "utilities": [
            {
                "station_code": m.get("StationCodeId"),
                "station_name": m.get("StationCodeName"),
                "response_code": m.get("ResponseCode"),
                "response_received_at": m.get("ResponseReceivedDateTime"),
                "comments": m.get("PosrComments"),
            }
            for m in raw.get("Members", [])
        ],

        "metadata": {
            "raw_blob_uri": blob_uri,
            "processed_at": now,
            "message_version": raw.get("MessageVersion"),
        },
    }

    return normalized



def main(inputblob: func.InputStream):
    logging.info(f"BlobNormalize fired for: {inputblob.name} ({inputblob.length} bytes)")

    # 1) Read raw blob
    try:
        raw_bytes = inputblob.read()
        raw_json = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        logging.error(f"Failed to read/parse blob {inputblob.name}: {e}")
        return  # don't poison

    # 2) Normalize
    try:
        normalized = normalize_ticket(raw_json, getattr(inputblob, "uri", inputblob.name))
        ticket_id = normalized["ticket"]["id"]
        if not ticket_id:
            logging.error("Missing ticket id after normalization; skipping")
            return
    except Exception as e:
        logging.error(f"Normalization failed for {inputblob.name}: {e}")
        return  # don't poison

    # 3) Write normalized blob (canonical output)
    try:
        now = datetime.now(timezone.utc)
        out_path = f"{now:%Y/%m/%d}/{ticket_id}.json"

        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
        out_container = blob_service.get_container_client(NORM_CONTAINER)
        try:
            out_container.create_container()
        except Exception:
            pass

        out_blob = out_container.get_blob_client(out_path)
        out_blob.upload_blob(
            json.dumps(normalized, ensure_ascii=False),
            overwrite=True,
            content_type="application/json",
        )

        logging.info(f"Wrote normalized blob: {NORM_CONTAINER}/{out_path}")
    except Exception as e:
        logging.error(f"Failed writing normalized blob for {ticket_id}: {e}")
        return
