import logging
import json
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.data.tables import TableServiceClient

TABLE_NAME = "MissDigTickets"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]

table_service = TableServiceClient.from_connection_string(STORAGE_CONN)
table_client = table_service.get_table_client(TABLE_NAME)


def main(blob: func.InputStream):
    logging.info(f"BlobToTable fired for {blob.name}")

    try:
        raw = json.loads(blob.read().decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid JSON in blob {blob.name}: {e}")
        return

    notification = raw.get("Notification")

    if notification:
        # MEMBER RESPONSE, ALL MEMBERS RESPONDED, etc.
        ticket_number = notification.get("TicketNumber")
        event_type = notification.get("Event")
        event_time = notification.get("TimeStamp")
    else:
        # TICKET CREATION (flat payload)
        ticket_number = raw.get("TicketNumber")
        event_type = raw.get("Event")
        event_time = raw.get("TimeStamp")


    if not ticket_number or not event_type:
        logging.error("Missing TicketNumber or Event — skipping")
        return

    now = datetime.now(timezone.utc).isoformat()

    entity = {
        "PartitionKey": ticket_number,
        "RowKey": "ticket",

        "TicketNumber": ticket_number,
        "LastEventType": event_type,
        "LastEventAt": event_time or now,
        "LastRawBlobUri": blob.uri,
    }

    try:
        table_client.upsert_entity(entity=entity, mode="MERGE")
        logging.info(f"Upserted base ticket row {ticket_number}")
    except Exception as e:
        logging.error(f"Table write failed for {ticket_number}: {e}")
