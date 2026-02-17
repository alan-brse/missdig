import logging
import json
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.data.tables import TableServiceClient

TABLE_NAME = "MissDigTickets"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]

table_service = TableServiceClient.from_connection_string(STORAGE_CONN)
table_service.create_table_if_not_exists(TABLE_NAME)
table_client = table_service.get_table_client(TABLE_NAME)


def main(blob: func.InputStream):
    logging.info(f"BlobToTable fired for {blob.name}")

    try:
        raw = json.loads(blob.read().decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid JSON in blob {blob.name}: {e}")
        return

    notification = raw.get("Notification", {})
    members = notification.get("Members", [])
    ticket_number = notification.get("TicketNumber")
    event_type = raw.get("Event")
    event_time = raw.get("TimeStamp")

    if not ticket_number or not event_type:
        logging.error(
            f"Missing required fields: "
            f"TicketNumber={ticket_number}, Event={event_type}"
        )
        return

    if not ticket_number or not event_type:
        logging.error("Missing TicketNumber or Event — skipping")
        return

    now = datetime.now(timezone.utc).isoformat()

    # Calculate member statistics
    member_count = len(members)
    response_count = sum(1 for m in members if m.get("ResponseCode"))

    # Build base entity with always-updated fields
    entity = {
        "PartitionKey": ticket_number,
        "RowKey": "ticket",

        "TicketNumber": ticket_number,

        "DigsiteAddress": notification.get("DigsiteAddress"),
        "LegalStartDate": notification.get("LegalStartDateTime"),

        "Members": json.dumps(members),
        "MemberCount": member_count,
        "ResponseCount": response_count,

        "LastEventAt": event_time,
        "LastRawBlobUri": blob.uri,
    }

    # Only update LastEventType for TICKET_CREATED events
    # This prevents member response events from overwriting the ticket creation event
    if event_type == "TICKET_CREATED":
        entity["LastEventType"] = event_type

    try:
        table_client.upsert_entity(entity)
        logging.info(f"Upserted base ticket row {ticket_number}")
    except Exception as e:
        logging.error(f"Table write failed for {ticket_number}: {e}")
