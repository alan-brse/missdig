import logging
import json
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.data.tables import TableServiceClient, UpdateMode

TABLE_NAME = "MissDigTickets"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]

table_service = TableServiceClient.from_connection_string(STORAGE_CONN)
table_service.create_table_if_not_exists(TABLE_NAME)
table_client = table_service.get_table_client(TABLE_NAME)


def main(blob: func.InputStream):
    """
    Process incoming Miss Dig event blobs and update the tickets table.
    
    Expected behavior:
    - TICKET_CREATED events: Initialize ticket with LastEventType set to TICKET_CREATED
    - MEMBER_RESPONSE events: Update member data, preserve existing LastEventType
    - Other events: Update relevant fields, preserve existing LastEventType
    
    Assumptions:
    - TICKET_CREATED events arrive before MEMBER_RESPONSE events for a ticket
    - Events contain the current full state of members (not deltas)
    - MERGE mode is used to preserve LastEventType while updating other fields
    """
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

    # Only set LastEventType for TICKET_CREATED events to preserve the creation event type.
    # This assumes TICKET_CREATED is always the first event for a ticket.
    # MEMBER_RESPONSE and other events will not overwrite this field due to MERGE mode.
    if event_type == "TICKET_CREATED":
        entity["LastEventType"] = event_type

    try:
        # Use MERGE mode to preserve existing fields not included in this update.
        # Most importantly, this preserves LastEventType when MEMBER_RESPONSE events update member data.
        table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
        logging.info(f"Upserted base ticket row {ticket_number}")
    except Exception as e:
        logging.error(f"Table write failed for {ticket_number}: {e}")
