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

    # Extract first meaningful member response
    station_code = None
    response_code = None
    posr_comments = None
    posr_short_description = None
    response_by = None

    for m in members:
        # pick the first member that has ANY meaningful response data
        if (
            m.get("ResponseCode")
            or m.get("PosrComments")
            or m.get("PosrShortDescription")
            or m.get("ResponseBy")
        ):
            station_code = m.get("StationCodeId")
            response_code = m.get("ResponseCode")
            posr_comments = m.get("PosrComments")
            posr_short_description = m.get("PosrShortDescription")
            response_by = m.get("ResponseBy")
            break

    entity = {
        "PartitionKey": ticket_number,
        "RowKey": "ticket",

        "TicketNumber": ticket_number,

        "DigsiteAddress": notification.get("DigsiteAddress"),
        "LegalStartDate": notification.get("LegalStartDateTime"),

        "StationCode": station_code,
        "ResponseCode": response_code,
        "PosrComments": posr_comments,
        "PosrShortDescription": posr_short_description,
        "ResponseBy": response_by,

        "LastEventType": event_type,
        "LastEventAt": event_time,
        "LastRawBlobUri": blob.uri,
    }

    try:
        table_client.upsert_entity(entity)
        logging.info(f"Upserted base ticket row {ticket_number}")
    except Exception as e:
        logging.error(f"Table write failed for {ticket_number}: {e}")
