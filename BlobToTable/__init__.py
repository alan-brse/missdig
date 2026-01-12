import logging
import json
import os
from datetime import datetime

import azure.functions as func
from azure.data.tables import TableServiceClient

TABLE_NAME = "missdigtickets"
TABLE_CONN = os.environ["AzureWebJobsStorage"]


def main(blob: func.InputStream):
    logging.info(
        f"BlobToTable fired for {blob.name}, size={blob.length} bytes"
    )

    # Parse normalized blob
    try:
        data = json.loads(blob.read())
    except Exception as e:
        logging.error(f"Invalid JSON in normalized blob: {e}")
        return

    # --- Extract required fields ---
    ticket = data["ticket"]
    location = ticket["location"]
    work = ticket.get("work", {})

    ticket_number = ticket["ticket_number"]

    state = location.get("state", "UNK")
    county = location.get("county", "UNK")

    partition_key = f"{state}|{county}"
    row_key = ticket_number

    # --- Build Table entity ---
    entity = {
        "PartitionKey": partition_key,
        "RowKey": row_key,

        "schema_version": data.get("schema_version"),

        "ticket_number": ticket_number,
        "request_type": ticket.get("request_type"),

        "event_type": data.get("event_type"),
        "event_timestamp": data.get("event_timestamp"),

        "state": state,
        "county": county,
        "city": location.get("city"),

        "street": location.get("street"),
        "cross_street": location.get("cross_street"),

        "work_start_date": work.get("start_date"),
        "work_end_date": work.get("end_date"),

        "excavator_company": ticket.get("excavator", {}).get("company"),

        "utilities_affected": len(ticket.get("utilities", [])),
        "positive_responses": sum(
            1 for u in ticket.get("utilities", [])
            if u.get("status") == "POSITIVE"
        ),

        "normalized_blob_path": blob.name,
        "last_updated_utc": datetime.utcnow().isoformat()
    }

    # --- Write to Table ---
    try:
        service = TableServiceClient.from_connection_string(TABLE_CONN)
        table = service.create_table_if_not_exists(TABLE_NAME)

        table.upsert_entity(entity=entity)
        logging.info(f"Upserted ticket {ticket_number}")

    except Exception as e:
        logging.error(f"Table write failed for {ticket_number}: {e}")

