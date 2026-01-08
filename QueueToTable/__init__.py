import logging
import json
import os

import azure.functions as func
from azure.data.tables import TableServiceClient


TABLE_NAME = "NormalizedTickets"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]


def main(msg: func.QueueMessage):
    logging.info("QueueToTable triggered")

    try:
        event = json.loads(msg.get_body().decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid queue message: {e}")
        raise

    # Idempotency keys
    partition_key = event["source"]["system"]
    row_key = f"{event['source']['vendor_ticket_id']}::{event['event']['type']}"

    entity = {
        "PartitionKey": partition_key,
        "RowKey": row_key,

        # Core searchable fields
        "ticket_id": event["ticket"]["id"],
        "event_type": event["event"]["type"],
        "event_time": event["event"]["occurred_at"],
        "received_at": event["event"]["received_at"],
        "status": event["ticket"]["status"],
        "expires_at": event["ticket"]["expires_at"],

        # Flattened helpers
        "city": event["location"]["address"]["city"],
        "state": event["location"]["address"]["state"],

        # Full payload for replay / read models
        "payload": json.dumps(event)
    }

    service = TableServiceClient.from_connection_string(STORAGE_CONN)
    table = service.get_table_client(TABLE_NAME)

    try:
        table.create_table()
    except Exception:
        pass  # table already exists

    table.upsert_entity(
        entity=entity,
        mode="MERGE"
    )

    logging.info(
        f"Upserted ticket {entity['ticket_id']} "
        f"event {entity['event_type']}"
    )
