import logging
import json
import azure.functions as func
from azure.data.tables import TableClient
import os

def main(msg: func.QueueMessage) -> None:
    logging.info("tickets() processor running.")
    
    body = msg.get_body().decode("utf-8")
    data = json.loads(body)
    logging.info(f"Processing ticket: {data}")

    # Insert record into Table Storage
    try:
        table = TableClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            table_name="MissDigTickets"
        )
        table.create_table_if_not_exists()

        entity = {
            "PartitionKey": "MissDig",
            "RowKey": data.get("ticket_id", msg.id),
            "Payload": body
        }

        table.upsert_entity(entity)
        logging.info("Stored ticket in Table Storage.")
    except Exception as e:
        logging.error(f"Table insert failed: {e}")
