import json
import logging
import os
import azure.functions as func
from azure.data.tables import TableClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    table = TableClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"],
        table_name="MissDigTickets"
    )

    entities = list(table.list_entities())

    logging.warning(f"[DEBUG] MissDigTickets row count: {len(entities)}")

    # Convert to plain JSON-safe objects
    rows = []
    for e in entities:
        row = dict(e)
        rows.append(row)

    return func.HttpResponse(
        json.dumps(rows, default=str),
        mimetype="application/json",
        status_code=200
    )
