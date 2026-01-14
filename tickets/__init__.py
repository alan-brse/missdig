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

    entities = list(table.query_entities("PartitionKey ne ''"))

    logging.warning(f"TICKETS RETURNING {len(entities)} ROWS")

    return func.HttpResponse(
        json.dumps([dict(e) for e in entities], default=str),
        mimetype="application/json",
        status_code=200
    )
