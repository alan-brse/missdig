import json
import logging
import os
import traceback
import azure.functions as func
from azure.data.tables import TableClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.warning("TICKETS FUNCTION STARTED")

        table = TableClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            table_name="MissDigTickets"
        )

        logging.warning(f"TABLE ENDPOINT: {table._client._endpoint}")

        entities = list(table.query_entities("PartitionKey ne ''"))

        logging.warning(f"ROW COUNT: {len(entities)}")

        return func.HttpResponse(
            json.dumps([dict(e) for e in entities], default=str),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error("TICKETS FUNCTION FAILED")
        logging.error(str(e))
        logging.error(traceback.format_exc())

        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
