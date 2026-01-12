import json
import os
import logging
import azure.functions as func
from azure.data.tables import TableServiceClient

TABLE_NAME = "MissDigTickets"
CONN_STR = os.environ["AzureWebJobsStorage"]


def derive_status(event_type: str) -> str:
    return {
        "TICKET CREATION": "OPEN",
        "MEMBER RESPONSE": "RESPONSES_IN_PROGRESS",
        "ALL MEMBERS RESPONDED": "READY",
        "LEGAL START DATE": "LEGAL_START"
    }.get(event_type, "UNKNOWN")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("GET /tickets called")

    service = TableServiceClient.from_connection_string(CONN_STR)
    table = service.get_table_client(TABLE_NAME)

    rows = table.query_entities("RowKey eq 'ticket'")
    tickets = []

    for e in rows:
        event_type = e.get("event_type")

        tickets.append({
            "ticketNumber": e.get("ticket_number"),
            "status": derive_status(event_type),
            "eventType": event_type,
            "digsiteAddress": e.get("DigsiteAddress"),
            "legalStartDate": e.get("LegalStartDateTime"),
            "lastUpdated": e.get("last_updated_utc")
        })

    return func.HttpResponse(
        json.dumps(tickets),
        mimetype="application/json",
        status_code=200
    )
