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

    rows = []

    for e in table.query_entities("PartitionKey ne ''"):
        # Parse members JSON safely
        members = []
        try:
            members = json.loads(e.get("Members", "[]"))
        except (json.JSONDecodeError, TypeError):
            members = []
        
        rows.append({
            "ticketNumber": e.get("TicketNumber"),
            "status": e.get("LastEventType"),        # temporary mapping
            "eventType": e.get("LastEventType"),
            "digsiteAddress": e.get("DigsiteAddress"),                    # not available yet
            "legalStartDate": e.get("LegalStartDate"),                    # not available yet
            "lastUpdated": e.get("LastEventAt"),
            "memberCount": e.get("MemberCount", 0),
            "responseCount": e.get("ResponseCount", 0),
            "members": members
        })

    return func.HttpResponse(
        json.dumps(rows, default=str),
        mimetype="application/json",
        status_code=200
    )


    return func.HttpResponse(
        json.dumps([dict(e) for e in entities], default=str),
        mimetype="application/json",
        status_code=200
    )
