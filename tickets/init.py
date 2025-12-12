import azure.functions as func
from azure.data.tables import TableServiceClient
import os, json

def main(req: func.HttpRequest) -> func.HttpResponse:
    status = (req.params.get("status") or "NEW").upper()
    county = (req.params.get("county") or "UNKNOWN").title()
    limit  = int(req.params.get("limit") or "200")

    tsc = TableServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    table = tsc.get_table_client("Tickets")

    pk = f"{status}#{county}"
    # Table queries don't sort server-side; we used reverse timestamp in RowKey so scanning ascending returns newest first.
    entities = table.query_entities(f"PartitionKey eq '{pk}'")
    rows = []
    for e in entities:
        rows.append({
            "ticketId": e.get("ticketId"),
            "status": e.get("status"),
            "county": e.get("county"),
            "receivedAtUtc": e.get("receivedAtUtc")
        })
        if len(rows) >= limit:
            break

    return func.HttpResponse(json.dumps(rows), headers={"Content-Type":"application/json"})

