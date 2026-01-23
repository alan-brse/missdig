import logging
import os
from datetime import datetime, timezone

from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from openpyxl import Workbook

TABLE_NAME = "MissDigTickets"
EXPORT_CONTAINER = "exports"
EXPORT_BLOB_NAME = "missdig-tickets.xlsx"

STORAGE_CONN = os.environ["AzureWebJobsStorage"]

table_service = TableServiceClient.from_connection_string(STORAGE_CONN)
table_client = table_service.get_table_client(TABLE_NAME)

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
container_client = blob_service.get_container_client(EXPORT_CONTAINER)
container_client.create_container(exist_ok=True)


def main(mytimer):
    logging.info("Starting ticket Excel export")

    # 1️⃣ Query active tickets
    entities = table_client.query_entities(
        filter="IsActive eq true"
    )

    rows = list(entities)
    logging.info(f"Exporting {len(rows)} active tickets")

    # 2️⃣ Create workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Tickets"

    # 3️⃣ Header row
    headers = [
        "Ticket #",
        "Address",
        "Station Code",
        "Response Code",
        "Posr Comments",
        "Posr Short Description",
        "Response By",
        "Legal Start",
    ]
    ws.append(headers)

    # 4️⃣ Data rows
    for e in rows:
        ws.append([
            e.get("TicketNumber"),
            e.get("DigsiteAddress"),
            e.get("StationCode"),
            e.get("ResponseCode"),
            e.get("PosrComments"),
            e.get("PosrShortDescription"),
            e.get("LegalStartDate"),
        ])

    # Optional: freeze header row
    ws.freeze_panes = "A2"

    # 5️⃣ Save to temp file
    temp_path = "/tmp/missdig-tickets.xlsx"
    wb.save(temp_path)

    # 6️⃣ Upload to blob (overwrite)
    with open(temp_path, "rb") as f:
        container_client.upload_blob(
            name=EXPORT_BLOB_NAME,
            data=f,
            overwrite=True,
            content_type=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        )

    logging.info("Ticket Excel export complete")
