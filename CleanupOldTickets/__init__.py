import logging
import os
from datetime import datetime, timezone, timedelta

from azure.data.tables import TableServiceClient

TABLE_NAME = "MissDigTickets"
STORAGE_CONN = os.environ["AzureWebJobsStorage"]

table_service = TableServiceClient.from_connection_string(STORAGE_CONN)
table_client = table_service.get_table_client(TABLE_NAME)


def main(mytimer):
    logging.info("Starting cleanup of old tickets")

    # Current date for comparison
    current_date = datetime.now(timezone.utc)
    logging.info(f"Current date: {current_date.isoformat()}")

    # Query all tickets
    entities = table_client.query_entities(
        query_filter="PartitionKey ne ''"
    )

    deleted_count = 0
    skipped_count = 0

    for entity in entities:
        legal_start_date_str = entity.get("LegalStartDate")
        ticket_number = entity.get("TicketNumber")

        if not legal_start_date_str:
            logging.warning(
                f"Ticket {ticket_number} has no LegalStartDate, skipping"
            )
            skipped_count += 1
            continue

        try:
            # Parse the legal start date
            # Handle different date formats that might be in the data
            legal_start_date = None
            if isinstance(legal_start_date_str, datetime):
                legal_start_date = legal_start_date_str
            else:
                # Try parsing ISO format
                try:
                    legal_start_date = datetime.fromisoformat(
                        legal_start_date_str.replace('Z', '+00:00')
                    )
                except (ValueError, AttributeError):
                    # Try other common formats
                    for fmt in [
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                    ]:
                        try:
                            legal_start_date = datetime.strptime(
                                legal_start_date_str, fmt
                            )
                            # Make it timezone aware if not already
                            if legal_start_date.tzinfo is None:
                                legal_start_date = legal_start_date.replace(
                                    tzinfo=timezone.utc
                                )
                            break
                        except ValueError:
                            continue

            if not legal_start_date:
                logging.warning(
                    f"Could not parse LegalStartDate '{legal_start_date_str}' "
                    f"for ticket {ticket_number}, skipping"
                )
                skipped_count += 1
                continue

            # Make timezone aware if not already
            if legal_start_date.tzinfo is None:
                legal_start_date = legal_start_date.replace(tzinfo=timezone.utc)

            # Check if the ticket is older than legal start + 30 days
            ticket_expiry = legal_start_date + timedelta(days=30)

            if ticket_expiry < current_date:
                # Delete the ticket
                table_client.delete_entity(
                    partition_key=entity["PartitionKey"],
                    row_key=entity["RowKey"]
                )
                logging.info(
                    f"Deleted ticket {ticket_number} "
                    f"(Legal start: {legal_start_date.isoformat()}, "
                    f"Expiry: {ticket_expiry.isoformat()})"
                )
                deleted_count += 1
            else:
                skipped_count += 1

        except Exception as e:
            logging.error(
                f"Error processing ticket {ticket_number}: {e}"
            )
            skipped_count += 1
            continue

    logging.info(
        f"Cleanup complete. Deleted: {deleted_count}, Skipped: {skipped_count}"
    )
