import logging
import json
import azure.functions as func
from azure.storage.queue import QueueClient
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function hit.")

    # Parse JSON safely
    try:
        body = req.get_json()
        logging.info(f"Received JSON: {body}")
    except Exception as e:
        raw = req.get_body().decode("utf-8", errors="ignore")
        logging.error(f"JSON parse failed: {e}")
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Convert JSON to string
    msg = json.dumps(body)

    # Send to Queue
    try:
        queue = QueueClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"],
            queue_name="missdig-tickets"
        )
        queue.create_queue()  # safe to call each time
        queue.send_message(msg)
        logging.info("Message queued successfully.")
    except Exception as e:
        logging.error(f"Queue write error: {e}")

    return func.HttpResponse(
        json.dumps({"status": "received", "queued": True}),
        mimetype="application/json",
        status_code=200
    )
