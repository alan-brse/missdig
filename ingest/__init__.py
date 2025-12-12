import logging
import json
import azure.functions as func

# Optional Azure Storage imports (these are safely wrapped)
try:
    from azure.storage.blob import BlobClient
    from azure.storage.queue import QueueClient
    from azure.data.tables import TableClient
    STORAGE_AVAILABLE = True
except Exception as e:
    STORAGE_AVAILABLE = False
    logging.warning(f"Azure Storage modules not loaded: {e}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Miss Dig ingest function received a request.")

    #
    # 1️⃣ SAFE JSON PARSE
    #
    try:
        body = req.get_json()
        logging.info(f"Parsed JSON body: {body}")
    except Exception as e:
        raw = req.get_body().decode("utf-8", errors="ignore")
        logging.error(f"JSON parse error: {e}")
        logging.error(f"Raw body: {raw}")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON", "raw": raw}),
            status_code=400,
            mimetype="application/json",
        )

    #
    # 2️⃣ OPTIONAL: SAVE TO STORAGE (Blob / Queue / Table)
    #
    if STORAGE_AVAILABLE:
        try:
            # Example: Save JSON to Blob storage (optional)
            # Replace with your actual connection strings / container names
            """
            blob = BlobClient.from_connection_string(
                conn_str="<AZURE_STORAGE_CONNECTION_STRING>",
                container_name="missdig-ingest",
                blob_name="latest.json"
            )
            blob.upload_blob(json.dumps(body), overwrite=True)
            logging.info("Saved payload to Blob storage.")
            """
            pass
        except Exception as storage_error:
            logging.error(f"Storage write failed: {storage_error}")

    #
    # 3️⃣ SUCCESS RESPONSE
    #
    return func.HttpResponse(
        json.dumps({"status": "received", "ok": True}),
        status_code=200,
        mimetype="application/json",
    )
