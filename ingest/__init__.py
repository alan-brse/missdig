import azure.functions as func
from azure.storage.blob import BlobClient
from azure.data.tables import TableServiceClient, UpdateMode
import os, json, hmac, hashlib, datetime

def _constant_time_equals(a,b):
    return hmac.compare_digest(a, b)

def main(req: func.HttpRequest) -> func.HttpResponse:
    secret = os.environ.get("HMAC_SHARED_SECRET", "")
    if not secret:
        return func.HttpResponse("Missing secret", status_code=500)

    # raw body for signature
    body = req.get_body()
    sig  = req.headers.get("x-missdig-signature", "")
    calc = hmac.new(bytes.fromhex(secret), body, hashlib.sha256).hexdigest()
    if not sig or not _constant_time_equals(calc, sig):
        return func.HttpResponse("Invalid signature", status_code=401)

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return func.HttpResponse("Bad JSON", status_code=400)

    # required fields (adapt to real payload)
    ticket_id = payload.get("id") or payload.get("ticketId")
    status    = (payload.get("status") or "NEW").upper()
    county    = (payload.get("county") or "UNKNOWN").title()
    received  = payload.get("receivedAt") or datetime.datetime.utcnow().isoformat()+"Z"
    if not ticket_id:
        return func.HttpResponse("Missing ticket id", status_code=400)

    # 1) store raw to Blob: raw/YYYY/MM/dd/<ticketId>.json
    today = datetime.datetime.utcnow()
    blob_name = f"raw/{today:%Y/%m/%d}/{ticket_id}.json"
    # use connection via managed identity (no conn string needed)
    blob = BlobClient(
        account_url=f"https://{os.environ['AzureWebJobsStorage'].split(';')[1].split('=')[1]}.blob.core.windows.net",
        container_name=blob_name.split('/')[0],
        blob_name="/".join(blob_name.split('/')[1:])
    )
    # simpler: use connection string from AzureWebJobsStorage
    from azure.storage.blob import BlobServiceClient
    bsc = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    bc  = bsc.get_container_client("raw").get_blob_client(f"{today:%Y/%m/%d}/{ticket_id}.json")
    bc.upload_blob(body, overwrite=True)

    # 2) upsert compact row to Table: PartitionKey=status#county, RowKey=reverseTimestamp or ticketId
    tsc = TableServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    table = tsc.get_table_client("Tickets")
    # reverse timestamp for latest-first scans (optional)
    epoch_ms = int(today.timestamp() * 1000)
    rev = 9999999999999 - epoch_ms
    entity = {
        "PartitionKey": f"{status}#{county}",
        "RowKey": str(rev),
        "ticketId": ticket_id,
        "status": status,
        "county": county,
        "receivedAtUtc": received
    }
    table.upsert_entity(mode=UpdateMode.MERGE, entity=entity)

    return func.HttpResponse("OK", status_code=200)

