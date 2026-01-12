import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    path = os.path.join(os.path.dirname(__file__), "tickets.html")

    with open(path, "r", encoding="utf-8") as f:
        html = f.read()

    return func.HttpResponse(
        html,
        mimetype="text/html"
    )
