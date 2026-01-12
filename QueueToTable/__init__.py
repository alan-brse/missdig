import logging
import azure.functions as func

def main(msg: func.QueueMessage):
    logging.warning(f"QueueToTable FIRED. Message: {msg.get_body().decode()}")
