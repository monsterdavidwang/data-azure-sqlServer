import datetime
import sys
import os
import logging as logger

import azure.functions as func
sys.path.append(os.path.abspath(""))

from shared_code.zyltus_call_data import prepare_send_data_to_sql

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    prepare_send_data_to_sql()
    logger.info('Python timer trigger function ran at %s', utc_timestamp)