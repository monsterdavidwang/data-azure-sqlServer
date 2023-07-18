import datetime
import sys
import os
import logging as logger

import azure.functions as func
sys.path.append(os.path.abspath(""))

from shared_code.completed_wo_by_route import read_write_completed_wo_report


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    logger.info("Triggering load for report completed_wo_by_route")
    read_write_completed_wo_report()

    logger.info('Python timer trigger function ran at %s', utc_timestamp)

