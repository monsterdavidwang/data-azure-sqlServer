import datetime
import logging as logger
import os
import sys

import azure.functions as func
sys.path.append(os.path.abspath(""))

from shared_code.completed_wo_totals_by_primary_employee_by_service import read_write_completed_wo_totals_by_primary_employee_by_service


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    read_write_completed_wo_totals_by_primary_employee_by_service()

    logger.info('Python timer trigger function ran at %s', utc_timestamp)
