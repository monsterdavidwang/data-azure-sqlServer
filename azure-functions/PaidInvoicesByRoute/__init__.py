import datetime
import sys
import os
import logging as logger

import azure.functions as func
sys.path.append(os.path.abspath(""))

from shared_code.paid_invoices_by_route import read_write_paid_invoices_by_route_report


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    read_write_paid_invoices_by_route_report()
    logger.info('Python timer trigger function ran at %s', utc_timestamp)
