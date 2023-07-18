import datetime
import sys
import os
import logging as logger

import azure.functions as func
sys.path.append(os.path.abspath(""))

from shared_code.customer_growth_loss_by_program_category_by_account import read_write_customer_growth_loss_by_program_category_by_account_report


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    read_write_customer_growth_loss_by_program_category_by_account_report()

    logger.info('Python timer trigger function ran at %s', utc_timestamp)
