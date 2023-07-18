# import pandas as pd
import numpy as np
import logging as logger

from .sql_loader_utils import send_df_to_sql
from .sql_loader_utils import data_lake_storage_file_reader


def read_write_paid_invoices_by_route_report():
    report_name = "paid_invoices_by_route"
    data = data_lake_storage_file_reader(report_name)
    if data is None:
        logger.error("Did not receive an instance of pandas df, exiting")
        exit(1)

    logger.info("Data: {}".format(data))
    logger.info("Total number of rows: {}".format(data.count()))

    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna(0)

    #data = data.rename(columns={'employee#': 'employee_num'})
    data['invoicedue'] = data['invoicedue'].astype(float)
    data['paid'] = data['paid'].astype(float)
    data['balance'] = data['balance'].astype(float)

    table_name = 'paid_invoices_by_route'
    send_df_to_sql(data, table_name)
