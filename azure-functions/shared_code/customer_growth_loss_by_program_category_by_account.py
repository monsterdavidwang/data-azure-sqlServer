import numpy as np
import logging as logger

from .sql_loader_utils import send_df_to_sql
from .sql_loader_utils import data_lake_storage_file_reader


def read_write_customer_growth_loss_by_program_category_by_account_report():
    report_name = "customer_growth_loss_by_program_category_by_account"
    data = data_lake_storage_file_reader(report_name)
    if data is None:
        logger.error("Did not receive an instance of pandas df, exiting")
        exit(1)

    logger.info("Data: {}".format(data))
    logger.info("Total number of rows: {}".format(data.count()))

    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna(0)

    data['prgcountstart'] = data['prgcountstart'].astype(float)
    data['accountcountstart'] = data['accountcountstart'].astype(float)
    data['annualstart'] = data['annualstart'].astype(float)
    data['prgcountbetween'] = data['prgcountbetween'].astype(float)
    data['accountbetween'] = data['accountbetween'].astype(float)
    data['annualbetween'] = data['annualbetween'].astype(float)
    data['prgcountcancelbetween'] = data['prgcountcancelbetween'].astype(float)
    data['accountcountcancelbetween'] = data['accountcountcancelbetween'].astype(float)
    data['annualcancelbetween'] = data['annualcancelbetween'].astype(float)
    data['prgcountend'] = data['prgcountend'].astype(float)
    data['accountcountend'] = data['accountcountend'].astype(float)
    data['annualend'] = data['annualend'].astype(float)

    column_names = ','.join(data.columns)

    logger.info("Column names: {}".format(column_names))

    table_name = 'customer_growth_loss_by_program_category_by_account'
    send_df_to_sql(data, table_name)
