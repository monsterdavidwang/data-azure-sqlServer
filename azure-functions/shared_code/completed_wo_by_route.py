# import pandas as pd
import numpy as np
import logging as logger

from .sql_loader_utils import send_df_to_sql
from .sql_loader_utils import data_lake_storage_file_reader


def read_write_completed_wo_report():

    report_name = "completed_wo_by_route"
    data = data_lake_storage_file_reader(report_name)
    if data is None:
        logger.error("Did not receive an instance of pandas df, exiting")
        return None

    logger.info("Data: {}".format(data))
    logger.info("Total number of rows: {}".format(data.count()))

    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna(0)

    data['amountbilled'] = data['amountbilled'].astype(float)
    data['prodvalue'] = data['prodvalue'].astype(float)

    column_names = ','.join(data.columns)

    logger.info("Column names: {}".format(column_names))

    table_name = 'completed_wo_by_route'
    send_df_to_sql(data, table_name)
