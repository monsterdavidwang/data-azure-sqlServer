import sys
import numpy as np
import logging as logger

from .sql_loader_utils import send_df_to_sql
from .sql_loader_utils import data_lake_storage_file_reader


def read_write_completed_wo_totals_by_primary_employee_by_service():
    report_name = "completed_wo_totals_by_primary_employee_by_service"
    data = data_lake_storage_file_reader(report_name)
    if data is None:
        logger.error("Did not receive an instance of pandas df, exiting")
        sys.exit(1)

    logger.info("Data: {}".format(data))
    logger.info("Total number of rows: {}".format(data.count()))

    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna(0)

    data['completedamount'] = data['completedamount'].astype(float)
    data['prodamount'] = data['prodamount'].astype(float)
    data['commissionamount'] = data['commissionamount'].astype(float)

    column_names = ','.join(data.columns)

    logger.info("Column names: {}".format(column_names))
    table_name = 'completed_wo_totals_by_primary_employee_by_service'
    send_df_to_sql(data, table_name)
