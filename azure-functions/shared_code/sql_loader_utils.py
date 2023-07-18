import sys
import os
import logging as logger
import pandas as pd

from datetime import timedelta
from datetime import date

from urllib.parse import quote_plus
from sqlalchemy import create_engine, event

from azure.datalake.store import core, lib

insert_mode = 'append'

server_name = "localhost,1433"
db_name = "db_name"
username = "admin"
password = "password"

# Use AppRegistrations to generate the client_id, tenant_id, client_secret to access AzureDataLakeStorage
client_id = "azure-client-id"
tenant_id = "azure-tenant-id"
client_secret = "client_secret"
storage_account_name = "storage_account_name"
base_path = "reports_base_path"

last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
start_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
start_of_prev_month = str(start_of_prev_month)


def get_file_data(adl, base_path, report_name):
    folder_path = base_path + "/" + report_name
    report_file = report_name + "_" + start_of_prev_month

    file_path = folder_path + "/" + report_file
    logger.info("File path: {}".format(file_path))
    try:
        files = adl.listdir(folder_path)
        logger.info("Looking for file with path: {} in {}".format(file_path, files))
        file_to_process = None
        for file in files:
            if file_path in file:
                logger.info("Found file to process: {}".format(file))
                file_to_process = file

        if file_to_process is not None:
            logger.info("Processing file: {}".format(file_to_process))
            with adl.open(file_to_process, 'rb') as f:
                data = pd.read_excel(f, header='infer')
                data['lastupdateddate'] = start_of_prev_month

            return data

        return None
    except Exception as e:
        logger.error("Exception raised while reading file from data lake storage: {}, exiting".format(e))
        sys.exit(1)


def data_lake_storage_file_reader(report_name):
    logger.info("Reading files for report: {}".format(report_name))
    try:
        logger.info("DataLakeStorage credentials received for storage_account: {}".format(storage_account_name))
        token = lib.auth(tenant_id=tenant_id,
                         client_secret=client_secret,
                         client_id=client_id)
        adl = core.AzureDLFileSystem(token, store_name=storage_account_name)
        return get_file_data(adl, base_path, report_name)

    except Exception as e:
        logger.error("Exception raised while fetching file from data lake storage: {}, exiting".format(e))
        sys.exit(1)


def delete_latest_data(table_name, engine):
    try:
        delete_query = "delete from {0} where lastupdateddate >= '{1}'".format(table_name, start_of_prev_month)
        logger.info(delete_query)
        engine.execute(delete_query)
    except Exception as e:
        logger.error("Exception raised while deleting data: {}".format(e))
        return None


def send_df_to_sql(data, table_name, mode=insert_mode):
    server_name = os.environ['server_name']
    db_nam = os.environ['db_name']
    username = os.environ['user_name']
    password = os.environ['password']

    # azure sql connect tion string
    conn = 'Driver={ODBC Driver 17 for SQL Server};Server=' + server_name + ';Database=' + db_nam +';Uid=' + username +';Pwd=' + password +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    logger.info("Connection string: {}".format(conn))
    delete_latest_data(table_name, engine)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True
    logger.info("Inserting records into the table: {}".format(table_name))
    try:
        data.to_sql(table_name, engine, index=False, if_exists=mode, schema='dbo')
        logger.info("Successfully inserted {} records to SQL Server table: {}".format(data.shape[0], table_name))
    except Exception as e:
        logger.error("Exception raised while sending df to sql_server: {}, exiting".format(e))
        sys.exit(1)
