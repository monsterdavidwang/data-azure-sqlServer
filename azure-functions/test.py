import datetime
import sys
import os
import logging as logger

import numpy as np
import azure.functions as func
#sys.path.append(os.path.abspath(""))

import sys
import os
import configparser

import pandas as pd

from datetime import timedelta
from datetime import date

from urllib.parse import quote_plus
from sqlalchemy import create_engine, event

from azure.datalake.store import core, lib

insert_mode = 'append'
default_config_file_path = "stw_app_config.properties"
storage_section = 'DataLakeStorageSection'
db_section = 'DatabaseSection'
config = configparser.ConfigParser()
# Reads the config file passed and set the required configuration in environment variables

server_name="tcp:stwt-fin-a.database.windows.net,1433"
db_name="servsuite"
username="stewarts-admin"
password="Stewart@5715"

client_id="4bcde214-96b5-44fc-862f-f9565d6bf786"
tenant_id="71f78d41-2b28-4f10-b6b8-59078df9a99e"
client_secret="-571-_op19KSKOY_UKbH7.qn.dy0HPBtg-"
storage_account_name="stwtfin"
base_path="financial_reports"


def get_file_data(adl, base_path, report_name):
    date_str = date.today()
    start_of_week = date_str - timedelta(days=date_str.weekday() + 1) #Sunday
    start_of_week = str(start_of_week)
    folder_path = base_path + "/" + report_name
    report_file = report_name + "_" + start_of_week

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

            return data

    except Exception as e:
        logger.error("Exception raised while reading file from data lake storage: {}, exiting".format(e))
        sys.exit(1)


def data_lake_storage_file_reader(report_name, config_file=default_config_file_path):
    logger.info("Reading files for report: {}".format(report_name))
    logger.info("Reading ConfigFile: {}".format(config_file))
    config.read(config_file)
    logger.info("Config sections: {}, Looking for {}".format(config.sections(), storage_section))
    try:
        # client_id = config[storage_section]['client_id']
        # client_secret = config[storage_section]['client_secret']
        # tenant_id = config[storage_section]['tenant_id']
        # storage_account_name = config[storage_section]['storage_account_name']
        # base_path = config[storage_section]['base_path']
        logger.info("DataLakeStorage credentials received for storage_account: {}".format(storage_account_name))
        token = lib.auth(tenant_id=tenant_id,
                         client_secret=client_secret,
                         client_id=client_id)
        adl = core.AzureDLFileSystem(token, store_name=storage_account_name)
        return get_file_data(adl, base_path, report_name)

    except Exception as e:
        logger.error("Exception raised while fetching file from data lake storage: {}, exiting".format(e))
        sys.exit(1)


def config_reader(config_file=default_config_file_path):
    logger.info("Reading config file: {}".format(config_file))
    config.read(config_file)
    logger.info("Config sections: {}, Looking for {}".format(config.sections(), db_section))

    try:
        # server_name = config['DatabaseSection']['server_name']
        # db_name = config[db_section]['db_name']
        # username = config[db_section]['username']
        # password = config[db_section]['password']

        logger.info("Server config read: "
                    "ServerName: {}, "
                    "DB_Name: {}, "
                    "Username: {}, "
                    "Password: {}".format(server_name,
                                          db_name,
                                          username,
                                          password))

        os.environ['server_name'] = server_name
        os.environ['db_name'] = db_name
        os.environ['user_name'] = username
        os.environ['password'] = password

    except Exception as e:
        logger.error("Error reading config file, exiting")
        sys.exit(1)


def send_df_to_sql(data, table_name, config_path=default_config_file_path):
    config_reader(config_path)
    server_name = os.environ['server_name']
    db_nam = os.environ['db_name']
    username = os.environ['user_name']
    password = os.environ['password']

    # azure sql connect tion string
    conn = 'Driver={ODBC Driver 17 for SQL Server};Server='+ server_name + ';Database=' + db_nam +';Uid=' + username +';Pwd=' + password +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    logger.info("Connection string: {}".format(conn))

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True
    logger.info("Inserting records into the table: {}".format(table_name))
    try:
        data.to_sql(table_name, engine, index=False, if_exists=insert_mode, schema='dbo')
        logger.info("Successfully inserted {} records to SQL Server table: {}".format(data.shape[0], table_name))
    except Exception as e:
        logger.error("Exception raised while sending df to sql_server: {}, exiting".format(e))
        sys.exit(1)


def read_write_completed_wo_report():

    report_name = "completed_wo_by_route"
    data = data_lake_storage_file_reader(report_name)
    if data is None:
        logger.error("Did not receive an instance of pandas df, exiting")
        sys.exit(1)


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

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    logger.info("Triggering load for report completed_wo_by_route")
    read_write_completed_wo_report()
    if mytimer.past_due:
        logger.info('The timer is past due!')

    logger.info('Python timer trigger function ran at %s', utc_timestamp)
