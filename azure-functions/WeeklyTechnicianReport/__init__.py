from datetime import datetime, date, timedelta
import logging
import time
import azure.functions as func
import pandas as pd
from azure.storage.filedatalake import FileSystemClient
import pyodbc
import urllib
from urllib.parse import quote_plus
from sqlalchemy import create_engine

storage_account_name = 'storage_account_name'
client_id = 'client_id'
client_secret = 'client_secret'
tenant_id = 'tenant_id'
driver = "{ODBC Driver 17 for SQL Server}"
server = "localhost"
database = "db_name"
user = "admin"
password = "password"

complete_work_update_query = """
update  complete_work_dump
set service = (
   select
           case
               when esm.event is not null then esm.service_type
               when esm.event is null and lower(cwd.eventname) like '%lawn%' then  'Lawn'
               when esm.event is null and lower(cwd.eventname) like '%flowerbed%' then  'Flowerbed'
               when esm.event is null and lower(cwd.eventname) like '%pest%' then  'Pest'
               when esm.event is null and lower(cwd.eventname) like '%tree%' then  'Tree'
               else 'other' end
   from complete_work_dump as cwd
   left join event_service_mapping as esm on cwd.eventname  = esm.event
   where complete_work_dump.woheaderid = cwd.woheaderid and complete_work_dump.lastupdateddate = cwd.lastupdateddate
   )
where service is null"""

material_work_update_query = """
update  material_report_dump
set service = (
   select distinct
                   case
                       when esm.event is not null then lower(esm.service_type)
                       when esm.event is null and lower(mr.eventname) like '%lawn%' then  'Lawn'
                       when esm.event is null and lower(mr.eventname) like '%flowerbed%' then  'Flowerbed'
                       when esm.event is null and lower(mr.eventname) like '%pest%' then  'Pest'
                       when esm.event is null and lower(mr.eventname) like '%tree%' then  'Tree'
                    else 'other' end
   from material_report_dump as mr
   left join event_service_mapping as esm on mr.eventname  = esm.event
   where material_report_dump.wonumber = mr.wonumber 
   and material_report_dump.eventname = mr.eventname 
   and material_report_dump.itemdescription = mr.itemdescription 
   and material_report_dump.useunit = mr.useunit
   and material_report_dump.lastupdateddate = mr.lastupdateddate
   )
where service is null"""


def insert_into_db(df, table_name, delete_date):
    conn = f"""Driver={driver};Server=tcp:{server},1433;Database={database}; Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=True)
    logging.info(table_name)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
    delete_query = "delete from {0} where lastupdateddate >= '{1}'".format(table_name, delete_date)
    logging.info(delete_query)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
    engine.execute(delete_query)
    df.to_sql(table_name, con=engine, index=False, chunksize=100, method="multi", if_exists='append', schema='dbo')
    logging.info(df.head(5))
    time.sleep(5)


def update_table(query):
    conn = pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    time.sleep(5)
    logging.info('Python timer trigger function ran at %s', datetime.now())


def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function start at %s', datetime.now())
    file_system_client = FileSystemClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=technicianreports;AccountKey=2j2FGV2HP91RKblD/zjFhBZ9DtNEUMh8Dgk6L2l7YwWZufShFztc9V4+ozjlAECXh6gzZgcHFY0MLW6k2j0TXw==;EndpointSuffix=core.windows.net",
        file_system_name="technicianreports")
    sas_token = "sp=r&st=2021-05-30T17:09:12Z&se=2026-05-31T01:09:12Z&spr=https&sv=2020-02-10&sr=c&sig=MHM0v%2BvbZ9MIyDLeD61jF5swdCdpaFT0WR6LuwZTWGk%3D"
    date_str = date.today()
    start_of_week = date_str - timedelta(days=date_str.weekday() + 8)  # Sunday
    end_of_week = start_of_week + timedelta(days=6)
    delete_date = start_of_week - timedelta(days=0)
    start_of_week = str(start_of_week)
    paths = file_system_client.get_paths()
    complete_order_file = 'stwtfin/complete_work_order_' + start_of_week + '.xlsx'
    material_work_file = 'stwtfin/material_report_' + start_of_week + '.xlsx'
    work_hours_file = 'stwtfin/work_hours_' + start_of_week + '.xlsx'
    for path in paths:
       # logging.info(path.name, path, complete_order_file, material_work_file, work_hours_file)
        if 'stwtfin' in path.name and complete_order_file in path.name:
            file = 'https://technicianreports.blob.core.windows.net/technicianreports/stwtfin/complete_work_order_' + start_of_week + '.xlsx?' + str(
                sas_token)
            df = pd.read_excel(file)
            df2 = df[
                ['accountid', 'woheaderid', 'routename', 'description', 'eventname', 'completeddate', 'measurement',
                 'employeenumber', 'empfirstname', 'emplastname', 'completedamount', 'prodamount', 'emailaddress']]
            df2['lastupdateddate'] = str(start_of_week)
            insert_into_db(df2, 'complete_work_dump', delete_date)
            update_table(complete_work_update_query)
        if 'stwtfin' in path.name and material_work_file in path.name:
            df = pd.read_excel(
                'https://technicianreports.blob.core.windows.net/technicianreports/stwtfin/material_report_' + start_of_week + '.xlsx?' + str(
                    sas_token))
            df2 = df[['completeddate', 'routename', 'employee', 'accountnum', 'wonumber', 'name', 'eventname',
                      'itemdescription', 'materialquantity', 'useunit']]
            df2['lastupdateddate'] = str(start_of_week)
            insert_into_db(df2, 'material_report_dump', str(delete_date))
            update_table(material_work_update_query)

        if 'stwtfin' in path.name and work_hours_file in path.name:
            df = pd.read_excel(
                'https://technicianreports.blob.core.windows.net/technicianreports/stwtfin/work_hours_' + start_of_week + '.xlsx?' + str(
                    sas_token))
            df.columns = df.iloc[1]
            df = df[2:]
            df.drop(df.tail(1).index, inplace=True)
            df.drop(df.columns[[1, 3, 4]], axis=1, inplace=True)
            col = list(df.columns)
            col[0] = 'Code'
            col[1] = 'Name'
            col[2] = 'SUN'
            col[3] = 'MO'
            df.columns = col
            df2 = df[
                ['Code', 'Name', 'SUN', 'MO', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'REG', 'OT1', 'OT2', 'OT3', 'UNPAID',
                 'TOTAL']]
            df2 = df2.rename({'Code': 'emp_code', 'Name': 'name', 'SUN': 'sun', 'MO': 'mon', 'TUE': 'tue', 'WED': 'wed',
                              'THU': 'thur', 'FRI': 'fri', 'SAT': 'sat', 'REG': 'reg', 'OT1': 'ot1', 'OT2': 'ot2',
                              'OT3': 'ot3', 'UNPAID': 'unpaid', 'TOTAL': 'total'}, axis=1)
            df2.fillna(0, inplace=True)
            df2['name'] = df2.name.astype(str)
            logging.info(df2.head(5))
            df2['end_of_week'] = str(end_of_week)
            df2['start_of_week'] = str(start_of_week)
            df2['lastupdateddate'] = str(start_of_week)
            df2['sun'] = df2['sun'].astype(float)
            df2['mon'] = df2['mon'].astype(float)
            df2['tue'] = df2['tue'].astype(float)
            df2['wed'] = df2['wed'].astype(float)
            df2['thur'] = df2['thur'].astype(float)
            df2['fri'] = df2['fri'].astype(float)
            df2['sat'] = df2['sat'].astype(float)
            df2['reg'] = df2['reg'].astype(float)
            df2['ot1'] = df2['ot1'].astype(float)
            df2['ot2'] = df2['ot2'].astype(float)
            df2['ot3'] = df2['ot3'].astype(float)
            df2['unpaid'] = df2['unpaid'].astype(float)
            df2['total'] = df2['total'].astype(float)

            insert_into_db(df2, 'employee_work_hours', str(delete_date))

    import pyodbc
    import urllib
    from sqlalchemy import create_engine, event
    conn = f"""Driver={driver};Server={server},1433;Database={database}; Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=True)
    table_name = 'weekly_technican_report'
    delete_query = "delete from {0} where start_of_week >= '{1}'".format(table_name, delete_date)
    logging.info(delete_query)
    engine.execute(delete_query)
    query = """ insert into weekly_technican_report (employeenumber, technician,empfirstname,emplastname, start_of_week, end_of_week, lastupdateddate, day_worked,total_hours, hours_per_day,
                                    dollar_per_day, dollar_per_hour,sqft_per_day,job_per_day,adjust_job_per_day,special_jobs,
                                            rate_per_day,gallon_per_day,service)
    (
        select t1.employeenumber,
              t1.technician,
              t1.empfirstname,
              t1.emplastname,
              '{}' as start_of_week,
              '{}' as end_of_week,
              '{}' as lastupdateddate,
              t1.day_worked,
              cast(wh.total_hrs as DECIMAL(16, 2)) as hours,
              cast(cast(wh.total_hrs as DECIMAL(16, 2)) /
                   cast(t1.day_worked as DECIMAL(16, 2)) as DECIMAL(16, 2)) as hours_per_day,
              t1.dollar_per_day,
              cast(cast(t1.total_dollar as DECIMAL(16, 2)) /
                   cast(wh.total_hrs as DECIMAL(16, 2)) as DECIMAL(16, 2)) as dollar_per_hour,
              t1.sq_ft,
              t1.job_per_day,
              t1.adjust_job_per_day,
              t1.special_jobs,
              cast(coalesce(t2.rate_per_day, 0) as DECIMAL(16, 2)) as rate_per_day,
              cast(coalesce(t2.gallon_per_day, 0) as DECIMAL(16, 2)) as gallon_per_day,
              t1.service
       from (select employeenumber,
                    emplastname + '-' + empfirstname as technician,
                    empfirstname,
                    emplastname,
                    service,
                    (select min(completeddate) from complete_work_dump) as start_of_week,
                    (select max(completeddate) from complete_work_dump) as end_of_week,
                    count(distinct completeddate) as day_worked,
                    cast(sum(completedamount) as DECIMAL(16, 2)) as total_dollar,
                    cast(cast(sum(completedamount) as DECIMAL(16, 2)) /
                         cast(count(distinct completeddate) as DECIMAL(16, 2)) as DECIMAL(16, 2)) as dollar_per_day,
                    cast(cast(count(distinct woheaderid) as DECIMAL(16, 2)) /
                         cast(count(distinct completeddate) as DECIMAL(16, 2)) as DECIMAL(16, 2)) as job_per_day,
                    cast(cast(count(distinct (
                        case
                            when eventname not like 'Recall%' then woheaderid
                            end
                        )) as DECIMAL(16, 2)) /
                         cast(count(distinct completeddate) as DECIMAL(16, 2)) as DECIMAL(16, 2)) as adjust_job_per_day,
                    count(distinct (
                        case
                            when eventname like 'Recall%' then woheaderid
                            end
                        ))  as special_jobs,
                    cast(cast(sum(measurement) as DECIMAL(16, 2)) /
                         (cast(count(distinct completeddate) * 1000 as DECIMAL(16, 2))) as DECIMAL(16, 2)) as sq_ft
             from complete_work_dump as cwd
             where lastupdateddate = '{}'
             group by employeenumber, emplastname + '-' + empfirstname, empfirstname,emplastname, service) as t1
                left join
            (
                select substring(employee, 0, charindex(' ', employee)) as employeenumber,
                       service,
                       nullif(cast(coalesce(
                               avg(case
                                       when service = 'flowerbed' then 0
                                       when itemdescription like '%Rate%'
                                           then cast(materialquantity as DECIMAL(16, 2)) end), 0) as DECIMAL(16, 2)),
                              0)                                        as rate_per_day,
                       cast(
                                   cast(
                                           sum(case
                                                   when service = 'lawn' and itemdescription = 'Fertilizer (46-0-0)'
                                                       then materialquantity
                                                   when service = 'pest' and itemdescription = 'Bifenthrin'
                                                       then materialquantity
                                                   when service = 'tree' and
                                                    (itemdescription = 'Tengard/T&S' or itemdescription = 'Bayleton')
                                                       then materialquantity
                                                   else 0 end) as DECIMAL(16, 2)) /
                                   nullif(count(distinct (case
                                                              when service = 'lawn' and itemdescription = 'Fertilizer (46-0-0)'
                                                                  then completeddate
                                                              when service = 'pest' and itemdescription = 'Bifenthrin'
                                                                  then completeddate
                                                              when service = 'tree' and
                                                                   (itemdescription = 'Tengard/T&S' or itemdescription = 'Bayleton' or itemdescription ='Carbaryl')
                                                                  then completeddate
                                       end)), 0)
                           as DECIMAL(16, 2))                           as gallon_per_day
                from material_report_dump as mr
                where lastupdateddate = '{}'
                  and service not like 'flowerbed'
                group by substring(employee, 0, charindex(' ', employee)), service
            ) as t2 on t1.employeenumber = t2.employeenumber and t2.service = t1.service
                left join (
           select cw.technician, cw.service, wd.total_hrs
           from (select distinct service,
                                 complete_work_dump.employeenumber,
                                 emplastname + '-' + empfirstname as technician,
                                 SUBSTRING(empfirstname, PATINDEX('%[A-Z]%', empfirstname),
                                           LEN(empfirstname)) as empfirstname,
                                 emplastname
                 from complete_work_dump) as cw
                    left join (select emp_code,
                                      trim(substring(replace(replace(name, '"', ''), '.', ','), 0,
                                                     charindex(',', replace(replace(name, '"', ''), '.', ',')))) as emplastname,
                                      trim(substring(replace(replace(name, '"', ''), '.', ','),
                                                     charindex(',', replace(replace(name, '"', ''), '.', ',')) + 1,
                                                     len(replace(replace(name, '"', ''), '.', ','))))            as empfirstname,
                                      cast(sum(reg) + sum(ot1) + sum(ot2) + sum(ot3) as DECIMAL(16, 2)) as total_hrs
                               from employee_work_hours
                               group by emp_code,
                                        trim(substring(replace(replace(name, '"', ''), '.', ','), 0,
                                                       charindex(',', replace(replace(name, '"', ''), '.', ',')))),
                                        trim(substring(replace(replace(name, '"', ''), '.', ','),
                                                       charindex(',', replace(replace(name, '"', ''), '.', ',')) + 1,
                                                       len(replace(replace(name, '"', ''), '.', ','))))) as wd
                              on cw.emplastname = wd.emplastname and (cw.empfirstname = wd.empfirstname or
                                                                      cw.emplastname = substring(wd.empfirstname, 0, 3))) as wh
                          on t1.technician = wh.technician and t1.service = wh.service);""".format(start_of_week,
                                                                                                   end_of_week,
                                                                                                   start_of_week,
                                                                                                   start_of_week,
                                                                                                   start_of_week)
    conn = pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    logging.info('Python timer trigger function ran at %s', datetime.now())


