import requests
import json
import pandas as pd
import logging as logger
from datetime import datetime

from .sql_loader_utils import send_df_to_sql


userName = "delaney"
password = "Stewartsoffic3"

data = {"userLogin": userName, "password": password}
ip = "97.75.169.198"
zyltus_mx_admin_url = "https://{}/newapi/users/".format(ip)
zyltus_stats_url = "https://{}/newapi/config/".format(ip)


def get_json_resposne(content):
    return json.loads(content)


def get_session_token():

    response = requests.post(zyltus_mx_admin_url, data, verify=False)
    if response.status_code == 200:
        logger.info("Received response: {}".format(response.status_code))
        jsonResposne = get_json_resposne(response.content)

        session_token = jsonResposne['session']
        logger.debug(jsonResposne)
        logger.info("Session_token: {}".format(session_token))

        return session_token
    else:
        logger.error("Could not receive data from the API, response: {}, status code: {}, exiting".format(response.content, response.status_code))
        exit(1)


def get_agent_data():
    session_token = get_session_token()
    command = "sv_get_statistic"
    payload = {"session": session_token,
               "command": command,
               "headers": 1,
               "sessions": 1,
               "groups": 1,
               "agents": 1
               }
    response = requests.get(zyltus_stats_url, params=payload, verify=False)
    if response.status_code == 200:
        jsonResponse = get_json_resposne(response.content)
        logger.debug("Agent & Group details: {}".format(jsonResponse))

        return jsonResponse['content']
    else:
        logger.error("Could not retreive agent & group details: {}, StatusCode: {}, exiting".format(response.content, response.status_code))
        exit(1)


def parse_incoming_data(incoming_data):
    logger.info("Parsing incoming data")
    group_stats = 'grp_stats'

    grp_name = incoming_data['grp_name']
    grp_id = incoming_data['grp_id']
    total_agents = incoming_data['grp_agents_total']
    grp_calls_abandoned = incoming_data[group_stats][0]['grp_calls_abandoned']
    grp_queue_calls = incoming_data[group_stats][0]['grp_queue_calls']
    grp_queue_calls_total = incoming_data[group_stats][0]['grp_queue_calls_total']

    logger.debug("AbandonedCalls: {}, "
                 "CurrentQueueCalls: {}, "
                 "TotalQueueCalls: {}".format(grp_calls_abandoned,
                                              grp_queue_calls,
                                              grp_queue_calls_total))

    agent_data = [(agent['agt_id'],
                   agent['agt_name'],
                   agent['agt_presence_usr'],
                   agent['agt_presence_agt'],
                   agent['agt_calls_answered'],
                   agent['agt_calls_inbound'],
                   agent['agt_calls_outgoing'],
                   agent['agt_calls_total']
                   )
                   for agent in incoming_data['agents']]

    logger.debug("Agents: {}".format(agent_data))
    agent_columns = ['agt_id', 'agt_name', 'agt_presence_usr', 'agt_presence_agt', 'agt_calls_answered',
                     'agt_calls_inbound', 'agt_calls_outgoing', 'agt_calls_total']
    data = pd.DataFrame(agent_data, columns=agent_columns)
    data['group_name'] = grp_name
    data['group_id'] = grp_id
    data['total_agents'] = total_agents
    data['grp_calls_abandoned'] = grp_calls_abandoned
    data['grp_queue_calls'] = grp_queue_calls
    data['grp_queue_calls_total'] = grp_queue_calls_total
    data['inserted_date_time'] = datetime.now()

    logger.debug("DataFrame: {}".format(data))

    return data


def prepare_data():
    logger.info("In preparing data")
    groups = get_agent_data()
    for grp in groups:
        grp_name = grp['grp_name']
        if grp_name.lower() == "incoming":
            incoming_data = grp
            logger.info("Found incmoing data: {}".format(json.dumps(incoming_data)))
            return parse_incoming_data(incoming_data)


def prepare_send_data_to_sql():
    table_name = 'zyltus'
    data = prepare_data()
    logger.debug("Received dataframe: {}, to send to SQLServer".format(data))
    if isinstance(data, pd.DataFrame):
        mode = "replace"
        send_df_to_sql(data, table_name, mode=mode)
    else:
        logger.error("Received object is not a dataframe, cannot load to sqlServer")
        return None
