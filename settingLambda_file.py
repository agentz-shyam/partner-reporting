import json
import logging
import os

import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s: [ %(message)s ]', '%m/%d/%Y %I:%M:%S %p')
console = logging.StreamHandler()
console.setFormatter(fmt)
logger.addHandler(console)

env = os.environ["env"]


def lambda_handler(event, context):
    if 'warm' in event:
        return {"statusCode": 200, "body": "OK"}
    method = event["httpMethod"]
    db = pymysql.connect(host=os.environ["host"], user=os.environ["user"],
                         passwd=os.environ["pswd"], db=os.environ["reportDB"])
    cur = db.cursor()
    if method.upper() == "GET":
        data = event['queryStringParameters']
        agentId = data["id"]
        settingsQuery = "SELECT settings FROM report_master where business_agent_id = %s"
        cur.execute(settingsQuery, (agentId,))
        response = dict()
        try:
            settings = json.loads(cur.fetchone()[0])
            response["statusCode"] = 200
            response["body"] = json.dumps(settings)
        except Exception as e:
            logger.error(e)
            settings = {"error": e}
            response["statusCode"] = 500
            response["body"] = json.dumps(settings)
        logger.info(response)
        return response

    elif method.upper() == 'POST':
        data = json.loads(event['body'])
        logger.info(data)
        businessAgentId = data['businessAgentId']
        del data['businessAgentId']

        response = {}
        try:
            cur.execute("UPDATE `report_master` SET settings = %s WHERE business_agent_id = %s",
                        (json.dumps(data), businessAgentId))
            db.commit()
            response["status"] = "SUCCESS"
            response["error"] = ' '

        except Exception as e:
            logger.info(e)
            response["status"] = "FAILURE"
            response["error"] = e
        logger.info(response)
        return {"statusCode": 200, "body": json.dumps(response)}
