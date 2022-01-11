import configparser
import datetime
import json
import logging
import os
import sys

import MySQLdb
from dateutil.relativedelta import relativedelta
from flask import Flask, request, jsonify

import util

app = Flask(__name__)
now = datetime.datetime.now()
last_month = now - relativedelta(months=1)

lastMonthLog = "./setup_{}.log".format(format(last_month, '%B'))
if os.path.isfile(lastMonthLog):
    os.remove(lastMonthLog)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s: [ %(message)s ]', '%m/%d/%Y %I:%M:%S %p')
console = logging.FileHandler("./setup_{}.log".format(now.strftime("%B")))
console.setFormatter(fmt)
logger.addHandler(console)

env = sys.argv[-1].lower()
configFile = configparser.ConfigParser()
configFile.read(env + '_config.ini')

@app.route('/report/v1/business/settings', methods=['POST', "GET"])
def report():
    db = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pswd"], db=configFile["analysis"]["reportDB"])
    cur = db.cursor()
    if request.method == "GET":
        agentId = request.args["id"]
        cur.execute(util.settings, (agentId,))
        try:
            settings = json.loads(cur.fetchone()[0])
        except Exception as e:
            logger.error(e)
            settings = {}
        return jsonify(settings)

    elif request.method == 'POST':
        data = request.get_json()
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

        return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9092)
