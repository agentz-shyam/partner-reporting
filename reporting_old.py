import configparser
import datetime as dt
import json
import logging
import os
import sys

import MySQLdb
import pytz
import requests
from dateutil.relativedelta import relativedelta
# from flask import Flask, request
from pytz import timezone

import util
from ETL_verification import ETL_verification

# from multiprocessing import Process

env = sys.argv[-1].lower()
configFile = configparser.ConfigParser()
configFile.read(env + '_config.ini')

now = dt.datetime.now()
last_month = now - relativedelta(months=1)
# app = Flask(__name__)

lastMonthLog = "./reporting_scheduler_{}.log".format(format(last_month, '%B'))
if os.path.isfile(lastMonthLog):
    os.remove(lastMonthLog)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s: [ %(message)s ]', '%m/%d/%Y %I:%M:%S %p')
console = logging.FileHandler("./reporting_scheduler_{}.log".format(now.strftime("%B")))
console.setFormatter(fmt)
logger.addHandler(console)


class reportingMethod(object):
    def __init__(self):
        self.messageDB = configFile["analysis"]["consumerDatabase"]
        self.reportDB = configFile["analysis"]["reportDB"]
        self.businessDB = configFile["analysis"]["businessDB"]

        self.db1 = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                   passwd=configFile["analysis"]["pswd"], db=self.messageDB)
        self.cur1 = self.db1.cursor()

        self.db2 = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                   passwd=configFile["analysis"]["pswd"], db=self.reportDB)
        self.cur2 = self.db2.cursor()

        self.db3 = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                   passwd=configFile["analysis"]["pswd"], db=self.businessDB)

        self.cur3 = self.db3.cursor()
        self.ETL = ETL_verification(env)

    def business_id(self, timezones):
        self.Date = dt.datetime.today().date() - dt.timedelta(1)
        self.my_timezone = timezone(timezones)

        self.date = self.my_timezone.localize(
            dt.datetime.strptime(str(self.Date) + ' 00:00:00', "%Y-%m-%d %H:%M:%S")).astimezone(timezone('UTC'))
        self.endDate = self.my_timezone.localize(
            dt.datetime.strptime(str(self.Date) + ' 00:00:00', "%Y-%m-%d %H:%M:%S").replace(hour=23, minute=59,
                                                                                            second=59)).astimezone(
            timezone('UTC'))

        timeZoneQuery = "SELECT business_id FROM business_agent_mapping WHERE timezone = '{}'"
        self.cur3.execute(timeZoneQuery.format(str(self.my_timezone)))

        timezonesBusinessID = [str(id[0]) for id in list(self.cur3.fetchall())]
        logger.info("The available business id for the timezone {} is {}".format(str(self.my_timezone),
                                                                                 ','.join(timezonesBusinessID)))
        if timezonesBusinessID:

            query = "SELECT DISTINCT domain_id, business_id, business_agent_id FROM conversation WHERE created_date >= %s AND created_date <= %s AND business_id IN ({})"
            self.cur1.execute(query.format(','.join('"{0}"'.format(id) for id in timezonesBusinessID)),
                              (self.date, self.endDate))
            for row in self.cur1.fetchall():
                self.cur2.execute(util.statusCheck, (row[2], str(dt.datetime.utcnow().date()),))
                existingStatus = self.cur2.fetchone()
                logger.info("The status for the particular business id is {} => {}".format(row[2], existingStatus))
                if row[1] in timezonesBusinessID:
                    if existingStatus is None or existingStatus[0] != "SUCCESS":
                        logger.info(
                            "The pocess started for the business {} which agent id is {}".format(row[1], row[2]))
                        self.master_entry(row)
            # self.db1.close()
            # self.db2.close()
            # self.db3.close()

    def master_entry(self, row):
        self.row = row
        self.cur2.execute("SELECT * FROM `report_master` WHERE business_agent_id = '%s'" % (self.row[2],))
        selectRow = list(self.cur2.fetchall())

        defaultData = {}
        defaultData['hourlyRate'] = 25
        defaultData['conversionRevenue'] = 100
        defaultData['yearlyValue'] = 2500
        defaultData['agentzRate'] = 0

        defaultValue = json.dumps(defaultData)

        if not selectRow:
            self.cur2.execute(
                "INSERT INTO `report_master` VALUES (NULL,'%s','%s','%s','%s') " % (
                    self.row[0], self.row[1], self.row[2], defaultValue))
            self.db2.commit()

        self.cur2.execute("SELECT id FROM `report_master` WHERE business_agent_id = '%s'" % (self.row[2],))
        for id in self.cur2.fetchone():
            self.id = id
            self.startTime = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            try:
                self.report_generation()
                self.status = "FAILED"
                self.error = "No data"
                if "errorText" not in self.statistics:
                    self.Task2id()
                    self.statistics_entry()
                    self.task_count()
                    self.exiting_task()
                    self.middle_exit()
                    self.bounce_rate()
                    self.sessions_timezone()
                    self.phonecall_os()
                    self.concurrent_session()
                    self.engagement()
                    self.ETL.status_entry(self.db2, self.id, self.row[2], self.Date, logger)
                    self.status = "SUCCESS"
                    self.error = " "

            except TypeError:
                pass

            except Exception as e:
                logger.info(e)
                self.status = "FAILED"
                self.error = "OperationalError"

            finally:
                self.endTime = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                message = [self.id, str(self.Date), str(self.my_timezone), self.startTime,
                           self.endTime, self.status, self.error]
                logger.info(message)
                self.cur2.execute("INSERT INTO `report_event` VALUES {}".format(tuple(message)))
                self.db2.commit()

    def report_generation(self):
        self.data = {
            'start': str(self.Date),
            'end': str(self.Date),
            'agentId': ','.join(self.row),
            'type': "api",
            'timezone': str(self.my_timezone)
        }
        self.url = "http://0.0.0.0:9091/report"

        response = requests.post(url=self.url, data=self.data)

        self.statistics = ' '

        if (response.status_code) != 500:
            self.statistics = response.json()
        return self.statistics

    def Task2id(self, ):
        self.cur2.execute(util.agentDomainId, (self.row[2],))
        self.agentDomainId = list(self.cur2.fetchall())
        self.taskApi = "https://" + env + configFile["charting"]["taskAPi"] + self.agentDomainId[0][0] + "/tasks"
        self.headers = {"Authorization": configFile["charting"]["token"], "Content-type": "application/json"}
        response = requests.get(url=self.taskApi, headers=self.headers)

        self.taskIdMap = dict()
        if response.status_code != 500 or response.status_code != 404:
            results = response.json()
            if not "errorCode" in results:
                for taskList in results["tasks"]:
                    self.taskIdMap[taskList["name"].strip()] = taskList["id"]
        logger.info("Task2id : Done")

    def statistics_entry(self, ):
        self.cur2.execute("SHOW COLUMNS FROM `statistics_value`")
        self.columnName = [name[0] for name in list(self.cur2.fetchall())]
        valuesList = [self.id, str(self.Date)]
        for names in self.columnName[2:]:
            valuesList.append(self.statistics[names])
        self.cur2.execute("INSERT INTO `statistics_value` values {}".format(tuple(valuesList)))
        self.db2.commit()
        logger.info("statistics_entry : Done")

    def task_count(self, ):
        for task in self.statistics['taskNumber']:
            try:
                taskList = [self.id, str(self.Date), self.taskIdMap[task.split(',')[0].strip()], task.split(',')[0],
                            task.split(',')[1]]
            except Exception as e:
                logging.info(e)
                continue
            self.cur2.execute("INSERT INTO `task_count` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("task_count : Done")

    def exiting_task(self):
        for exitTask in self.statistics['exit_tasks_list']:
            try:
                taskList = [self.id, str(self.Date), self.taskIdMap[exitTask.split(',')[0].strip()],
                            exitTask.split(',')[0], exitTask.split(',')[1]]
            except Exception as e:
                logging.info(e)
                continue
            self.cur2.execute("INSERT INTO `exiting_task` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("exiting_task : Done")

    def middle_exit(self, ):
        for dropTask in self.statistics['dropTaskNumber']:
            try:
                taskList = [self.id, str(self.Date), self.taskIdMap[dropTask.split(',')[0].strip()],
                            dropTask.split(',')[0], dropTask.split(',')[1]]
            except Exception as e:
                logging.info(e)
                continue
            self.cur2.execute("INSERT INTO `middle_exit` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("middle_exit : Done")

    def bounce_rate(self):
        for bounceTask, bouncevalue in self.statistics['rate'].items():
            try:
                taskList = [self.id, str(self.Date), self.taskIdMap[bounceTask.strip()], bounceTask, bouncevalue[0],
                            bouncevalue[1]]
            except Exception as e:
                logging.info(e)
                continue
            self.cur2.execute("INSERT INTO `bounce_rate` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("bounce_rate : Done")

    def sessions_timezone(self, ):
        for sessions in self.statistics['sessionCount']:
            taskList = [self.id, str(self.Date), sessions.split(',')[0], sessions.split(',')[1]]
            self.cur2.execute("INSERT INTO `sessions_timezone` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("sessions_timezone : Done")

    def phonecall_os(self, ):
        for osCount in self.statistics['osCount']:
            taskList = [self.id, str(self.Date), osCount.split(',')[0], osCount.split(',')[1],
                        osCount.split(',')[2]]
            self.cur2.execute("INSERT INTO `phonecall_os` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("phonecall_os : Done")

    def engagement(self):
        self.fingerPrints = self.statistics['engagement']["Finger Print"]
        self.cur2.execute(util.userFlag, (self.row[2],))
        self.existingFingerPrint = [tableFingerprint[0] for tableFingerprint in list(self.cur2.fetchall())]
        self.newUser = []

        for fingerPrint in self.fingerPrints:
            if fingerPrint not in self.existingFingerPrint:
                self.newUser.append(1)
            else:
                self.newUser.append(0)

        for engagementDetails in (
                zip(self.fingerPrints, self.statistics['engagement']["Browser Name"],
                    self.statistics['engagement']["OS"], self.statistics['engagement']["Device"],
                    self.newUser)):
            taskList = [self.id, str(self.Date), engagementDetails[0], engagementDetails[1], engagementDetails[2],
                        engagementDetails[3], engagementDetails[4]]
            self.cur2.execute("INSERT INTO `engagement` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("engagement : Done")

    def concurrent_session(self):
        for concurrentSession in self.statistics['simultaneousDayLine'].values():
            taskList = [self.id, str(self.Date), concurrentSession[0].split(',')[0],
                        concurrentSession[0].split(',')[1]]
            self.cur2.execute("INSERT INTO `concurrent_session` values {}".format(tuple(taskList)))
            self.db2.commit()
        logger.info("concurrent_session : Done")


while True:
    reporting = reportingMethod()
    for timezones in pytz.common_timezones:
        allZones_time = dt.datetime.now(timezone(timezones)).time().strftime('%H-%M-%S')
        if dt.datetime.strptime('00-00-05', '%H-%M-%S').time() < dt.datetime.strptime(allZones_time,
                                                                                      '%H-%M-%S').time() <= dt.datetime.strptime(
            '00-05-00', '%H-%M-%S').time():
            try:
                logger.info("Process initiated for {}".format(timezones))
                reporting.business_id(timezones)
            except Exception as e:
                logger.error(e)
