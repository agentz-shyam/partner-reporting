import datetime as dt
# import json
import logging
import os
import re
from collections import OrderedDict
from itertools import islice, repeat

import pymysql
import requests

import util

env = os.environ['env']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s: [ %(message)s ]', '%m/%d/%Y %I:%M:%S %p')
console = logging.StreamHandler()
console.setFormatter(fmt)
logger.addHandler(console)


def response_dict(tempDict):
    answer = dict()
    # answer["moneySpent"] = int(float(sum(tempDict["moneySpent"])))
    # answer["moneySaved"] = int(float(sum(tempDict["moneySaved"])))
    # answer["conversionValue"] = sum(tempDict["conversionValue"])
    # lifetime = [int(value.strip('k')) if 'k' in str(value) else value for value in tempDict["lifetimeValue"]]
    # if sum(lifetime) > 9999:
    #     answer["lifetimeValue"] = str(int(sum(lifetime) / 1000)) + 'k'
    # else:
    #     answer["lifetimeValue"] = int(sum(lifetime))

    answer['conversion'] = sum(tempDict["conversion"])
    answer["phoneCall"] = sum(tempDict["phoneCall"])
    answer["engagement"] = sum(tempDict["engagement"])
    answer["offEngagement"] = sum(tempDict["offEngagement"])
    answer["sessions"] = sum(tempDict["sessions"])
    # answer['timeSpent'] = "%0.1f" % (sum(list(map(float, tempDict["timeSpent"]))))
    # answer["uniqueUsers"] = sum(tempDict["uniqueUsers"])
    # answer["newUsers"] = sum(tempDict["newUsers"])
    # answer["concurrentSession"] = sum(tempDict["concurrentSession"])
    #
    # answer["averageMessage"] = int(sum(tempDict["averageMessage"]) / len(tempDict["averageMessage"]))
    # answer["averageDuration"] = "%0.2f" % (
    #         sum(list(map(float, tempDict["averageDuration"]))) / len(tempDict["averageDuration"]))
    return answer


def multiBusiness(startDate, endDate, domainId, businessAgentId, type):
    tempanswer = process(startDate, endDate, domainId, businessAgentId, type)
    if 'error' not in tempanswer:
        temp = dict(zip(tempanswer['name'], tempanswer['count']))
        return temp


def process(startDate, endDate, domainId, businessAgentId, type):
    answer = dict()
    startDate = re.findall(r'\d{4}-\d{2}-\d{2}', startDate)
    endDate = re.findall(r'\d{4}-\d{2}-\d{2}', endDate)
    chart = Chart(dt.datetime.strptime(startDate[0], "%Y-%m-%d"), dt.datetime.strptime(endDate[0], "%Y-%m-%d"),
                  domainId, businessAgentId)

    try:
        if type == "statistics":
            flatValue = chart.statistis()
            answer = flatValue
        elif type == "taskCount":
            TaskName, TaskCount = chart.taskcounts()
            answer["name"] = TaskName
            answer["count"] = TaskCount
        elif type == "exitingTask":
            TaskName, TaskCount = chart.exitcounts()
            answer["name"] = TaskName
            answer["count"] = TaskCount
        elif type == "bounceRate":
            bounceCount = chart.bouncerate()
            answer["name"] = list(bounceCount.keys())
            answer["count"] = list(bounceCount.values())
        elif type == "OSCount":
            OSCount = chart.OSEngagement()
            answer["name"] = list(OSCount.keys())
            answer["count"] = list(OSCount.values())
        elif type == "deviceCount":
            deviceCount = chart.deviceEngagement()
            answer["name"] = list(deviceCount.keys())
            answer["count"] = list(deviceCount.values())
        elif type == "browserCount":
            browserCount = chart.browserEngagement()
            answer["name"] = list(browserCount.keys())
            answer["count"] = list(browserCount.values())
        logger.info("process completed")
        logger.info(answer)
    except Exception as e:
        logger.info(e)
        answer = {"error": "No data found"}
    finally:
        chart.db.close()
    return answer


class Chart(object):

    def __init__(self, startDate, endDate, domainId, businessAgentId):

        self.startDate = str(startDate.date())
        self.endDate = str(endDate.date())
        self.businessAgentId = businessAgentId
        self.agentDomainId = domainId
        self.db = pymysql.connect(host=os.environ["host"], user=os.environ["user"],
                                  passwd=os.environ["pswd"], db=os.environ["reportDB"], port=3306)
        self.cur = self.db.cursor()

    def conversionsession(self, ):
        self.cur.execute(util.concurrent, (self.businessAgentId, self.startDate, self.endDate,))
        return list(self.cur.fetchall())

    def statistis(self, ):

        conversion = list()
        phoneCall = list()
        concurrentSession = list()
        one_tasksession = list()
        offSessions = list()
        sessions = list()
        # moneySaved = list()
        # conversionValue = list()
        # lifetimeValue = list()
        # averageDuration = list()
        # timeSpent = list()
        # sumuser_messages = list()
        # lengthuser_messages = list()
        # self.nlquery = list()

        conversion_count = self.conversionsession()
        conversionCount = [cc[0] for cc in conversion_count]
        concurrentSession.append(list(map(int, conversionCount)))

        # reportingPeriod = (dt.datetime.strptime(self.endDate, "%Y-%m-%d") - dt.datetime.strptime(self.startDate,
        #                                                                                          "%Y-%m-%d")).days + 1

        # self.cur.execute(util.settings, (self.businessAgentId,))
        # settings = json.loads(self.cur.fetchone()[0])
        # data = self.cur.fetchone()
        # if data:
        #     settings = json.loads(data[0])
        # else:
        #     settings = {"agentzRate": 0, 'hourlyRate': 0, "conversionRevenue": 0, "yearlyValue": 0}

        self.cur.execute(util.stats, (self.businessAgentId, self.startDate, self.endDate))
        columns = [desc[0] for desc in self.cur.description]

        # moneySpent = "%0.2f" % ((reportingPeriod / 30) * int(settings["agentzRate"]))
        for self.rows in self.cur.fetchall():
            self.tableData = dict(zip(columns, self.rows))
            conversion.append(int(self.tableData.get('completedconversion_task')))
            phoneCall.append(int(self.tableData.get('phonecalls')))
            one_tasksession.append(int(self.tableData.get('one_tasksession')))
            offSessions.append(int(self.tableData.get('sessions_offhours')))
            sessions.append(int(self.tableData.get('sessions_loaded')))

            # moneySaved.append(
            #     (float(self.tableData['timespent']) / 60) * float(settings["hourlyRate"]))
            # conversionValue.append(int(self.tableData['completedconversion_task']) * int(settings["conversionRevenue"]))
            # lifetimeValue.append(int(self.tableData['completedconversion_task']) * int(settings["yearlyValue"]))
            # averageDuration.append(float(self.tableData['averagesession_time']))
            # timeSpent.append(float(self.tableData['timespent']))
            # sumuser_messages.append(int(self.tableData['sumuser_messages']))
            # lengthuser_messages.append(int(self.tableData['lengthuser_messages']))

            # self.nlquery.append(int(self.tableData['nlquery']))

        self.flatValue = dict()
        self.flatValue['conversion'] = sum(conversion)
        self.flatValue["phoneCall"] = sum(phoneCall)
        self.flatValue["engagement"] = sum(one_tasksession)
        self.flatValue["offEngagement"] = sum(offSessions)
        self.flatValue["sessions"] = sum(sessions)

        # self.flatValue["moneySpent"] = int(float(moneySpent))
        # self.flatValue["moneySaved"] = int(float(sum(moneySaved)))
        # self.flatValue["conversionValue"] = sum(conversionValue)
        #
        # if sum(lifetimeValue) > 9999:
        #     self.flatValue["lifetimeValue"] = str(int(sum(lifetimeValue) / 1000)) + 'k'
        # else:
        #     self.flatValue["lifetimeValue"] = int(sum(lifetimeValue))
        #
        # self.cur.execute(util.uniqueUser, (self.businessAgentId, self.startDate, self.endDate))
        # self.flatValue['uniqueUsers'] = list(self.cur.fetchall())[0][0]
        # self.cur.execute(util.newUser, (self.businessAgentId, self.startDate, self.endDate))
        # self.flatValue["newUsers"] = list(self.cur.fetchall())[0][0]

        # try:
        #     self.flatValue["averageDuration"] = "%0.2f" % (sum(averageDuration) / sum(one_tasksession))
        # except Exception as e:
        #     logger.info(e)
        #     self.flatValue["averageDuration"] = 0
        # self.flatValue["timeSpent"] = "%0.1f" % (sum(timeSpent))
        # try:
        #     self.flatValue["averageMessage"] = int(sum(sumuser_messages) / sum(lengthuser_messages))
        # except Exception as e:
        #     logger.info(e)
        #     self.flatValue["averageMessage"] = 0
        #
        # count = 0
        # for concurrent in concurrentSession:
        #     count += sum(concurrent)
        # self.flatValue["concurrentSession"] = count

        return self.flatValue

    def isInfoTask(self):
        self.headers = {"Authorization": os.environ["token"], "Content-type": "application/json"}

        # self.domainIdApi = "https://" + env + os.environ["domainAPi"] + self.businessAgentId
        # domainData = requests.get(url=self.domainIdApi, headers=self.headers).json()
        # self.agentDomainId = domainData['domainId']

        self.taskApi = "https://" + env + os.environ["taskAPi"] + self.agentDomainId + "/tasks"
        response = requests.get(url=self.taskApi, headers=self.headers)

        self.isbusinessTask = list()
        self.idTaskMap = dict()
        if response.status_code != 500:
            results = response.json()
            for taskList in results["tasks"]:
                if taskList["isInfoTask"]:
                    self.isbusinessTask.append(taskList["id"])
                self.idTaskMap[taskList["id"]] = taskList["name"].strip()

    def taskcounts(self, ):
        self.isInfoTask()
        TaskName = []
        TaskCount = []
        if self.isbusinessTask:
            self.cur.execute(util.taskcounts.format(tuple(self.isbusinessTask)),
                             (self.businessAgentId, self.startDate,
                              self.endDate))
            self.taskCount = dict()
            for self.taskset in self.cur.fetchall():
                # if self.taskset[0] in self.isbusinessTask:
                if self.idTaskMap[self.taskset[0]] not in self.taskCount:
                    self.taskCount[self.idTaskMap[self.taskset[0]]] = self.taskset[2]
                else:
                    self.taskCount[self.idTaskMap[self.taskset[0]]] = self.taskCount[
                                                                          self.idTaskMap[self.taskset[0]]] + \
                                                                      self.taskset[2]
            result = dict(
                islice(dict(
                    OrderedDict(sorted(self.taskCount.items(), key=lambda val: val[1], reverse=False))).items(),
                       0, 24))
            self.cur.execute(util.nlquerycounts, (self.businessAgentId, self.startDate, self.endDate))
            nlqueryCount = self.cur.fetchone()[0]
            if nlqueryCount > 0:
                result["Natural language queries"] = int(nlqueryCount)

            for tasks, cnt in sorted(result.items(), key=lambda val: val[1], reverse=False):
                TaskName.append(tasks)
                TaskCount.append(cnt)
            if len(self.taskCount) > 24:
                TaskName.append("Others")
                TaskCount.append(sum([self.taskCount[counts] for counts in set(result) & set(self.taskCount)]))
        return TaskName, TaskCount

    def exitcounts(self, ):
        self.isInfoTask()
        TaskName = []
        TaskCount = []
        if self.isbusinessTask:
            self.cur.execute(util.exitcounts.format(tuple(self.isbusinessTask)),
                             (self.businessAgentId, self.startDate,
                              self.endDate))
            self.finalCount = dict()
            for self.exittask in self.cur.fetchall():
                # if self.exittask[0] in self.isbusinessTask:
                if self.idTaskMap[self.exittask[0]] not in self.finalCount:
                    self.finalCount[self.idTaskMap[self.exittask[0]]] = self.exittask[2]
                else:
                    self.finalCount[self.idTaskMap[self.exittask[0]]] = self.finalCount[
                                                                            self.idTaskMap[self.exittask[0]]] + \
                                                                        self.exittask[2]
            result = dict(
                islice(dict(
                    OrderedDict(sorted(self.finalCount.items(), key=lambda val: val[1], reverse=False))).items()
                       , 0, 24))
            for tasks, cnt in sorted(result.items(), key=lambda val: val[1], reverse=False):
                TaskName.append(tasks)
                TaskCount.append(cnt)
            if len(self.finalCount) > 24:
                TaskName.append("Others")
                TaskCount.append(sum([self.finalCount[counts] for counts in set(result) & set(self.finalCount)]))
        return TaskName, TaskCount

    def bouncerate(self, ):
        self.isInfoTask()
        self.cur.execute(util.bouncerate, (self.businessAgentId, self.startDate, self.endDate))
        self.bounceCount = dict()
        self.temp = dict()
        for self.bouncetask in self.cur.fetchall():
            if self.bouncetask[0] in self.isbusinessTask:
                if self.bouncetask[0] not in self.temp:
                    self.temp[self.bouncetask[0]] = [self.bouncetask[2], self.bouncetask[3]]
                else:
                    self.temp[self.bouncetask[0]] = [self.temp[self.bouncetask[0]][0] + self.bouncetask[2],
                                                     self.temp[self.bouncetask[0]][1] + self.bouncetask[3]]
            else:
                continue

        for self.temptask, self.tempvalue in self.temp.items():
            self.bounceCount[self.idTaskMap[self.temptask]] = "%0.2f" % ((self.tempvalue[0] / self.tempvalue[1]) * 100)
        return dict(OrderedDict(sorted(self.bounceCount.items(), key=lambda val: val[1], reverse=True)))

    def OSEngagement(self, ):
        self.cur.execute(util.OSEngagement, (self.businessAgentId, self.startDate, self.endDate))
        self.OSCount = dict()
        for self.osName in self.cur.fetchall():
            if self.osName[1] not in self.OSCount:
                self.OSCount[self.osName[1]] = self.osName[0]
            else:
                self.OSCount[self.osName[1]] = self.OSCount[self.osName[1]] + self.osName[0]
        return dict(OrderedDict(sorted(self.OSCount.items(), key=lambda val: val[1], reverse=True)))

    def deviceEngagement(self, ):
        self.cur.execute(util.deviceEngagement, (self.businessAgentId, self.startDate, self.endDate))
        self.deviceCount = dict()
        for self.deviceName in self.cur.fetchall():
            self.deviceName = list(self.deviceName)

            if str(self.deviceName[1]).startswith("SM"):
                self.deviceName[1] = 'Samsung'

            if str(self.deviceName[1]) == 'nan':
                self.deviceName[1] = 'Others'

            if self.deviceName[1] not in self.deviceCount:
                self.deviceCount[self.deviceName[1]] = self.deviceName[0]
            else:
                self.deviceCount[self.deviceName[1]] = self.deviceCount[self.deviceName[1]] + self.deviceName[0]
        return dict(OrderedDict(sorted(self.deviceCount.items(), key=lambda val: val[1], reverse=True)))

    def browserEngagement(self, ):
        self.cur.execute(util.browserEngagement, (self.businessAgentId, self.startDate, self.endDate))
        self.browserCount = dict()
        for self.browserName in self.cur.fetchall():
            if self.browserName[1] not in self.browserCount:
                self.browserCount[self.browserName[1]] = self.browserName[0]
            else:
                self.browserCount[self.browserName[1]] = self.browserCount[self.browserName[1]] + self.browserName[0]
        return dict(OrderedDict(sorted(self.browserCount.items(), key=lambda val: val[1], reverse=True)))


def lambda_handler(event, context):
    if 'warm' in event:
        return {"statusCode": 200, "body": "OK"}
    data = event
    logger.info(data)

    startDate = data['startDate']
    endDate = data['endDate']
    businessAgentId = data['businessAgentId']
    try:
        masterBusinessAgentId = data['masterBusinessAgentId']
    except:
        masterBusinessAgentId = None
    domainId = data['domainId']
    type = data["type"]

    answer = dict()

    if masterBusinessAgentId:
        db = pymysql.connect(host=os.environ["host"], user=os.environ["user"],
                             passwd=os.environ["pswd"], db=os.environ["database"], port=3306)
        cur = db.cursor()
        cur.execute(util.masterSlaveID, (masterBusinessAgentId,))
        agentId = [ids[0] for ids in list(cur.fetchall())]
        db.close()
        tempDict = dict()
        if type != "statistics":
            response = list(map(multiBusiness, repeat(startDate), repeat(endDate), domainId, agentId, repeat(type)))
            for temp in filter(None, response):
                try:
                    tempDict = {k: tempDict.get(k, 0) + temp.get(k, 0) for k in set(tempDict) | set(temp)}
                except TypeError:
                    tempDict = {k: tempDict.get(k, 0) + float(temp.get(k, 0)) for k in set(tempDict) | set(temp)}

            tempDict = dict(OrderedDict(sorted(tempDict.items(), key=lambda val: val[1], reverse=True)))
            answer["name"] = list(tempDict.keys())
            answer["count"] = list(tempDict.values())

        else:
            for businessID in agentId:
                businessAgentId = businessID
                tempanswer = process(startDate, endDate, domainId, businessAgentId, type)
                for key, value in tempanswer.items():
                    if key not in tempDict:
                        tempDict[key] = [value]
                    else:
                        tempDict[key].extend([value])
            if 'error' not in tempDict:
                answer = response_dict(tempDict)
            else:
                answer["error"] = 'No data found'
    elif businessAgentId:
        answer = process(startDate, endDate, domainId, businessAgentId, type)

    # elif domainId:
    #     db = pymysql.connect(host=os.environ["host"], user=os.environ["user"],
    #                          passwd=os.environ["pswd"], db=os.environ["reportDB"], port=3306)
    #     cur = db.cursor()
    #     cur.execute(util.domainId, (domainId,))
    #     agentId = [ids[0] for ids in list(cur.fetchall())]
    #     db.close()
    #     tempDict = dict()
    #     if type != "statistics":
    #         response = list(map(multiBusiness, repeat(startDate), repeat(endDate), domainId, agentId, repeat(type)))
    #         for temp in filter(None, response):
    #             try:
    #                 tempDict = {k: tempDict.get(k, 0) + temp.get(k, 0) for k in set(tempDict) | set(temp)}
    #             except TypeError:
    #                 tempDict = {k: tempDict.get(k, 0) + float(temp.get(k, 0)) for k in set(tempDict) | set(temp)}
    #
    #         tempDict = dict(OrderedDict(sorted(tempDict.items(), key=lambda val: val[1], reverse=True)))
    #         answer["name"] = list(tempDict.keys())
    #         answer["count"] = list(tempDict.values())
    #
    #     else:
    #         for businessID in agentId:
    #             businessAgentId = businessID
    #             tempanswer = process(startDate, endDate, businessAgentId, type)
    #             for key, value in tempanswer.items():
    #                 if key not in tempDict:
    #                     tempDict[key] = [value]
    #                 else:
    #                     tempDict[key].extend([value])
    #         if 'error' not in tempDict:
    #             answer = response_dict(tempDict)
    #         else:
    #             answer["error"] = 'No data found'

    else:
        answer["error"] = "No master or business agent id / domain id is given"

    if 'error' in answer and answer["error"] == 'No data found':
        if type == 'statistics':
            answer = {"phoneCall": 0, "averageDuration": 0, "offEngagement": 0, "conversion": 0,
                      "timeSpent": 0.0, "engagement": 0, "concurrentSession": 0, "moneySpent": 0,
                      "newUsers": 0, "moneySaved": 0, "averageMessage": 0, "conversionValue": 0,
                      "uniqueUsers": 0, "lifetimeValue": 0, "sessions": 0}
        else:
            answer = {"name": [], "count": []}

    return answer