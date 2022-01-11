import configparser
import datetime as dt
import json
import os as operating
import re
import sys
import warnings
import zipfile
from collections import Counter
from datetime import datetime, time

import MySQLdb
import pandas as pd
import requests
from flask import Flask, request, send_file, redirect, url_for, Response
from flask_caching import Cache
from pytz import timezone

from nocache import nocache

# from dateutil.relativedelta import relativedelta

warnings.filterwarnings("ignore")

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
cache.clear()

operating.environ['DEBUG'] = 'true'
operating.environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'true'

env = sys.argv[-1].lower()
configFile = configparser.ConfigParser()
configFile.read(env + '_config.ini')

database = configFile["analysis"]["consumerDatabase"]


def Session(agentId, startDate, endDate, agentzFingerPrint, my_timezone, fmt, env):
    sessionId = list()
    browserName = list()
    fingerPrint = list()
    device = list()
    browserVersion = list()
    os = list()
    osVersion = list()
    deviceType = list()
    createdAt = list()
    agentz = list()
    cookie = list()
    timeZone = list()
    language = list()
    resolution = list()

    db = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pswd"], db=database)
    cur = db.cursor()

    query = "select * from conversation where created_date  >=%s  and  created_date <=%s  and business_agent_id = %s " \
            "and business_id = %s order by created_date asc "
    cur.execute(query, (startDate, endDate, agentId[2], agentId[1],))

    for row in cur.fetchall():

        if "AdsBot-Google" in row[19] or "YandexBot" in row[19] or row[19] == "{}":
            continue

        sessionId.append(row[0])
        createdAt.append(timezone('UTC').localize(row[2]).astimezone(my_timezone).strftime(fmt))

        try:
            ans = json.loads(row[19])

            fingerPrint.append(ans["fingerPrint"])
            cookie.append(ans["isCookie"])
            timeZone.append(ans['timeZone'])
            language.append(ans['language'])
            resolution.append(ans['currentResolution'])

            try:
                browserVersion.append(ans["browserVersion"])
            except:
                browserVersion.append("NA")

            try:
                os.append(ans["os"])
            except:
                os.append("NA")

            try:
                browserName.append(ans["browser"])
            except:
                browserName.append("NA")

            try:
                osVersion.append(ans["osVersion"])
            except:
                osVersion.append("NA")

            try:
                if ans["browserData"]["device"]:
                    device.append(ans["browserData"]["device"]["model"])
                    deviceType.append(ans["browserData"]["device"]["type"])
                else:
                    device.append("NA")
                    deviceType.append("NA")
            except:
                device.append("NA")
                deviceType.append("NA")

            if env != "dev":
                if ans["fingerPrint"] in agentzFingerPrint:
                    agentz.append("YES")
                else:
                    agentz.append("NO")
            else:
                agentz.append("NO")

        except Exception as e:
            ans = json.dumps(row[19]).lstrip('[').rstrip(']').strip('][').split(',')
            browserIndice = [i for i, s in enumerate(ans) if 'browser:[name' in s]
            if browserIndice:
                browserName.append(ans[browserIndice[0]].split(":")[-1].strip(']'))
            else:
                browserName.append("NA")

            browserVersionIndice = [i for i, s in enumerate(ans) if ' version' in s]
            if browserVersionIndice:
                browserVersion.append(ans[browserVersionIndice[0]].split(":")[-1])
            else:
                browserVersion.append("NA")

            osIndice = [i for i, s in enumerate(ans) if ' os:[name' in s]
            if osIndice:
                os.append(ans[osIndice[0]].split(":")[-1])
            else:
                os.append("NA")

            osVersionIndice = [i for i, s in enumerate(ans) if ' version:' in s]
            if len(osVersionIndice) > 1:
                osVersion.append(ans[osVersionIndice[1]].split(":")[-1].strip(']'))
            else:
                osVersion.append("NA")

            fingerIndices = [i for i, s in enumerate(ans) if 'fingerPrint' in s]
            if fingerIndices:
                fingerPrint.append(ans[fingerIndices[0]].split(":")[-1])
            else:
                fingerPrint.append("NA")

            deviceIndice = [i for i, s in enumerate(ans) if ' device:[' in s]
            if deviceIndice:
                device.append(ans[deviceIndice[0]].split(":")[-1].strip(']'))
            else:
                device.append("NA")

            deviceTypeIndice = [i for i, s in enumerate(ans) if ' type:' in s]
            if deviceTypeIndice:
                deviceType.append((ans[deviceTypeIndice[0]].split(":")[-1]).strip(']'))
            else:
                deviceType.append("NA")

            cookieIndice = [i for i, s in enumerate(ans) if ' isCookie:' in s]
            if cookieIndice:
                cookie.append((ans[cookieIndice[0]].split(":")[-1]).strip(']'))
            else:
                cookie.append("NA")

            languageIndice = [i for i, s in enumerate(ans) if ' language:' in s]
            if languageIndice:
                language.append((ans[languageIndice[0]].split(":")[-1]).strip(']'))
            else:
                language.append("NA")

            resolutionIndice = [i for i, s in enumerate(ans) if ' currentResolution:' in s]
            if cookieIndice:
                resolution.append((ans[resolutionIndice[0]].split(":")[-1]).strip(']'))
            else:
                resolution.append("NA")

            timeIndice = [i for i, s in enumerate(ans) if ' timeZone:' in s]
            if timeIndice:
                timeZone.append((ans[timeIndice[0]].split(":")[-1]).strip(']'))
            else:
                timeZone.append("NA")

            if env != "dev":
                if ans[fingerIndices[0]].split(":")[-1] in agentzFingerPrint:
                    agentz.append("YES")
                else:
                    agentz.append("NO")
            else:
                agentz.append("NO")

    sessionDF = {"Finger Print": fingerPrint, "Session Id": sessionId, "Created At": createdAt,
                 "Browser Name": browserName, "Browser Version": browserVersion, "OS": os, "OS Version": osVersion,
                 "Device": device, "Device Type": deviceType, "Current Resolution": resolution, "Language": language,
                 "Cookie": cookie, "Time Zone": timeZone, "Is agentz ID?": agentz}

    df = pd.DataFrame(sessionDF)
    df.sort_values(["Finger Print", "Created At"], ascending=[True, True], inplace=True, )
    if not operating.path.isdir("./Session_report/"):
        operating.mkdir("./Session_report/")

    df.to_csv("./Session_report/SessionDetails_{}to{}.csv".format(str(startDate).split()[0], str(endDate).split()[0]),
              index=False)
    db.close()

    return df


def Message(agentId, sessionDF, startDate, endDate, agentzFingerPrint, my_timezone, fmt):
    sessionID = []
    intentName = []
    time = []
    agentz = []
    fingerprint = []
    engagement = []

    db = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pswd"], db=database)
    cur = db.cursor()

    sessionId = sessionDF["Session Id"]

    for session in sessionId:

        query = "select * from message where conversation_id=%s  and created_date  >=%s and  created_date <=%s order" \
                " by created_date asc"
        engageQuery = "select count(event_type) from conversation_event where business_agent_mapping_id=%s and " \
                      "conversation_id=%s and (event_type='USER_INPUT' or (event_type='CHAT_WINDOW_EVENT' and " \
                      "event_value='Phone_Call'))"

        cur.execute(query, (session, startDate, endDate))

        for data in cur.fetchall():
            ans = json.loads(data[5])

            if ans["sender"]["senderType"] == "CONSUMER" and ans["payload"]["payloadType"] == "CHAT_WINDOW_EVENT":

                intent = ans["payload"]["intentName"]
                intentName.append(intent)
                time.append(timezone('UTC').localize(data[2]).astimezone(my_timezone).strftime(fmt))
                sessionID.append(ans["sessionId"])
                fingerP = sessionDF[sessionDF["Session Id"] == session]["Finger Print"]
                fingerprint.extend(list(fingerP))
                cur.execute(engageQuery, (agentId[2], session,))
                eventType = [events[0] for events in list(cur.fetchall())]
                if eventType[0] > 0:
                    engagement.append("engaged")
                else:
                    engagement.append("not engaged")
                if list(fingerP)[0] in agentzFingerPrint:
                    agentz.append("YES")
                else:
                    agentz.append("NO")

            elif ans["sender"]["senderType"] == "BOT":

                try:
                    intent = ans["payload"]["intentName"]
                except:
                    intent = 'NA'
                intentName.append(intent)
                time.append(timezone('UTC').localize(data[2]).astimezone(my_timezone).strftime(fmt))
                sessionID.append(ans["sessionId"])
                fingerP = sessionDF[sessionDF["Session Id"] == session]["Finger Print"]
                fingerprint.extend(list(fingerP))
                cur.execute(engageQuery, (agentId[2], session,))
                eventType = [events[0] for events in list(cur.fetchall())]
                if eventType[0] > 0:
                    engagement.append("engaged")
                else:
                    engagement.append("not engaged")
                if list(fingerP)[0] in agentzFingerPrint:
                    agentz.append("YES")
                else:
                    agentz.append("NO")

    messageDF = {"Finger Print": fingerprint, "Session Id": sessionID, "Time": time, "Intent": intentName,
                 "Engagement": engagement, "Is agentz ID?": agentz}

    DF = pd.DataFrame(messageDF)
    DF.drop_duplicates(inplace=True)
    DF = DF.loc[DF.Intent.shift(1) != DF.Intent]
    DF.sort_values(["Finger Print", "Time"], ascending=[True, True], inplace=True, )
    if not operating.path.isdir("./Message_report/"):
        operating.mkdir("./Message_report/")
    DF.to_csv("./Message_report/MessageDetails_{}to{}.csv".format(str(startDate).split()[0], str(endDate).split()[0]),
              index=False)
    db.close()
    return DF


def analysis(start, end, agentID):
    messageDF = pd.read_csv("./Message_report/MessageDetails_{}to{}.csv".format(start, end))
    data = messageDF[messageDF["Is agentz ID?"] == "NO"]

    sessionDF = pd.read_csv("./Session_report/SessionDetails_{}to{}.csv".format(start, end))
    sessiondata = sessionDF[sessionDF["Is agentz ID?"] == "NO"]
    sessionData = sessiondata[sessiondata["Session Id"].isin(data["Session Id"])]

    db = MySQLdb.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pswd"], db=database)

    cur = db.cursor()

    # try:
    #     previousData = thisMonth[thisMonth["Is agentz ID?"] == "NO"]
    # except TypeError:
    #     previousData = thisMonth

    taskApi = "https://" + env + configFile["charting"]["taskAPi"] + agentID[0] + "/tasks"
    headers = {"Authorization": configFile["charting"]["token"], "Content-type": "application/json"}
    response = requests.get(url=taskApi, headers=headers)

    conversion = []
    if response.status_code != 500 or response.status_code != 404:
        results = response.json()
        if "errorCode" not in results:
            for taskList in results["tasks"]:
                if taskList["isConversion"]:
                    conversion.append(taskList['name'])

    nlp = ["KRR Suggestions Call To Action", "KRR_MODEL2", "BUSINESS_AGENT_KRR_MODEL2"]
    menu = ["Welcome", "Main Menu", "Main Menu 2 - Visitor Funnel", "Menu  - Starting Visitors Flows"]
    fmt = "%Y-%m-%d %H:%M:%S"

    df3 = data[data["Engagement"] == "engaged"]

    # task = df3[df3["Intent"].isin(conversion)]
    completedTask = 0

    engaged = list()
    userMessage = list()
    sessionId = list()
    exitTask = list()
    startTask = list()
    oneTask = list()
    oneConversion = list()
    incompleteTask = list()
    completeTask = list()
    nlpSession = list()
    nlpquerycount = list()
    suggestionSession = list()
    fallbackSession = list()
    offTasks = list()
    offTime = list()
    exit_tasks_list = list()
    rate = list()
    offEngaged = list()
    sessionoffId = list()
    taskNumber = list()
    dropTask = list()
    offTask_dropTask = list()
    dropTaskNumber = list()
    bouncetask = dict()
    monthlyDate = dict()
    simultaneousDayLine = dict()

    for session in df3["Session Id"]:
        tempTime = list()
        if session not in sessionId:
            sessionId.append(session)
            temp = df3[df3["Session Id"] == session]
            Inte = [inte for inte in list(temp["Intent"]) if str(inte) != 'nan']
            One = [atleast for atleast in Inte[2:]]

            if len([OneTask for OneTask in One if not re.findall('[^A-Za-z0-9- ]', OneTask)]) >= 1:
                oneTask.append(session)
                Temp = df3[df3["Session Id"] == session]
                timeData = sorted(list(Temp["Time"]), reverse=True)
                for timeIdx in range(len(timeData)):
                    if timeIdx + 1 > len(timeData) - 1:
                        break
                    diff = (datetime.strptime(" ".join(timeData[timeIdx].split()[:-1]), fmt) -
                            datetime.strptime(" ".join(timeData[timeIdx + 1].split()[:-1]), fmt)
                            ).total_seconds()
                    if diff < 900.0:
                        tempTime.append(diff)
                engaged.append(sum(tempTime))
                if len(list(set(One) & set(conversion))) >= 1:
                    oneConversion.append(session)
                    if "EXIT_TASK_EVENT" in One:
                        incompleteTask.append(session)
                    elif "EXIT_TASK_EVENT" not in One:
                        completeTask.append(session)

            finalTask = [Task for Task in Inte if not re.findall('[^A-Za-z0-9- ]', Task)]
            if len(set(One) & set(nlp)) >= 1:
                nlpSession.append(session)
                nlpquerycount.append(len(set(One) & set(nlp)))
                oneTask.append(session)
            if "ML_SUGGESTIONS" in list(set(One) & set(nlp)):
                suggestionSession.append(len(list(set(One) & set(nlp))))
            if "Fallback" in finalTask:
                fallbackSession.append(finalTask.count("Fallback"))
            if len(finalTask) > 2:
                if finalTask[-1] not in menu:
                    exitTask.append(finalTask[-1])
                if finalTask[2] not in menu:
                    startTask.append(finalTask[2])
                if finalTask[-1] == finalTask[2]:
                    if finalTask[2] in bouncetask:
                        bouncetask[finalTask[2]] += 1
                    else:
                        bouncetask[finalTask[2]] = 1
            taskLength = [userTask for userTask in Inte if not re.findall('[^A-Za-z0-9- ]', userTask)]
            if len(taskLength) > 2:
                userMessage.append(len(taskLength) - (taskLength.count(menu[0]) + taskLength.count(menu[1])))

            query = "select count(event_type) from conversation_event where business_agent_mapping_id = '{}' and " \
                    "conversation_id='{}' and event_type ='TASK_COMPLETE' and event_additional_info in {}"
            cur.execute(query.format(agentID[2], session, tuple(conversion)))
            completedTask += list(cur.fetchone())[0]

    for exit_tasks in Counter(exitTask).most_common():
        exit_tasks_list.append(exit_tasks[0] + ' , ' + str(exit_tasks[1]))

    bounceRate = dict()
    for starttask in Counter(startTask).most_common():
        if starttask[0] in bouncetask:
            rate.append(
                starttask[0] + "," + "{0:.2f}".format((bouncetask[starttask[0]] / starttask[1]) * 100) + " , " + str(
                    starttask[1]))
            bounceRate[starttask[0]] = [bouncetask[starttask[0]], starttask[1]]

    task = df3[df3["Intent"].isin(conversion)]
    for times in task["Time"]:
        if (datetime.strptime(" ".join(times.split()[:-1]), fmt).time()) > time(18, 00, 00):
            offTime.append(times)

    offTask = task[task["Time"].isin(offTime)]
    completedOffTask = offTask.shape[0]

    for offTimes in df3["Time"]:
        if datetime.strptime(" ".join(offTimes.split()[:-1]), fmt).time() <= time(8, 00, 00):
            offTasks.append(offTimes)
        if datetime.strptime(" ".join(offTimes.split()[:-1]), fmt).time() >= time(18, 00, 00):
            offTasks.append(offTimes)

    offhourTask = df3[df3["Time"].isin(offTasks)]

    for sessions in offhourTask["Session Id"]:
        tempTime1 = list()

        if sessions not in sessionoffId:
            sessionoffId.append(sessions)
            temp = offhourTask[offhourTask["Session Id"] == sessions]
            InteOff = [ine for ine in list(temp["Intent"]) if str(ine) != 'nan']
            oneOff = [atleast for atleast in InteOff[2:]]
            if len([Task for Task in oneOff if not re.findall('[^A-Za-z0-9- ]', Task)]) > 2:
                timeData = sorted(list(temp["Time"]), reverse=True)
                for timeIdx in range(len(timeData)):
                    if timeIdx + 1 > len(timeData) - 1:
                        break
                    diff1 = (datetime.strptime(" ".join(timeData[timeIdx].split()[:-1]), fmt) -
                             datetime.strptime(" ".join(timeData[timeIdx + 1].split()[:-1]), fmt)).total_seconds()
                    if diff1 < 900.0:
                        tempTime1.append(diff1)
                offEngaged.append(sum(tempTime1))

    Intent = [intents for intents in list(df3[df3["Session Id"].isin(oneTask)]["Intent"]) if str(intents) != 'nan']
    taskExecuted = [task for task in Intent if not re.findall('[^A-Za-z0-9- ]', task)]

    for tasks in Counter(taskExecuted).most_common():
        if not tasks[0] in menu:
            taskNumber.append(tasks[0] + ' , ' + str(tasks[1]))

    df4 = df3[df3["Session Id"].isin(oneTask)]
    df4["time"] = df4["Time"].apply(
        lambda Date: datetime.strptime(" ".join(Date.split()[:-1]), fmt).time())

    for startEnd in list(set(df4["Session Id"].tolist())):

        timta = sorted(list(df4[df4["Session Id"] == startEnd]["Time"]))

        starttime = datetime.strptime(" ".join(timta[0].split()[:-1]), fmt).time()
        endtime = datetime.strptime(" ".join(timta[1].split()[:-1]), fmt).time()

        startdate = datetime.strptime(" ".join(timta[0].split()[:-1]), fmt).date()
        enddate = datetime.strptime(" ".join(timta[1].split()[:-1]), fmt).date()

        tempKey = str(startdate) + '-' + str(starttime.hour)
        tempKey1 = str(enddate) + '-' + str(endtime.hour)

        for key in [tempKey, tempKey1]:
            if key not in monthlyDate:
                monthlyDate[key] = [startEnd]
            elif startEnd not in monthlyDate[key]:
                monthlyDate[key].append(startEnd)

    tempDict1 = {key: list(set(value)) for (key, value) in monthlyDate.items() if len(set(value)) >= 2}

    dict3 = {}

    for dated, sessionss in tempDict1.items():
        sessionRange = dict()

        for sess in sessionss:
            timeData1 = sorted(list(df4[df4["Session Id"] == sess]["time"]))
            sessionRange[sess] = [timeData1[0], timeData1[-1]]

        for sessionKey, timeValue in sessionRange.items():
            startTime = timeValue[0]
            endTime = timeValue[1]
            dict3[dated] = [sessionKey]

            for session_key, time_value in sessionRange.items():
                if sessionKey == session_key:
                    continue
                startTime2 = time_value[0]
                endTime2 = time_value[1]
                if ((startTime2 >= startTime) and (endTime2 <= endTime)) or (endTime >= startTime2 >= startTime) or (
                        (startTime2 <= startTime) and (endTime2 >= endTime)) or (startTime <= endTime2 <= endTime):
                    (dict3[dated]).append(session_key)

    concurrency = {dateee: list(set(valuee)) for (dateee, valuee) in dict3.items() if len(set(valuee)) >= 2}

    for concurrencyDate, concurrencySession in concurrency.items():
        simultaneousDayTime = []
        timeValue = sorted(list(df4[df4["Session Id"] == concurrencySession[0]]["time"]))
        simultaneousDayTime.append(
            str("%02d" % timeValue[0].hour) + ":00 - " + str("%02d" % (timeValue[0].hour + 1)) + ":00, " + str(
                len(concurrencySession)))
        simultaneousDayLine['/'.join(concurrencyDate.split('-')[:3])] = simultaneousDayTime

    simultaneousSession = 0
    for concurrentSession in concurrency.values():
        simultaneousSession += len(concurrentSession)

    dataIntent = [event for event in df3["Intent"] if str(event) != 'nan']
    index = [idx for idx, intent in enumerate(dataIntent) if "EXIT_TASK_EVENT" in intent]

    conversionDropoff = []
    for indice in index:
        dropTask.append(dataIntent[indice - 1])
        if dataIntent[indice - 1] in conversion:
            conversionDropoff.extend(set(list(df3[df3["Intent"] == dataIntent[indice]]["Finger Print"])))

    offTask_dataIntent = [offTask_event for offTask_event in offTask["Intent"] if str(offTask_event) != 'nan']
    offTask_index = [offTask_idx for offTask_idx, offTask_intent in enumerate(offTask_dataIntent) if
                     "EXIT_TASK_EVENT" in offTask_intent]

    offTask_conversionDropoff = []
    for offTask_indice in offTask_index:
        offTask_dropTask.append(offTask_dataIntent[offTask_indice - 1])
        if offTask_dataIntent[offTask_indice - 1] in conversion:
            offTask_conversionDropoff.extend(
                set(list(df3[df3["Intent"] == offTask_dataIntent[offTask_indice]]["Finger Print"])))

    for droptasks in Counter(dropTask).most_common():
        dropTaskNumber.append(droptasks[0] + ',' + str(droptasks[1]))

    # returningUser = list(set(sessionData["Finger Print"]) & set(previousData["Finger Print"]))
    # returningTask = df3[df3["Finger Print"].isin(returningUser)]
    # returningUserConversion = returningTask[returningTask["Intent"].isin(conversion)].shape[0]
    #
    # newUser = [User for User in list(set(df3["Finger Print"])) if User not in returningUser]
    # newTask = df3[df3["Finger Print"].isin(newUser)]
    # newUserConversion = newTask[newTask["Intent"].isin(conversion)].shape[0]

    # newOffTask = offTask[offTask["Finger Print"].isin(newUser)]

    sessionCount = []
    timeZoneCount = sessionData.astype(str).groupby("Time Zone")["Session Id"].count().reset_index()

    for timeZone, count in zip(timeZoneCount["Time Zone"], timeZoneCount["Session Id"]):
        sessionCount.append(timeZone + ', ' + str(count))

    osCount = []
    osversion = sessionData[
        sessionData["Finger Print"].isin(list(df3[df3["Intent"] == "Phone_Call"]["Finger Print"]))]
    osversion.drop_duplicates(subset="Finger Print", inplace=True)
    osVersion = osversion.groupby(['OS', 'OS Version'])["Finger Print"].count().reset_index()

    for os, os_version, count in zip(osVersion['OS'], osVersion['OS Version'], osVersion['Finger Print']):
        osCount.append(os + ', ' + str(os_version) + ', ' + str(count))

    sessionData.drop_duplicates(inplace=True, subset="Finger Print")

    userCount = []
    fingerCount = sessionData.astype(str).groupby("Time Zone")["Finger Print"].count().reset_index()

    for timezone, user in zip(fingerCount["Time Zone"], fingerCount["Finger Print"]):
        userCount.append(timezone + ', ' + str(user))

    osUserCount = []
    osUserVersion = sessionData.astype(str).astype(str).groupby(['OS', 'OS Version'])[
        "Finger Print"].count().reset_index()

    for osUser, os_Userversion, usercount in zip(osUserVersion['OS'], osUserVersion['OS Version'],
                                                 osUserVersion['Finger Print']):
        osUserCount.append(osUser + ', ' + str(os_Userversion) + ', ' + str(usercount))

    browserUserCount = []
    browserUser = sessionData.astype(str).groupby(["Browser Name", "Browser Version"])[
        "Finger Print"].count().reset_index()

    for browUser, browversion, browcount in zip(browserUser["Browser Name"], browserUser["Browser Version"],
                                                browserUser["Finger Print"]):
        browserUserCount.append(browUser + ', ' + str(browversion) + ', ' + str(browcount))

    deviceUserCount = []
    deviceUser = sessionData.astype(str).groupby(["Device", "Device Type"])["Finger Print"].count().reset_index()

    for devicer, deviceversion, devicecount in zip(deviceUser["Device"], deviceUser["Device Type"],
                                                   deviceUser["Finger Print"]):
        deviceUserCount.append(devicer + ', ' + str(deviceversion) + ', ' + str(devicecount))

    if not operating.path.isdir("./Statistics_report/"):
        operating.mkdir("./Statistics_report/")

    file = open("./Statistics_report/StatisticsDetails_{}to{}.csv".format(start, end), "w")
    statistics = {}

    file.write("\nTotal number of sessions loaded , {}".format(len(data["Session Id"].unique().tolist())))
    statistics['sessions_loaded'] = len(data["Session Id"].unique().tolist())

    file.write(
        "\nTotal number of times users didn't engage , %d" % (len(data["Session Id"].unique().tolist()) - len(oneTask)))
    statistics['nonEngage_sessions'] = len(data["Session Id"].unique().tolist()) - len(oneTask)

    file.write("\nTotal number of unique user , %d" % len(list(set(sessionData["Finger Print"]))))
    statistics['unique_user'] = len(list(set(sessionData["Finger Print"])))

    # file.write("\nTotal number of new users , %d" % (len(newUser)))
    # statistics['new_user'] = len(newUser)

    try:

        file.write("\nAverage session time , %0.2f " % ((sum(engaged) / len(engaged)) / 60))
        statistics['averagesession_time'] = "%0.2f " % ((sum(engaged)) / 60)

        file.write("\nLongest session time , %0.2f " % (max(engaged) / 60))
        statistics['longestsession_time'] = "%0.2f " % (max(engaged) / 60)

        file.write("\nTotal time spent by bot in engagement , %0.2f " % (sum(engaged) / 60))
        statistics['timespent'] = "%0.2f " % (sum(engaged) / 60)

    except ZeroDivisionError:

        file.write("\nAverage session time ,0 ")
        statistics['averagesession_time'] = 0

        file.write("\nLongest session time , 0 ")
        statistics['longestsession_time'] = 0

        file.write("\nTotal time spent by bot in engagement , 0 ")
        statistics['timespent'] = 0

    try:

        file.write("\nAverage user messages per session , %d" % (sum(userMessage) / len(userMessage)))
        statistics['sumuser_messages'] = sum(userMessage)
        statistics['lengthuser_messages'] = len(userMessage)

    except ZeroDivisionError:

        file.write("\nAverage user messages per session ,0 ")
        statistics['sumuser_messages'] = 0
        statistics['lengthuser_messages'] = 0

    file.write(
        "\nTotal number of not handled messages , %d" % df3[df3["Intent"].isin(["ML_SUGGESTIONS", "Fallback"])].shape[
            0])
    statistics['nothandled_messages'] = df3[df3["Intent"].isin(["ML_SUGGESTIONS", "Fallback"])].shape[0]

    file.write("\nTotal phone calls placed , %d" % (df3[df3["Intent"] == "Phone_Call"].shape[0]))
    statistics['phonecalls'] = df3[df3["Intent"] == "Phone_Call"].shape[0]

    file.write("\nTotal number of completed conversion task , %d" % completedTask)
    statistics['completedconversion_task'] = completedTask

    # file.write("\nTotal number of conversions by returning users , %d" % returningUserConversion)
    # statistics['returningusers_conversion'] = returningUserConversion
    #
    # file.write("\nTotal number of conversions by new users , %d" % newUserConversion)
    # statistics['newusers_conversion'] = newUserConversion

    file.write(
        "\nTotal unique user who engaged , %d " % len(set(df3[df3["Session Id"].isin(oneTask)]["Finger Print"])))
    statistics['engagedunique_users'] = len(set(df3[df3["Session Id"].isin(oneTask)]["Finger Print"]))

    file.write("\nTotal number of users who dropped off during a conversion task without completing it , {}".format(
        len(set(conversionDropoff))))
    statistics['droppedoff_conversion'] = len(set(conversionDropoff))

    file.write("\nTotal number of sessions with atleast one task , %d " % (len(set(oneTask))))
    statistics['one_tasksession'] = len(set(oneTask))

    file.write("\nTotal number of sessions with atleast one conversion task , %d " % (len(oneConversion)))
    statistics['oneconversion_tasksession'] = len(oneConversion)

    file.write("\nTotal number of sessions with incomplete conversion tasks , %d " % (len(incompleteTask)))
    statistics['incompleteconversion_tasksession'] = len(incompleteTask)

    file.write("\nTotal number of sessions with completed conversion tasks , %d " % (len(completeTask)))
    statistics['completedconversion_tasksession'] = len(completeTask)

    file.write("\nTotal number of users who used natural language queries , %d " % (
        len(set(df3[df3["Session Id"].isin(nlpSession)]["Finger Print"]))))
    statistics['nlquery_users'] = len(set(df3[df3["Session Id"].isin(nlpSession)]["Finger Print"]))

    file.write("\nTotal number of natural language queries , %d " % sum(nlpquerycount))
    statistics['nlquery'] = sum(nlpquerycount)

    file.write("\nTotal number of suggestions , %d " % sum(suggestionSession))
    statistics['suggestions'] = sum(suggestionSession)

    file.write("\nTotal number of fallback , %d " % sum(fallbackSession))
    statistics['fallback'] = sum(fallbackSession)

    file.write("\n")
    file.write(
        "\nTotal number of users who dropped off during a conversion task without completing it during off-hours"
        " , {}".format(
            len(set(offTask_conversionDropoff))))
    statistics['droppedOff_conversion_offhours'] = len(set(offTask_conversionDropoff))

    file.write("\nTotal number of sessions during off-hours , %d " % (len(list(set(offhourTask["Session Id"])))))
    statistics['sessions_offhours'] = len(list(set(offhourTask["Session Id"]) & set(oneTask)))

    file.write("\nTotal phone calls placed during off-hours , %d " % (
        offhourTask[offhourTask["Intent"] == "Phone_Call"].shape[0]))
    statistics['phonecalls_offhours'] = offhourTask[offhourTask["Intent"] == "Phone_Call"].shape[0]

    try:

        file.write("\nTotal time spent by bot in engagement during off-hours , %0.2f " % (sum(offEngaged) / 60))
        statistics['timespent_offhours'] = "%0.2f " % (sum(offEngaged) / 60)

    except ZeroDivisionError:

        file.write("\nTotal time spent by bot in engagement during off-hours ,0 ")
        statistics['timespent_offhours'] = 0

    file.write("\nTotal number of conversion tasks completed during off-hours , %d" % completedOffTask)
    statistics['completedconversion_task_offhours'] = completedOffTask

    # file.write("\nTotal number of conversions by new users during off-hours , %d" % len(newOffTask["Intent"]))
    # statistics['newusers_conversion_offhours'] = len(newOffTask["Intent"])

    file.write("\n")
    file.write("\nTasks and the total number of times it was executed : \n{}".format(",\n".join(taskNumber)))
    statistics['taskNumber'] = taskNumber

    file.write("\n")
    file.write("\nFinal/Exiting task in a conversation and their counts : \n{}".format(",\n".join(exit_tasks_list)))
    statistics['exit_tasks_list'] = exit_tasks_list

    file.write("\n")
    file.write(
        "\nTasks from which users exited in the middle and their counts : \n{}".format(",\n".join(dropTaskNumber)))
    statistics['dropTaskNumber'] = dropTaskNumber

    file.write("\n")
    file.write("\nTasks and their bounce rate : \n{}".format(",\n".join(rate)))
    statistics['rate'] = bounceRate

    file.write("\n")
    file.write(
        "\nTotal number of conversations whom the bot serviced at the same time simultaneously ,"
        " %d " % simultaneousSession)
    statistics['simultaneousSession'] = simultaneousSession

    file.write("\nConcurrent bot engagement details - Daily :\n{}".format(
        '\n'.join('{}  \n{}'.format(key, "\n".join(value)) for key, value in simultaneousDayLine.items())))
    statistics['simultaneousDayLine'] = simultaneousDayLine

    file.write("\n")
    file.write("\nSessions count by timezone : \n{}".format(",\n".join(sessionCount)))
    statistics['sessionCount'] = sessionCount

    file.write("\n")
    file.write("\nUser count by timezone : \n{}".format(",\n".join(userCount)))
    statistics['userCount'] = userCount

    file.write("\n")
    file.write("\nPhone call clicked user count based on OS-OS version :\n{}".format(",\n".join(osCount)))
    statistics['osCount'] = osCount

    file.write("\n")
    file.write("\nEngagement by OS :\n{}".format(",\n".join(osUserCount)))
    statistics['osUserCount'] = osUserCount

    file.write("\n")
    file.write("\nEngagement by browser : \n{}".format(",\n".join(browserUserCount)))
    statistics['browserUserCount'] = browserUserCount

    file.write("\n")
    file.write("\nEngagement by device, device type: \n{}".format(",\n".join(deviceUserCount)))
    statistics['deviceUserCount'] = deviceUserCount

    engagementDF = sessionData.drop_duplicates('Finger Print').drop(
        ['Session Id', 'Created At', 'Current Resolution', 'Language', 'Cookie', 'Is agentz ID?'], axis=1)

    # newUser_check = [True if allUser in newUser else False for allUser in list(engagementDF['Finger Print'])]

    engagementDict = {'Finger Print': list(map(str, engagementDF['Finger Print'].tolist())),
                      'Browser Name': list(map(str, engagementDF['Browser Name'].tolist())),
                      'OS': list(map(str, engagementDF['OS'].tolist())),
                      'Device': list(map(str, engagementDF['Device'].tolist()))}
    # engagementDict['New User'] = newUser_check

    statistics["engagement"] = engagementDict
    file.close()
    db.close()
    return json.dumps(statistics)


@app.route('/download/<start>/<end>')
@nocache
def downloadFile(start, end):
    paths = ['Session_report', 'Message_report', 'Statistics_report']
    zipper = zipfile.ZipFile("reports.zip", "w", zipfile.ZIP_DEFLATED)

    for path in paths:
        for root1, dirs1, files1 in operating.walk(path):
            for csvFile1 in files1:
                if csvFile1 == path.split('_')[0] + "Details_{}to{}.csv".format(start, end):
                    try:
                        zipper.write(csvFile1)
                    except FileNotFoundError:
                        zipper.write(operating.path.join(path, csvFile1))
                    operating.remove(root1 + '/' + csvFile1)
    zipper.close()
    return send_file("reports.zip", as_attachment=True, mimetype='zip', cache_timeout=100,
                     attachment_filename='reports.zip')


@app.route('/report', methods=['POST', 'GET'])
@nocache
def reporting():
    if request.method == 'GET':
        return "i am up"
    elif request.method == 'POST':
        try:
            start = request.form.get('start')
            end = request.form.get('end')
            agentId = request.form.get('agentId').split(',')
            type = request.form.get('type')
            timeZone = request.form.get('timezone')
        except Exception as e:
            response = json.dumps({"errorText": str(e)})
            return response

        fmt = "%Y-%m-%d %H:%M:%S %Z%z"
        my_timezone = timezone(timeZone)

        startDate = my_timezone.localize(dt.datetime.strptime(start + ' 00:00:00', "%Y-%m-%d %H:%M:%S")).astimezone(
            timezone('UTC'))
        endDate = my_timezone.localize(dt.datetime.strptime(end + ' 23:59:59', "%Y-%m-%d %H:%M:%S")).astimezone(
            timezone('UTC'))

        try:
            agentzFingerPrint = list(map(int, configFile["analysis"]["fingerprint"].split(',')))
        except ValueError:
            agentzFingerPrint = []

        if env != "dev":
            for fingerprint in pd.read_excel("./Fingerprint.xlsx")["Finger Print"]:
                agentzFingerPrint.extend(fingerprint.split(','))

            agentzFingerPrint = list(set(agentzFingerPrint))
        sessionDF = Session(agentId, startDate, endDate, agentzFingerPrint, my_timezone, fmt, env)
        Message(agentId, sessionDF, startDate, endDate, agentzFingerPrint, my_timezone, fmt)
        # thisMonth = Session(agentId, startDate - dt.timedelta(weeks=4), startDate - dt.timedelta(seconds=5),
        #                     agentzFingerPrint, my_timezone, fmt, env)
        try:
            statistics = analysis(str(startDate).split()[0], str(endDate).split()[0], agentId)
        except ZeroDivisionError:
            statistics = json.dumps({"errorText": "Zero division error"})

        except SyntaxError as e:
            print(e)
            statistics = json.dumps({"errorText": "No valid data"})

        if type is None:
            return redirect(url_for('downloadFile', start=str(startDate).split()[0], end=str(endDate).split()[0]))
        else:
            paths = ['Session_report', 'Message_report', 'Statistics_report']
            for path in paths:
                for root1, dirs1, files1 in operating.walk(path):
                    for csvFile1 in files1:
                        if csvFile1 == path.split('_')[0] + "Details_{}to{}.csv".format(str(startDate).split()[0],
                                                                                        str(endDate).split()[0]):
                            operating.remove(root1 + '/' + csvFile1)
            return Response(statistics, mimetype='application/json')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9091)
