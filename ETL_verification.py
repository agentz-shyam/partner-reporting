# import datetime as dt
import json
from configparser import ConfigParser

import pandas as pd
import redis
import requests

configFile = ConfigParser()


class searchDetails:
    def __init__(self):
        self.names = ['@business.Name']
        self.language = "EN_US"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


_businessesDict = dict()
_percentageChanges = dict()

class ETL_verification:
    def __init__(self, env):
        configFile.read(env + '_config.ini')
        # self._defaultBusinessNames = []
        self._env = env
        self._allowedBusiness = open("{}_allowedBusiness.txt".format(self._env)).read().splitlines()
        self._allowedBusinessNames = dict()
        self._searchDict = searchDetails().toJSON()
        self._headers = {"Authorization": "93cc004b-8b15-4776-9cb8-b7410360b61a", "Content-type": "application/json"}
        self.redis_connec = redis.StrictRedis(host=configFile['verification']['elasticCache'], port=6379, db=0)
        self.redis_connec.set("allowedBusiness", self._allowedBusiness)
#         for id in self._allowedBusiness:
#             searchAPI = "https://" + self._env + "-business.agentz.ai/api/business/v1/agentvalues/{}/search".format(id)
#             try:
#                 response = json.loads(
#                     requests.post(searchAPI, data=self._searchDict, headers=self._headers).content.decode('utf-8'))
#                 self._allowedBusinessNames[id] = response['agentValues'][0]['value'][0]
#             except Exception as e:
#                 self._allowedBusinessNames[id] = None
#         self.redis_connec.set("allowedBusinessNames", self._allowedBusinessNames)

    def status_entry(self, db, pkId, agentId, reportingDate, logger):
        try: 
            logger.info("Entry process started for {}".format(agentId))
#             if agentId in self._allowedBusiness:
            searchAPI = "https://" + self._env + "-business.agentz.ai/api/business/v1/agentvalues/{}/search".format(agentId)
            try:
                response = json.loads(
                    requests.post(searchAPI, data=self._searchDict, headers=self._headers).content.decode('utf-8'))
                self._allowedBusinessNames[agentId] = response['agentValues'][0]['value'][0]
            except Exception as e:
                self._allowedBusinessNames[agentId] = None
            if agentId in _businessesDict:
                _businessesDict[agentId].append(str(reportingDate))
            else:
                _businessesDict[agentId] = [str(reportingDate)]
            logger.info("Added in businessesDict")

            query = "select  one_tasksession as engagement,sessions_offhours as offhour_engagement," \
                    "sessions_loaded as total_sessions,completedconversion_task as conversion,phonecalls as phone_call " \
                    "from statistics_value where id={} order by reporting_date desc".format(pkId)

            tableData = pd.read_sql_query(query, db)
            tableData.replace(0, 0.1, inplace=True)
            changesDf = tableData.pct_change().iloc[1, :]
            changesDf.dropna(inplace=True)

            _temp_name = self._allowedBusinessNames[agentId]
            _percentageChanges[_temp_name + '_' + str(reportingDate)] = dict(changesDf.multiply(100).round(2))
            logger.info("Percentage changes has been calculated")
            self.redis_connec.set("businessesDict", _businessesDict)
            self.redis_connec.set("percentageChanges", _percentageChanges)
        except Exception as e:
            logger.info(e)
            import pdb;pdb.set_trace()    
#         else:
#             logger.info("Given id {} is not in businessDict".format(agentId))

#     def process_check(self):
#         actualDict = dict.fromkeys(self._allowedBusiness)
#         temp = dict(filter(lambda elem: str(dt.date.today()) in elem[1], self._businessesDict.items()))
#         actualDict.update(temp)
#
#         self._ETL_default = dict(filter(lambda elem: not elem[1], actualDict.items()))
#
#         for id, _ in self._ETL_default.items():
#             self._defaultBusinessNames.append(self._allowedBusinessNames.get(id))
#
#         self._mail_data()
#
#     def _mail_data(self):
#         failedbusinesses = ''
#         for name in self._defaultBusinessNames:
#             failedbusinesses += "<li> {} </li>".format(name)
#
#         abnormalCounts = ''
#         for name, changesDict in self._percentageChanges.items():
#             dataDes = []
#             for desc, value in changesDict.items():
#                 if abs(value) >= 25:
#                     if value > 0:
#                         word = "{} has increased by {}% of yesterday's value.".format(desc.replace("_", " "), value)
#                     else:
#                         word = "{} has decreased by {}% of yesterday's value.".format(desc.replace("_", " "), value)
#                     dataDes.append(word)
#             abnormalCounts += "<li> {} </li>".format(name+':-'+"<ul>{}</ul>".format("</li>".join('<li>'+
#             data for data in dataDes)))
#
#         params = {'date': str(dt.date.today()), 'active': len(self._allowedBusiness), 'abnormalCounts':abnormalCounts,
#         'failed': len(self._ETL_default), 'success': len(self._allowedBusiness)-len(self._ETL_default),
#         'failedBusiness':failedbusinesses}
#
#         htmlContent = open("mailTemplate.txt").read().format_map(params)
#         toId = [configFile["verification"]["to"]]
#         bccIds = [configFile["verification"]["bcc"]]
#
#         alert = mailtDetails(toId, bccIds, htmlContent)
#         alertURL = "https://" + self._env + '-api.agentz.ai/notification/v1/email'
#
#         try:
#             response = requests.post(alertURL, data=alert.toJSON(), headers=self._headers)
#             return response.status_code
#         except Exception as e:
#             return str(e)
#
# schedule.every().day.at("17:30").do(ETL_verification('dev').process_check)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
