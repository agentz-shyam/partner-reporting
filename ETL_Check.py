import datetime as dt
import sys
import time
from configparser import ConfigParser

import redis
import requests
import schedule
import yaml

from alerting import mailtDetails

configFile = ConfigParser()
env = sys.argv[-1].lower()
configFile.read(env + '_config.ini')


class ETL_Check:
    def __init__(self):
        self._headers = {"Authorization": "93cc004b-8b15-4776-9cb8-b7410360b61a", "Content-type": "application/json"}

    def process_check(self):
        self.defaultBusinessNames = []
        self.redis_connec = redis.StrictRedis(host=configFile['verification']['elasticCache'], port=6379, db=0)
        self.allowedBusiness = yaml.load(self.redis_connec.get("allowedBusiness").decode('utf8'))
        self.businessesDict = yaml.load(self.redis_connec.get("businessesDict").decode('utf8'))
        self.allowedBusinessNames = yaml.load(self.redis_connec.get("allowedBusinessNames").decode('utf8'))
        self.percentageChanges = yaml.load(self.redis_connec.get("percentageChanges").decode('utf8'))
        actualDict = dict.fromkeys(self.allowedBusiness)
        temp = dict(
            filter(lambda elem: str(dt.date.today() - dt.timedelta(days=1)) in elem[1], self.businessesDict.items()))
        actualDict.update(temp)

        self._ETL_default = dict(filter(lambda elem: not elem[1], actualDict.items()))

        for id, _ in self._ETL_default.items():
            if id in self.allowedBusiness:
                self.defaultBusinessNames.append(self.allowedBusinessNames.get(id))

        self._mail_data()

    def _mail_data(self):
        failedbusinesses = ''
        for name in self.defaultBusinessNames:
            failedbusinesses += "<li> {} </li>".format(name)

        abnormalCounts = ''
        for ori, changesDict in self.percentageChanges.items():
            dataDes = []
            name = ori.split('_')
            flag = 0
            if str(dt.date.today() - dt.timedelta(days=1)) in name[1]:
                for desc, value in changesDict.items():
                    if abs(value) >= 25:
                        if value > 0:
                            word = "{} has increased by {}% of yesterday's value.".format(desc.replace("_", " "), value)
                        else:
                            word = "{} has decreased by {}% of yesterday's value.".format(desc.replace("_", " "), value)
                        dataDes.append(word)
                        flag = 1
                if flag:
                    abnormalCounts += "<li> {} </li>".format(
                        name[0] + ':-' + "<ul>{}</ul>".format("</li>".join('<li>' + data
                                                                                                     for data in
                                                                                                     dataDes)))

        params = {'date': str(dt.date.today()), 'active': len(self.allowedBusiness), 'failed': len(self._ETL_default),
                  'success': len(self.allowedBusiness) - len(self._ETL_default), 'failedBusiness': failedbusinesses,
                  'abnormalCounts': abnormalCounts}
        styleContent = open('mailTemplateStyle.txt').read()
        htmlContent = styleContent + open("mailTemplate.txt").read().format_map(params)
        toId = [configFile["verification"]["to"]]

        bccIds = [configFile["verification"]["bcc"]]
        if configFile["verification"]["bcc"] == "":
            bccIds = []

        alert = mailtDetails(toId, bccIds, htmlContent)
        alertURL = "https://" + env + '-api.agentz.ai/notification/v1/email'

        try:
            response = requests.post(alertURL, data=alert.toJSON(), headers=self._headers)
            return response.status_code
        except Exception as e:
            return str(e)


schedule.every().day.at("12:05").do(ETL_Check().process_check)

while True:
    schedule.run_pending()
    time.sleep(1)
