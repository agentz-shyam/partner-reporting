import pymysql
import pandas as pd
from datetime import datetime
from datetimerange import DateTimeRange
import configparser
import util

class MultipleChannelPerformance:
    def __init__(self, startDate, endDate, businessHoursStart, businessHoursEnd,env,logger):
        self.logger=logger
        self.env = env
        self.startDate = datetime.strftime(startDate, "%Y-%m-%d %H:%M:%S")
        self.endDate = datetime.strftime(endDate, "%Y-%m-%d %H:%M:%S")
        self.businessHoursStart = datetime.strftime(businessHoursStart, "%Y-%m-%d %H:%M:%S")
        self.businessHoursEnd = datetime.strftime(businessHoursEnd, "%Y-%m-%d %H:%M:%S")
        self.configFile = configparser.ConfigParser()
        self.configFile.read(env + '_config.ini')
        self.db = self.configFile["analysis"]["consumerDatabase"]
        
        
        self.conn = pymysql.connect(host=self.configFile["analysis"]["host"], user=self.configFile["analysis"]["user"],
                                   passwd=self.configFile["analysis"]["pswd"], port=3306,db = self.db) 
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def conversation_id(self,businessAgentMappingId):
        self.businessAgentMappingId = businessAgentMappingId

        switcher = {'DIRECT' : 'webChat','FACEBOOK' : 'facebook','SMS' : 'sms','BUSINESS_MESSAGES' : 'googleMessage'}
        allChannelEngagementDetails = {}

        for channel in switcher.keys():
            try:
                if channel == 'DIRECT':
                    self.cur.execute(util.webChatconversationIdQuery.format(channel,self.businessAgentMappingId,self.startDate,self.endDate)) 
                    conversationIds = self.cur.fetchall()
                    conversationIds = [conversationId['id'] for conversationId in conversationIds]
                else:
                    self.cur.execute(util.conversationIdQuery.format(channel,self.businessAgentMappingId)) 
                    conversationIds = self.cur.fetchall()
                    conversationIds = [conversationId['id'] for conversationId in conversationIds]

                if conversationIds:
                    
                    engagementData = [self.getEngagementCount(conversationId) for conversationId in conversationIds]
                    engagement,offHourEngagement = list(map(sum, zip(*engagementData)))
                    channel = switcher.get(channel)
                    if channel == 'webChat':
                        self.cur.execute(
                            util.webChatTrafficQuery.format(
                                self.businessAgentMappingId,
                                self.startDate,
                                self.endDate
                                ))

                        trafficResponse = self.cur.fetchone()
                        
                        self.cur.execute(
                            util.effectiveSessionsQuery.format(
                                self.businessAgentMappingId,
                                self.startDate,
                                self.endDate
                                ))
                        sessionResponse = self.cur.fetchone()

                        allChannelEngagementDetails[f'{channel}'] = {}
                        allChannelEngagementDetails[f'{channel}'].update({
                                    "engagement": sessionResponse['effectiveSessions'],
                                    "offHourEngagement": offHourEngagement,
                                    "sessions": trafficResponse['totalTraffic'],
                                    "discardedSessions" : trafficResponse['totalTraffic'] - sessionResponse['effectiveSessions'],
                                    "contacts" : 0
                                })
                    else:
                        allChannelEngagementDetails[f'{channel}'] = {}
                        allChannelEngagementDetails[f'{channel}'].update({
                           
                                    "engagement": engagement,
                                    "offHourEngagement": offHourEngagement,
                                    "sessions": engagement,
                                    "discardedSessions" : 0,
                                    "contacts" : 0
                                })             
                else:
                    channel = switcher.get(channel)
                    allChannelEngagementDetails[f'{channel}'] = {}
                    allChannelEngagementDetails[f'{channel}'].update({
                                "engagement": 0,
                                "offHourEngagement": 0,
                                "sessions": 0,
                                "discardedSessions" : 0,
                                "contacts" : 0
                            }) 
            except Exception as e:
                self.logger(f"Error while calculating getting {channel} engagement count values")
                               
        
        return allChannelEngagementDetails

    def getEngagementCount(self, conversationId):
        try:        
            date_format_str = '%Y-%m-%d %H:%M:%S'
            self.conversationId = conversationId
            engagementCount = 1
            offHourEngagementCount = 0
            df = pd.read_sql_query(util.eventsQuery.format(self.conversationId,self.startDate,self.endDate),self.conn)
            if not df.empty:
                df['created_date'] = df['created_date'].dt.strftime(date_format_str)
                indexes = [0]

                for row in df.itertuples():
                    if row[0] == 0:
                        start = datetime.strptime(str(df['created_date'][row[0]]), date_format_str)
                    else:
                        start = datetime.strptime(str(df['created_date'][row[0] - 1]), date_format_str)                                             
                    end =   datetime.strptime(str(row[1]), date_format_str)
                    diff = end - start
                    diff_in_minutes = round(diff.total_seconds() / 60,2)
                    if diff_in_minutes  >= 30:
                        start = datetime.strptime(str(row[1]), date_format_str)
                        indexes.append(row[0])
                        engagementCount = engagementCount + 1
                    # print("{} - {} -> {} minutes".format(end, start, diff_in_minutes))
                indexes.append(len(df))
                # print(indexes)
                for i in range(0, len(indexes) - 1):
                    businessHours = DateTimeRange(self.businessHoursStart, self.businessHoursEnd)
                    tempDf = df[indexes[i]:indexes[i + 1]]['created_date']
                    sessionStartTime = tempDf.head(1).values[0]
                    sessionEndTime = tempDf.tail(1).values[0]

                    if sessionStartTime not in businessHours and sessionEndTime not in businessHours:
                        offHourEngagementCheck = ["offHour" for t in tempDf.values if t not in businessHours]
                    else: 
                        offHourEngagementCheck = []   
                    try :
                        if len(offHourEngagementCheck) != 0:
                            offHourEngagementCount = offHourEngagementCount + 1
                    except Exception as e:
                        self.logger(f"Error {e}")
                return engagementCount,offHourEngagementCount 
            else:
                return 0,0

        except Exception as e:
            self.logger.info(" Error while calculating engagement count")
                    