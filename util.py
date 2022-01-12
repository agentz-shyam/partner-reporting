domainId = "SELECT business_agent_id FROM `report_master` WHERE domain_id = %s"

# masterSlaveID = "SELECT DISTINCT business_agent_id FROM `conversation` WHERE master_business_agent_mapping_id = %s"
masterSlaveID = "SELECT id FROM business_agent_mapping where master_business_agent_mapping_id=%s"

concurrent = "SELECT sessioncount FROM report_event AS a NATURAL JOIN concurrent_session AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' ORDER BY a.reporting_date ASC"

stats = "SELECT * FROM report_event AS a NATURAL JOIN statistics_value AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' ORDER BY a.reporting_date ASC"

uniqueUser = "SELECT  COUNT(DISTINCT fingerprint) FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' ORDER BY a.reporting_date ASC"

newUser = "SELECT  COUNT(DISTINCT fingerprint) FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' and b.newuser = 1 ORDER BY a.reporting_date ASC"

taskcounts = "SELECT  taskid, taskname, taskcount FROM report_event AS a NATURAL JOIN task_count AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' AND b.taskid in {} ORDER BY a.reporting_date ASC"

exitcounts = "SELECT  taskid, finaltask, taskcount FROM report_event AS a NATURAL JOIN exiting_task AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' AND b.taskid in {} ORDER BY a.reporting_date ASC"

bouncerate = "SELECT  taskid, bouncetask, numeratorvalue, denominatorvalue FROM report_event AS a NATURAL JOIN bounce_rate AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' ORDER BY a.reporting_date ASC"

OSEngagement = "SELECT  COUNT(DISTINCT fingerprint) AS count, os FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' group by b.os "

deviceEngagement = "SELECT  COUNT(DISTINCT fingerprint) AS count, devicename FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' group by b.devicename "

browserEngagement = "SELECT  COUNT(DISTINCT fingerprint) AS count, browsername FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS' group by b.browsername "

settings = "SELECT settings FROM report_master where business_agent_id = %s"

agentDomainId = "SELECT domain_id FROM `report_master` WHERE business_agent_id= '{}'"

userFlag = "SELECT  DISTINCT fingerprint FROM report_event AS a NATURAL JOIN engagement AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s"

statusCheck = "select status from report_event as a natural join report_master as b where b.business_agent_id = %s and a.reporting_date = %s"

nlquerycounts = "SELECT SUM(nlquery) FROM report_event AS a NATURAL JOIN statistics_value AS b NATURAL JOIN report_master AS c WHERE c.business_agent_id = %s AND a.reporting_date >= %s AND a.reporting_date <= %s AND a.status = 'SUCCESS'"

conversationIdQuery = '''
        select id from conversation where 
        channel = '{}' and 
        business_agent_id = '{}';
    '''
webChatconversationIdQuery = '''
        select id from conversation where 
        channel = '{}' and 
        business_agent_id = '{}' and
        created_date >= '{}' and
        created_date <= '{}'
        ;
    '''
webChatTrafficQuery = '''
        select count(*) as totalTraffic from conversation where 
        channel = 'DIRECT' and 
        business_agent_id = '{}' and 
        created_date >= '{}' and 
        created_date <= '{}'    
    '''
effectiveSessionsQuery = '''
        select count(distinct(m.conversation_id)) as effectiveSessions from conversation as c
        inner join message as m on (c.id = m.conversation_id)
        where 
        c.channel = 'DIRECT' and 
        c.business_agent_id = '{}' and 
        c.created_date >= '{}' and 
        c.created_date <= '{}'
'''   

eventsQuery = '''
        select created_date,event_type from conversation_event where 
        conversation_id = '{}' 
        and created_date >= '{}' 
        and created_date <= '{}'
        order by created_date asc;
    ''' 

deleteFailedEntriesQuery = "DELETE FROM {} where id = '{}' and reporting_date = '{}'"    