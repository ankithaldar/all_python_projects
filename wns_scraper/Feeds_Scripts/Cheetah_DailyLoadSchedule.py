'''This program Control All the process
If dates are are correct
Control the flow
Control concurrancy

Version History:
Version :   1.0
Author  :   Ranveer Sing (EMEA WNS)
Date    :   April, 2018

Important:
While deploying in other desktop/server Please change below parameter
"C:/WNS/Projects/"
This is reading configuration file
All the Requered Table Definations are added at last.
re-create all mentioned table in-case dropped.
'''
import datetime
import os
import cx_Oracle
import sys
sys.path.append('../')
sys.path.append('C:/WNS/Projects/')
try:
    from Feeds_Scripts import Cheetah_Extraction_config as Conf
    # print(Conf.Cheetah_Last_Logical_Date)
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise

#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='MAIN'
ApplicationPath=Conf.ApplicationPath
Daily_Input_Files=Conf.Daily_Input_Files
LogPath=Conf.LogPath
RunningFlag=Conf.RunningFlag
OraConnectionString=Conf.OraConnectionString

Current_Date=datetime.datetime.today().date()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'Cheetah_DailyLoadSchedule_'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
LogFeed.write("Process started for {} at {}".format(Current_Date,Current_DTTM))

class VelodateCycyleDate():
    def VelodateCycyleDate(self,query,Type):
        self.query=query
        self.Type=Type
        try:
            self.con = cx_Oracle.connect(OraConnectionString)
            LogFeed.write('Database connection established successfully.. \n')
            self.cur = self.con.cursor()
            print(self.query)

            if (self.Type=='SELECT'):
                self.cur.execute(self.query)
                self.DateVal = self.cur.fetchone()
                print(str(self.DateVal))
                return str(self.DateVal[0])
            else:
                self.cur.execute(self.query)
                self.cur.execute("COMMIT")
        except cx_Oracle.DatabaseError as e:
            print('Error:' + str(e))
            LogFeed.write("\n Error:" + str(e))
        finally:
            self.con.close()

#get for cycle_date
Query="select to_char(max(CYCLE_DATE),'YYYY-MM-DD') as dt from DEPT_INTL.TCYCLE_STATUS where APPLICATION_NAME='{}' and PROCESS_NAME='{}' and SUBPROCESS_NAME='{}'and STATUS='COMPLETE'"\
    .format(APPLICATION_NAME,PROCESS_NAME,SUBPROCESS_NAME)
Ob_VelodateCycyleDate=VelodateCycyleDate()
LG=Ob_VelodateCycyleDate.VelodateCycyleDate(Query,'SELECT')
LogicalDate=datetime.datetime.strptime(LG, '%Y-%m-%d').date()
print(str(LogicalDate))
print(str(Current_Date))
if (os.path.exists(RunningFlag)):
    LogFeed.write("Extraction process exists as previous flag does exist for date " + str(LogicalDate) + " AT" + str(
        Current_DTTM))
    exit(1)
else:
    LogFeed.write(
        "Flag has been set to run \nExtraction process triggered for date " + str(LogicalDate) + " AT " + str(
            Current_DTTM))

if(LogicalDate >= Current_Date - datetime.timedelta(days=1)):
    LogFeed.write('\nExtraction for today is already completed '+str(Current_Date)+'.. \n Exiting from the script')
    print('\nExtraction for today is already completed '+str(Current_Date)+'.. \n Exiting from the script')
    if (os.path.exists(RunningFlag)):
        LogFeed.write("Remove the flag")
        os.remove(RunningFlag)
    LogFeed.close()
    exit(1)
else:
    LogFeed.write('\nStart extraction process for day ..'+str(LogicalDate)+'\n')
    print('\nSet Logical Date as {}'.format(LogicalDate+datetime.timedelta(days=1)))
    LogFeed.write('\nSet Logical Date as {}'.format(LogicalDate+datetime.timedelta(days=1)))
    with open(RunningFlag, 'w') as f:
        f.write(str(LogicalDate))

print("Starting Cheetah_Daily_Extraction process")
LogFeed.write("\nStarting Cheetah_Daily_Extraction process")
try:
    from Feeds_Scripts import Cheetah_Daily_Extraction

    LogFeed.write("\nStarting Cheetah_Daily_Insert process\n")
    print("Starting Cheetah_Daily_Insert process")
    from Feeds_Scripts import Cheetah_Daily_Insert

    LogFeed.write("Both process got completed\n")

    print("Both process got completed")
    print("update TCYCLE_STATUS tbale")
    print('read logical date from Running flag')
    with open(RunningFlag, 'r') as f:
        LogicalDate = f.readline()
    if (LogicalDate == ''):
        print("exit from the process \n no date read")
        LogFeed.write("\nexit from the process \n no date read")
        if (os.path.exists(RunningFlag)):
            print("removing running flag")
            LogFeed.write("\nRemove the flag")
            os.remove(RunningFlag)
        LogFeed.close()
        exit(1)
    else:
        LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
        print("last logical date is : " + str(LogicalDate))
    Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
        APPLICATION_NAME, PROCESS_NAME,SUBPROCESS_NAME,LogicalDate)
    Ob_VelodateCycyleDate = VelodateCycyleDate()
    Ob_VelodateCycyleDate.VelodateCycyleDate(Query, 'INSERT')

    if (os.path.exists(RunningFlag)):
        print("removing running flag")
        LogFeed.write("\nRemove the flag")
        os.remove(RunningFlag)

    LogFeed.write("Flag removed\n")
    LogFeed.close()
except ImportError as e:
    print("Error occured")
    LogFeed.write("\nerror occured while importing package {}\n".format(e))
    if (os.path.exists(RunningFlag)):
        print("removing running flag")
        LogFeed.write("\nRemove the flag")
        os.remove(RunningFlag)
    LogFeed.close()



'''
CREATE TABLE DEPT_INTL.TCYCLE_STATUS(
APPLICATION_NAME VARCHAR2(20) not null,
PROCESS_NAME VARCHAR2(30) ,
SUBPROCESS_NAME VARCHAR2(30),
CYCLE_DATE DATE not null,
STATUS VARCHAR2(10) not null,
MOD_DT_TM timestamp(0) not null,
CONSTRAINT TCYCLE_STATUS_pk primary key(APPLICATION_NAME,PROCESS_NAME,SUBPROCESS_NAME,CYCLE_DATE))


describe DEPT_INTL.T_CHEETAH_DAILY_REF_FEED_HIST
Name         Null Type          
------------ ---- ------------- 
ISSUE_ID          VARCHAR2(25)  
ISSUENAME         VARCHAR2(25)  
DT_TIMESENT       DATE          
TM_TIMESENT       VARCHAR2(10)  
MAILING_ID        VARCHAR2(25)  
SUBJECT           VARCHAR2(200) 
MAILING_NAME      VARCHAR2(200) 
FILEDATE          DATE          
MOD_DT            DATE  



describe DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED_HIST
Name         Null Type           
------------ ---- -------------- 
EVENTTYPE         VARCHAR2(50)   
DT_TIMESTAMP      DATE           
TM_TIMESTAMP      VARCHAR2(10)   
ISSUEID           VARCHAR2(55)   
USERID            VARCHAR2(3000) 
RESULTCODE        VARCHAR2(40)   
MIMETYPE          VARCHAR2(100)  
EMAIL             VARCHAR2(200)  
MAILING_NAME      VARCHAR2(1500) 
FILEDATE          DATE           
MOD_DT            DATE           

describe DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED
Name         Null Type           
------------ ---- -------------- 
EVENTTYPE         VARCHAR2(50)   
TIMESTAMP         VARCHAR2(54)   
ISSUEID           VARCHAR2(55)   
USERID            VARCHAR2(3000) 
RESULTCODE        VARCHAR2(40)   
MIMETYPE          VARCHAR2(100)  
EMAIL             VARCHAR2(200)  
MAILING_NAME      VARCHAR2(1500) 
FILEDATE          VARCHAR2(20)   
MOD_DT            VARCHAR2(20)   

describe DEPT_INTL.T_CHEETAH_DAILY_REF_FEED
Name         Null Type          
------------ ---- ------------- 
ISSUE_ID          VARCHAR2(25)  
ISSUENAME         VARCHAR2(25)  
TIMESENT          VARCHAR2(25)  
MAILING_ID        VARCHAR2(25)  
SUBJECT           VARCHAR2(200) 
MAILING_NAME      VARCHAR2(200) 
FILEDATE          VARCHAR2(20)  
MOD_DT            VARCHAR2(20)  


'''