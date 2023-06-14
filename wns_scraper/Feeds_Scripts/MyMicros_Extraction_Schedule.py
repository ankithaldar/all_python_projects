import os
import os.path
import sys
import time
import datetime
import cx_Oracle
sys.path.append('../')
sys.path.append('C:/WNS/Projects/')
try:
    from Feeds_Scripts import MyMicros_Extraction_Config as Conf
    # print(Conf.Cheetah_Last_Logical_Date)
    from Feeds_Scripts import CommonFunction
    Ob_CommonFunction = CommonFunction.SendMail()
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise
#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='MAIN'
ApplicationPath=Conf.ApplicationPath
ProcessingPath=Conf.ProcessingPath
TargetPath=Conf.TargetPath
LogPath=Conf.LogPath
urllogon = Conf.urllogon
username = Conf.username_SKS
password = Conf.password_SKS
company = Conf.company_SKS
OraConnectionString=Conf.OraConnectionString
RunningFlag=Conf.RunningFlag
DataLagDays=Conf.DataLagDays
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
Current_Date=datetime.datetime.today().date()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'mymicros_ExtractionSchedule_'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
#Check for logical date Bug in whindows task scheduler
def ValidateCycyleDate(query,Type):
    try:
        con = cx_Oracle.connect(OraConnectionString)
        LogFeed.write('\nDatabase connection established successfully.. \n')
        cur = con.cursor()
        print(query)

        if (Type=='SELECT'):
            cur.execute(query)
            DateVal = cur.fetchone()
            print(str(DateVal))
            return str(DateVal[0])
        else:
            cur.execute(query)
            cur.execute("COMMIT")
    except cx_Oracle.DatabaseError as e:
        print('Error:' + str(e))
        LogFeed.write("\n Error:' + str(e)\n")
        msgString = "\n Error:" + str(e) + "\n"
        Subject = "MyMicros process LSRMyMicros_Schedule.py DB failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        raise
    finally:
        con.close()
StartTime=datetime.datetime.now().replace(microsecond=0)
def main():
    LogFeed.write("MyMicros Process has been started at {} \n".format(datetime.datetime.now().replace(microsecond=0)))
    print("get cycle date")

    Query = "select to_char(max(CYCLE_DATE),'YYYY-MM-DD') as dt from DEPT_INTL.TCYCLE_STATUS where APPLICATION_NAME='{}' and PROCESS_NAME='{}' and SUBPROCESS_NAME ='{}'and STATUS='COMPLETE'" \
        .format(APPLICATION_NAME, PROCESS_NAME,SUBPROCESS_NAME)
    LogicalDate1 = ValidateCycyleDate(Query, 'SELECT')
    LogicalDate1 = datetime.datetime.strptime(LogicalDate1, '%Y-%m-%d').date() + datetime.timedelta(days=1)
    LogicalDate=LogicalDate1

    LogFeed.write("Process started for {} at {}".format(LogicalDate1, Current_DTTM))
    print("Process started for {} at {}".format(LogicalDate1, Current_DTTM))

    if (os.path.exists(RunningFlag)):
        LogFeed.write(
            "Extraction process exits as previous flag exists for date " + str(LogicalDate1) + " AT" + str(
                Current_DTTM))
        exit(1)
    else:
        with open(RunningFlag, 'w') as f:
            f.write(str(LogicalDate1))
        LogFeed.write(
            "Flag has been set to run \nExtraction process triggered for date " + str(LogicalDate1) + " AT " + str(
                Current_DTTM))
    # LogicalDate=LogicalDate1 - datetime.timedelta(days=1)
    # while(LogicalDate < Current_Date - datetime.timedelta(days=DataLagDays)):
    #     LogicalDate = LogicalDate + datetime.timedelta(days=1)

    if (LogicalDate - datetime.timedelta(days=1) >= Current_Date - datetime.timedelta(days=DataLagDays)):
        LogFeed.write(
            '\nExtraction for day is already completed ' + str(LogicalDate) + '.. \n Exiting from the script')
        print('\nExtraction for day is already completed ' + str(LogicalDate) + '.. \n Exiting from the script')
        if (os.path.exists(RunningFlag)):
            LogFeed.write("Remove the flag")
            os.remove(RunningFlag)
        LogFeed.close()
        exit(0)
    else:
        LogFeed.write('Start extraction process for day ..' + str(LogicalDate) + '\n')
        print('Set Logical Date as {}'.format(LogicalDate ))
        LogFeed.write('Set Logical Date as {}'.format(LogicalDate))
        with open(RunningFlag, 'w') as f:
            f.write(str(LogicalDate))
        try:
            sys.path.append('../')
            print("call Extraction process EUG")
            from Feeds_Scripts import MyMicros_Extraction_EUG
            print("call Extraction process SKS")
            from Feeds_Scripts import MyMicros_Extraction_SKS


            print("call Merger")
            from Feeds_Scripts import MyMicros_Extract_Merger
            print("call Insert")
            from Feeds_Scripts import MyMicros_Insert

            print("Update cycleDate")
            print('read logical date from Running flag')
            with open(RunningFlag, 'r') as f:
                LogicalDate = f.readline()
            if (LogicalDate == ''):
                print("exit from the process \n no date read")
                LogFeed.write("exit from the process \n no date read")
                LogFeed.close()
                exit(1)
            else:
                LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
                print("last logical date is : " + str(LogicalDate))
            Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
                APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME, LogicalDate)
            ValidateCycyleDate(Query, 'INSERT')

            if (os.path.exists(RunningFlag)):
                print("removing running flag")
                LogFeed.write("\nRemove the flag")
                os.remove(RunningFlag)
                LogFeed.write("Flag removed\n")

        except ImportError  as e:
            print("Error occured")
            LogFeed.write("\nerror occured while importing package {}\n".format(e))
    LogFeed.write("\n\nprocess completed till date : {}".format(LogicalDate))
    print("MyMicros Process Took '{}' time to complete.\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.write("MyMicros Process Took '{}' time to complete.\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.close()

main()
'''

desc  DEPT_INTL.T_MYMICROSDAILYFEEDTOTALCOUNT 
Name            Null     Type         
--------------- -------- ------------ 
APPLICATION     NOT NULL VARCHAR2(10) 
TOTAL           NOT NULL VARCHAR2(10) 
COUNT           NOT NULL VARCHAR2(10) 
COMPANY         NOT NULL VARCHAR2(5)  
EXTRACTION_DATE NOT NULL VARCHAR2(10) 

desc  DEPT_INTL.T_MYMICROSDAILYFEED 
Name            Null     Type         
--------------- -------- ------------ 
LOCATION        NOT NULL VARCHAR2(70) 
REVENUECENTER            VARCHAR2(70) 
CHECK1                   VARCHAR2(10) 
TRANSACTIONTIME NOT NULL VARCHAR2(25) 
CHECK2                   VARCHAR2(70) 
TRANSACTION              VARCHAR2(70) 
DISCOUNTTOTAL   NOT NULL VARCHAR2(10) 
CHECKTOTAL               VARCHAR2(10) 
COMPANY         NOT NULL VARCHAR2(5)  
EXTRACTION_DATE NOT NULL VARCHAR2(10) 


desc  DEPT_INTL.T_MYMICROSDAILYFEED_HIST 
Name            Null     Type         
--------------- -------- ------------ 
LOCATION        NOT NULL VARCHAR2(70) 
REVENUECENTER            VARCHAR2(70) 
CHECK1                   NUMBER(10)   
TRANSACTIONDATE NOT NULL DATE         
TRASACTIONTIME           VARCHAR2(10) 
CHECK2                   VARCHAR2(70) 
TRANSACTION              VARCHAR2(70) 
DISCOUNTTOTAL   NOT NULL FLOAT(10)    
CHECKTOTAL               FLOAT(10)    
COMPANY         NOT NULL VARCHAR2(5)  
EXTRACTION_DATE NOT NULL DATE         
MOD_DT_TM                TIMESTAMP(0) 

'''