
"""This is wrapper script used to extract MyMicros Revenue data
LSRMyMicros_Schedule.py

Version History:
Version :   2.0
Author  :   Unnati Khandelwal (EMEA WNS)
Date    :   Dec, 2018
"""

import os
import os.path
import sys
# import time
import datetime
import cx_Oracle

sys.path.append('../')
# sys.path.append('C:/WNS/Feeds_Scripts')
try:
    from Feeds_Scripts import CHATMyMicros_Conf as Conf
    from Feeds_Scripts import CommonFunction
    Ob_CommonFunction = CommonFunction.SendMail()
    # print(Conf.Cheetah_Last_Logical_Date)
    # from Feeds_Scripts import LSRMyMicros_Extract_Merger as Merger
except ImportError as e:
    print("Error occurred while reading the config file:" + e)
    raise
# below parameters are getting read from configuration file
APPLICATION_NAME = Conf.APPLICATION_NAME
PROCESS_NAME = Conf.PROCESS_NAME
SUBPROCESS_NAME = 'MAIN'
ApplicationPath = Conf.ApplicationPath
ProcessingPath = Conf.ProcessingPath
TargetPath = Conf.TargetPath
LogPath = Conf.LogPath
OraConnectionString = Conf.OraConnectionString
RunningFlag = Conf.RunningFlag
DataLagDays = Conf.DataLagDays
Email_TO_DL = Conf.Email_TO_DL
Alert_TO_DL = Conf.Alert_TO_DL

Current_Date = datetime.datetime.today().date()
Current_DTTM = datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile = LogPath+'CHATMyMicros_Schedule'+str(Current_DTTM)+'.txt'
LogFeed = open(LogFile, 'w')

# Check for logical date Bug in windows task scheduler

def ValidateCycyleDate(Query, Type):
    try:
        con = cx_Oracle.connect(OraConnectionString)
        LogFeed.write('\nDatabase connection established successfully.. \n')
        cur = con.cursor()
        print(Query)

        if (Type=='SELECT'):
            cur.execute(Query)
            DateVal = cur.fetchone()
            print(str(DateVal))
            return str(DateVal[0])
        else:
            cur.execute(Query)
            cur.execute("COMMIT")
    except cx_Oracle.DatabaseError as e:
        print('Error:' + str(e))
        LogFeed.write("\n Error:' + str(e)\n")
        msgString = "\n Error:" + str(e)+"\n"
        Subject = "CHATMyMicros process CHATMyMicros_Schedule.py DB failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        raise
    finally:
        con.close()

StartTime = datetime.datetime.now().replace(microsecond=0)


def main():
    LogFeed.write("CHATMyMicros_Schedule Process has been started at {} \n".format(datetime.datetime.now().replace(microsecond=0)))
    print("get cycle date")

    Query = "select to_char(max(CYCLE_DATE),'YYYY-MM-DD') as dt from DEPT_INTL.TCYCLE_STATUS_CHAT where APPLICATION_NAME='{}' and PROCESS_NAME='{}' and SUBPROCESS_NAME ='{}'and STATUS='COMPLETE'"\
    .format(APPLICATION_NAME, PROCESS_NAME,SUBPROCESS_NAME)
    LogicalDate1 = ValidateCycyleDate(Query, 'SELECT')
    print('Set Logical Date1 as {}'.format(LogicalDate1 ))
    LogicalDate1 = datetime.datetime.strptime(LogicalDate1, '%Y-%m-%d').date() + datetime.timedelta(days=1)
    LogicalDate=LogicalDate1
    print('Set Logical Date as {}'.format(LogicalDate ))
    LogFeed.write("Process started for {} at {}".format(LogicalDate1, Current_DTTM))
    print("Process started for {} at {}".format(LogicalDate1, Current_DTTM))

    if (os.path.exists(RunningFlag)):
        LogFeed.write(
            "Extraction process exits as previous flag exists for date " + str(LogicalDate1) + " AT" + str(
                Current_DTTM))
        msgString="Extraction process exits as previous flag exists for date " + str(LogicalDate1) + " AT" + str(Current_DTTM)
        Subject="CHATMyMicros process CHATRMyMicros_Schedule.py failed"

        Ob_CommonFunction.SendMail(Alert_TO_DL,msgString,Subject)

        exit(1)
    else:
        with open(RunningFlag, 'w') as f:
            f.write(str(LogicalDate1))
        LogFeed.write(
            "Flag has been set to run \nExtraction process triggered for date " + str(LogicalDate1) + " AT " + str(
                Current_DTTM))


    if (LogicalDate - datetime.timedelta(days=1) >= Current_Date - datetime.timedelta(days=DataLagDays)):
        LogFeed.write(
            '\nExtraction for day is already completed ' + str(LogicalDate) + '.. \n Exiting from the script')
        print('\nExtraction for day is already completed ' + str(LogicalDate) + '.. \n Exiting from the script')
        if (os.path.exists(RunningFlag)):
            LogFeed.write("Remove the flag")
            os.remove(RunningFlag)
            msgString = 'Extraction for day is already completed ' + str(LogicalDate) + '.. \n Exiting from the script \n No action required'
            Subject = "MyMicros process CHATMyMicros_Schedule.py "
            Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
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

            print("call Extraction process CHAT MyMicros")
            from Feeds_Scripts import CHATMyMicros_Extraction_iCare
            print("call Merger")
            from Feeds_Scripts import CHATMyMicros_Extract_Merger
            print("call Insert")
            from Feeds_Scripts import CHATMyMicros_Insert
            print("Update cycleDate")
            print('read logical date from Running flag')
            with open(RunningFlag, 'r') as f:
                LogicalDate = f.readline()
            if (LogicalDate == ''):
                print("exit from the process \n no date read")
                LogFeed.write("exit from the process \n no date read")
                msgString = "exit from the process \n no date read"
                Subject = "MyMicros process CHATMyMicros_Schedule.py failed "
                Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
                LogFeed.close()
                exit(1)
            else:
                LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
                print("last logical date is : " + str(LogicalDate))
            Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS_CHAT VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
                APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME, LogicalDate)
            ValidateCycyleDate(Query, 'INSERT')

            if (os.path.exists(RunningFlag)):
                print("removing running flag")
                LogFeed.write("\nRemove the flag")
                os.remove(RunningFlag)
                LogFeed.write("Flag removed\n")

        except ImportError as e:
            print("Error occurred")
            LogFeed.write("\nerror occurred while importing package {}\n".format(e))
    LogFeed.write("\n\nprocess completed till date : {}".format(LogicalDate))
    print("CHATMyMicros Process Took '{}' time to complete.\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.write("CHATMyMicros Process Took '{}' time to complete.\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.close()

main()
'''

  create table DEPT_INTL.T_CHATMYMICROSDAILYFEED
  (
  Card_Program  varchar2(10),
  Gender varchar2(10),
  Language varchar2(10),
  Not_Activated_Cnt FLOAT(10),
  Deleted_Cnt FLOAT(10),
  Active_Cnt FLOAT(10),
  Welcome_Status_Cnt FLOAT(10),
  Welcome_Status_Stars FLOAT(10),
  Green_Status_Cnt FLOAT(10),
  Green_Status_Stars FLOAT(10),
  Gold_Status_Cnt FLOAT(10),
  Gold_Status_Stars FLOAT(10),
  Email_Special_Offers_Cnt FLOAT(10),
  Email_Newsletter_Cnt FLOAT(10),
  SMS_Special_Offers_Cnt FLOAT(10),
  mymicros_file_date date,
  mod_date date
  )

  
  create table DEPT_INTL.T_MYMICROSDAILYFEEDAVG_HIST
  (
  Card_Program  varchar2(10),
  Gender varchar2(10),
  Language varchar2(10),
  Active_Cnt FLOAT(10),
  Welcome_Status_Cnt FLOAT(10),
  Welcome_Status_Stars FLOAT(10),
  Green_Status_Cnt FLOAT(10),
  Green_Status_Stars FLOAT(10),
  Gold_Status_Cnt FLOAT(10),
  Gold_Status_Stars FLOAT(10),
  Email_Special_Offers_Cnt FLOAT(10),
  Email_Newsletter_Cnt FLOAT(10),
  SMS_Special_Offers_Cnt FLOAT(10),
  mymicros_file_date date,
  mod_date date
  )
  
'''
