'''This program Control All the process
If dates are correct
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

connect to SFTP server using ppk

>sftp -i rsa--pri-key-20180430-rasingh.ppk sbx_wns@195.190.24.39
><password>
'''
import datetime
import os
import cx_Oracle
import sys
sys.path.append('../')
sys.path.append('C:/WNS/Projects/')
try:
    from Feeds_Scripts import SBX_Germany_Data_config as Conf
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
Daily_Input_Files=Conf.Daily_Input_Files
LogPath=Conf.LogPath
RunningFlag=Conf.RunningFlag
OraConnectionString=Conf.OraConnectionString
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
DataDealyDays=Conf.DataDealyDays

Current_Date=datetime.datetime.today().date()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'SBX_Germany_Data_Schedule_'+str(Current_DTTM)+'.txt'
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

if(LogicalDate >= Current_Date - datetime.timedelta(days=DataDealyDays)):
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



try:
    LogFeed.write("\nStarting SBX_Germany_Data_SFTP process\n")
    print("Starting SBX_Germany_Data_SFTP process")

    from Feeds_Scripts import SBX_Germany_Data_SFTP
    print("Controlled flow")
    with open(RunningFlag, 'r') as f:
        LogicalDate1 = f.readline()
    LogicalDate1 = datetime.datetime.strptime(LogicalDate1, '%Y-%m-%d').date()
    if (LogicalDate != LogicalDate1):

        LogFeed.write("\nStarting SBX_Germany_Data_Insert process\n")
        print("Starting SBX_Germany_Data_Insert process")
        from Feeds_Scripts import SBX_Germany_Data_Insert

        print("last logical date is : " + str(LogicalDate1))
        LogFeed.write("\nlast logical date is : " + str(LogicalDate1))
        Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
            APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME, LogicalDate1)
        Ob_VelodateCycyleDate = VelodateCycyleDate()
        Ob_VelodateCycyleDate.VelodateCycyleDate(Query, 'INSERT')

        msgString = "SBX Germany Daily process has been completed for " + str(LogicalDate1) + " at " + str(
            Current_DTTM) + ".\n\nData has been loaded into DEPT_INTL.T_DE_SERVICECHARGES_HIST and DEPT_INTL.T_DE_TRANSACTIONS_HIST tables.\
                                    \n\nThis is an automated mail ! \n\n\nPlease contact WNS Starbucks EMEA Team for any queries.\n"
        Subject = "SBX Germany Daily process has been completed for " + str(LogicalDate1) + "."
        Ob_CommonFunction.SendMail(Email_TO_DL, msgString, Subject)
    else:
        msgString = "SFTP process exits as no data to read for logical date " + str(LogicalDate1+ datetime.timedelta(days=1)) + " AT" + str(Current_DTTM)
					 + "\n LogicalDate is" + str(LogicalDate) + "\n LogicalDate1 is" + str(LogicalDate1)
        Subject = "SBX Germany Data process SBX_Germany_Data_Schedule exit for logical date "+ str(LogicalDate1+ datetime.timedelta(days=1))

        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)


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
