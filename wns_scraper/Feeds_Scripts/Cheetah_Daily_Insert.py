'''This program Insert Extracted .Zip file into Oracle DB

Version History:
Version :   1.0
Author  :   Ranveer Sing (EMEA WNS)
Date    :   April, 2018

Important:
While deploying in other desktop/server Please change below parameter
"C:/WNS/Projects/"
This is reading configuration file
'''
import pandas
import gzip
import cx_Oracle
import unicodedata
import re
import datetime
import time
import os
import smtplib
import sys
import math
from email.mime.text import MIMEText
from pathlib import Path, PureWindowsPath

try:
    from Feeds_Scripts import Cheetah_Extraction_config as Conf
    # print(Conf.Cheetah_Last_Logical_Date)
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise

#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='LOAD'
ApplicationPath=Conf.ApplicationPath
Daily_Input_Files=Conf.Daily_Input_Files
LogPath=Conf.LogPath
Archive_Path=Conf.Archive_Path
RunningFlag=Conf.RunningFlag
SplitValue=Conf.SplitValue
OraConnectionString=Conf.OraConnectionString
Email_TO_DL=Conf.Email_TO_DL

# LogicalDate=datetime.datetime.strptime(Conf.Cheetah_Last_Logical_Date,'%Y%m%d').date()
Current_Date=datetime.date.today().isoformat()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
CurrDate=datetime.datetime.today().date()
LogFile=LogPath+'Cheetah_Daily_Insert'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
LogFeed.write("Process started for {} at {}".format(Current_Date,Current_DTTM))
LogFile=open(LogFile,'w')


class ConnectOraExecute():
    def ConnectOraExecute(self):
        self.RetryCon=0
        while(self.RetryCon < 5):
            print("re-trying connection at {} times".format(self.RetryCon))
            try:
                self.con = cx_Oracle.connect(OraConnectionString)
                LogFile.write('Database connection established successfully.. \n')
                self.cur = self.con.cursor()
                if not self.DataSet:
                    # print("empty set \n"+str(self.Query)+"\n\n\n"+str(self.DataSet))
                    self.cur.execute(self.Query)
                    self.cur.execute("COMMIT")
                    self.cur.close()
                    self.con.close()

                else:
                    self.cur.executemany(self.Query, self.DataSet)
                    self.cur.execute("COMMIT")
                    self.cur.close()
                    self.con.close()
                    print("Successfully loaded and commit")

                LogFile.write('Query Executed successfully:\n {}\n'.format(self.Query))
                self.RetryCon = 99
                print("Break on successful with value of RetryCon : {}".format(self.RetryCon))


            except cx_Oracle.DatabaseError as e:
                print("Incrementing the value")
                self.RetryCon = self.RetryCon+ 1
                LogFile.write('Error:' + str(e))
                print('Error:' + str(e))
                print("\n Try again to re-connect DB in 60 secs\n")
                LogFile.write("\n Try again to re-connect DB in 60 secs\n")
                time.sleep(60)

            finally:
                LogFile.write('\nClosing Connection\n')
                # self.con.commit()
                # self.con.close()
class CountValidLoadOra():
    def CountValidLoadOra(self):
        try:
            self.con = cx_Oracle.connect(OraConnectionString)
            LogFile.write('Database connection established successfully.. \n')
            self.cur = self.con.cursor()
            self.cur.execute(self.Query)
            self.LoadedCount=self.cur.fetchall()
            self.cur.close()
            self.con.close()
            if(self.LoadedCount[0][0] != self.FileRowCount):
                print("Count does not match \n failing the process")
                LogFile.write("\nCount does not match \n failing the process\n")
                Ob_SendMail1 = SendMail()
                LogFile.write("\n Count does not match")
                Ob_SendMail1.msgString = "Cheetah Mail File  Count does not match    "
                Ob_SendMail1.Subject = "Failed ! Cheetah Mail  Validation process" + str(Current_Date) + "."
                Ob_SendMail1.SendMail()
                exit(1)
            else:
                print("\nCount matched , Table count is : {} and FileCount is : {} .\n".format(self.LoadedCount[0][0],self.FileRowCount))
                LogFile.write("\nCount matched , Table count is : {} and FileCount is : {} .\n".format(self.LoadedCount[0][0],self.FileRowCount))

        except cx_Oracle.DatabaseError as e:
            Ob_SendMail1 = SendMail()
            LogFile.write("\n Count does not match")
            Ob_SendMail1.msgString = "Cheetah Mail File  Count does not match   "
            Ob_SendMail1.Subject = "Failed ! Cheetah Mail  Validation process" + str(e) + "."
            Ob_SendMail1.SendMail()
            raise


class PrepareQueryToExc():

    def TempTruncData(self):
        print("Inside TempTruncData")
        if (self.InstanceChk == 'REF'):
            self.Query = 'truncate table DEPT_INTL.T_CHEETAH_DAILY_REF_FEED'
        elif (self.InstanceChk == 'KPI'):
            self.Query = 'truncate table DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED'
        else:
            print("No Instance found.. Fail the process")
        LogFile.write("TempTruncData Running for " + str(self.InstanceChk) + "\n")
        Ob_ConnectOraExecute2 = ConnectOraExecute()
        Ob_ConnectOraExecute2.Query = self.Query
        Ob_ConnectOraExecute2.DataSet = []
        Ob_ConnectOraExecute2.ConnectOraExecute()
        # self.cur.execute(Query)
        # self.cur.execute("COMMIT")
    def DeltaLoadTable(self):
        print("inside DeltaLoadTable")
        LogFile.write("inside DeltaLoadTable\n")
        if (self.InstanceChk == 'REF'):
            self.Query = '''insert into DEPT_INTL.T_CHEETAH_DAILY_REF_FEED(ISSUE_ID,ISSUENAME,TIMESENT,MAILING_ID,SUBJECT,MAILING_NAME,FILEDATE,MOD_DT)
            values (:1,:2,:3,:4,:5,:6,:7,:8)'''
            self.ExtractCount='''select count(*) from DEPT_INTL.T_CHEETAH_DAILY_REF_FEED'''
        elif (self.InstanceChk == 'KPI'):
            self.Query = '''insert into DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED (EVENTTYPE,TIMESTAMP,ISSUEID,USERID,RESULTCODE,MIMETYPE,EMAIL,mailing_name,FILEDATE,MOD_DT) 
            values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)'''
            self.ExtractCount = '''select count(*) from DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED'''
        else:
            print("No Instance found.. Fail the process")

        LogFile.write("DeltaLoadTable Running for "+str(self.InstanceChk) +"\n")
        Ob_ConnectOraExecute1=ConnectOraExecute()
        Ob_ConnectOraExecute1.Query=self.Query
        # print("fffff"+str(self.DataSet))
        print("Total lenght of data set is :"+str(len(self.DataSet)))
        if (len(self.DataSet) > SplitValue):
            print("split the dataset inside DeltaLoadTable")
            temp = 0
            for i in range(math.ceil(len(self.DataSet) / SplitValue)):
                print("Looping inside deltaload loop is :{} out of {}".format(i , math.ceil(len(self.DataSet) / SplitValue)))
                startpoint = temp
                endpoint = startpoint + SplitValue
                temp = endpoint
                self.DataSet1 = self.DataSet[startpoint:endpoint]
                Ob_ConnectOraExecute1.DataSet = self.DataSet1
                print("Dataset lenght after slicing is start point: {} end point {}".format(startpoint,endpoint))
                Ob_ConnectOraExecute1.ConnectOraExecute()
        else:
            print("No split required")
            Ob_ConnectOraExecute1.DataSet = self.DataSet
            Ob_ConnectOraExecute1.ConnectOraExecute()
        print("Velidation ...")
        Ob_CountValidLoadOra=CountValidLoadOra()
        Ob_CountValidLoadOra.FileRowCount=len(self.DataSet)
        Ob_CountValidLoadOra.Query = self.ExtractCount
        Ob_CountValidLoadOra.CountValidLoadOra()


    def HistInsert(self):
        print("inside HistInsert")
        if (self.InstanceChk == 'REF'):
            self.Query = '''insert into DEPT_INTL.T_CHEETAH_DAILY_REF_FEED_HIST
            select ISSUE_ID, case when ISSUENAME='nan' then NULL else ISSUENAME end
            ,to_date(TIMESENT,'YYYYMMDDHH24MISS') as DT_TIMESENT
            ,to_char(to_date(TIMESENT,'YYYYMMDDHH24MISS'),'HH24:MI:SS') as TM_TIMESENT
            ,MAILING_ID,SUBJECT,MAILING_NAME
            ,to_date(FILEDATE,'YYYYMMDD') as FILEDATE
            ,to_date(MOD_DT ,'YYYY_MM_DD') as MOD_DT
            from T_CHEETAH_DAILY_REF_FEED'''
        elif (self.InstanceChk == 'KPI'):
            self.Query = "insert into DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED_HIST \
            select EVENTTYPE, to_date(TIMESTAMP,'YYYYMMDDHH24MISS' )as DT_TIMESTAMP\
            ,to_char(to_date(TIMESTAMP,'YYYYMMDDHH24MISS'),'HH24:MI:SS') as TM_TIMESENT\
            ,ISSUEID,USERID,RESULTCODE,MIMETYPE,EMAIL,MAILING_NAME,to_date(FILEDATE,'YYYYMMDD') as FILEDATE\
            ,to_date(MOD_DT,'YYYY-MM-DD') MOD_DT from T_CHEETAH_DAILY_KPI_FEED  where EVENTTYPE <>'None'"
        else:
            print("No Instance found.. Fail the process")
        LogFile.write("HistInsert Running for " + str(self.InstanceChk) + "\n")
        Ob_ConnectOraExecute3 = ConnectOraExecute()
        Ob_ConnectOraExecute3.Query = self.Query
        Ob_ConnectOraExecute3.DataSet = []
        Ob_ConnectOraExecute3.ConnectOraExecute()
        # self.cur.execute(Query)
        # self.cur.execute("COMMIT")

class UnzipFiles():
    def UnzipFiles(self,FileName):
        #print("Filename : %s" %(FileName))
        print("Filename : {}".format(self.FileName))
        if(self.InstanceChk == 'REF'):
            self.df=pandas.read_csv(gzip.open(self.FileName,'r'), header=0, sep='|')
            self.translationTable = str.maketrans("éàèùâêîôûç", "eaeuaeiouc")
            self.df['FILEDATE']=self.DateOnFile
            self.df['MOD_DT'] = self.Current_Date
            #f = lambda x, y: x.translate(y)
            self.rows = [tuple(x) for x in self.df.values]

            self.lst1=[]
            for rr in self.rows:
                self.lst1.append(tuple(map(lambda x: str(
                        unicodedata.normalize("NFC", str(x).translate(self.translationTable)).encode('ASCII', 'ignore').decode(
                            "utf-8")), rr)))
            LogFile.write("UnzipFiles Running for " + str(self.InstanceChk) + "\n lst1 is craeted")
            print("List created lst1")
            return self.lst1
        elif(self.InstanceChk == 'KPI'):
            self.lll = "None,None,None,None,None,None,None,None"
            self.DataFile=gzip.open(self.FileName, 'rt')
            self.pdf = pandas.DataFrame()
            self.pdf = pandas.concat([self.pdf, pandas.DataFrame([tuple(self.lll.strip().split(','))])], ignore_index=True)
            self.pdf = pandas.concat([self.pdf, pandas.read_table(self.DataFile, header=0, sep='|', usecols=range(0, 8),
                                                    engine='python', dtype='object',index_col=False, names=list(range(8)))])
            self.pdf['FILEDATE'] = self.DateOnFile
            self.pdf['MOD_DT'] = self.Current_Date
            self.rows1 = [tuple(x) for x in self.pdf.values]
            LogFile.write("UnzipFiles Running for " + str(self.InstanceChk) + "\n rows1 is craeted")
            self.DataFile.close()
            LogFeed.write("Number of rows in file is : {}".format(len(self.rows1)))
            return self.rows1
        else:
            print("Faile the process")
class GetFileDetails():
    def GetFileDetails(self,OsDir):
        self.OsDir=OsDir
        self.FileList=[]
        for self.FileName in os.listdir(self.OsDir):
            if self.FileName.endswith('.gz'):
                print(os.path.join(self.OsDir, self.FileName))
                self.FileList.append(os.path.join(self.OsDir, self.FileName))
        LogFile.write("GetFileDetails Running to get the file list \n File List is \n"+str(self.FileList))
        # print(str(self.FileList))
        return self.FileList
class SendMail():
    def SendMail(self):
        print("Semding mail ...")
        sys.stdout.flush()
        LogFile.write("Sending mail\n")
        self.s = smtplib.SMTP(host='smtp-prod.starbucks.net')
        self.msg = MIMEText(self.msgString)
        self.msg['Subject'] = self.Subject
        self.msg['From'] = 'prod-wns-EMEA@starbucks.com'
        self.msg['To'] = Email_TO_DL

        self.s.sendmail('prod-wns-EMEA@starbucks.com',list(Email_TO_DL.split(',')), self.msg.as_string())
        self.s.quit()


def main():
    print("Starting process on {} at {}".format(Current_Date,Current_DTTM))

    Ob_GetFileDetails=GetFileDetails()
    FileList=Ob_GetFileDetails.GetFileDetails(Daily_Input_Files)
    Ob_SendMail1=SendMail()
    if not FileList:
        print("no File at Input Path {}. PLease check..".format(Daily_Input_Files))
        LogFile.write("\n no File at Input Path {}. PLease check..\n Sending Mail".format(Daily_Input_Files))
        LogFeed.write("Exiting from the script\nFailed ! Cheetah Mail Daily Insert Process")
        Ob_SendMail1.msgString="Hi,\nFile are not available at Source end. \nFailing the Insert Process\n Input Path is: "+str(PureWindowsPath(Daily_Input_Files))+"\n"
        Ob_SendMail1.Subject="Failed ! Cheetah Mail Daily Insert Process "+str(Current_Date)+"."
        Ob_SendMail1.SendMail()
        LogFeed.close()
        exit(1)

    else:
        print("We have file for the day")
        LogFile.write("Got File List\n Exiting from loop")
        # break
    for FileName in FileList:
        LogFile.write("Looping to process File {} \n".format(FileName))
        head, Tail = os.path.split(FileName)
        DateOnFile=re.search(r'\d+',Tail).group()
        print(str(DateOnFile))
        if(re.search('iid',FileName)):
            InstanceChk='REF'
        elif(re.search('data',FileName)):
            InstanceChk='KPI'
        else:
            print("Please check the files")
            # InstanceChk = 'REF'
        print("Call UnzipFiles object")
        LogFile.write("Call UnzipFiles object \n")
        Ob_UnzipFiles=UnzipFiles()
        Ob_UnzipFiles.FileName=FileName
        Ob_UnzipFiles.InstanceChk = InstanceChk
        Ob_UnzipFiles.DateOnFile=DateOnFile
        Ob_UnzipFiles.Current_Date=Current_Date
        CleanedDataList=Ob_UnzipFiles.UnzipFiles(Ob_UnzipFiles)
        LogFile.write("got List from UnzipFiles object \n")
        print("call OracleConnExecute")
        LogFile.write("Call OracleConnExecute object \n")
        Ob_PrepareQueryToExc=PrepareQueryToExc()
        Ob_PrepareQueryToExc.DataSet=CleanedDataList
        Ob_PrepareQueryToExc.InstanceChk = InstanceChk
        Ob_PrepareQueryToExc.TempTruncData()
        Ob_PrepareQueryToExc.DeltaLoadTable()
        Ob_PrepareQueryToExc.HistInsert()
        #TCYCLE_STATUS date insert

        Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYYMMDD'),'COMPLETE',Current_timestamp(0))".format(
            APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME+'_'+InstanceChk, DateOnFile)
        Ob_ConnectOraExecute3 = ConnectOraExecute()
        Ob_ConnectOraExecute3.Query = Query
        Ob_ConnectOraExecute3.DataSet = []
        Ob_ConnectOraExecute3.ConnectOraExecute()

        LogFile.write("OracleConnExecute completed for filr {}\n".format(FileName))
        LogFeed.write("Process completed for file {} \n Move file to the archive path".format(FileName))

        os.rename(FileName,Archive_Path+Tail)
    print("\n END of the process \n")

    LogFile.write("\n Sending final mail")
    Ob_SendMail1.msgString = "Cheetah Mail upload is completed for the day    "+str(Current_Date)+".\n data_ukfr_"+str(DateOnFile)+".dat.gz -> DEPT_INTL.T_CHEETAH_DAILY_KPI_FEED_HIST  "\
                            "\nand iid_keys_ukfr_"+str(DateOnFile)+".dat.gz -> DEPT_INTL.T_CHEETAH_DAILY_REF_FEED_HIST\n have been loaded into respective table."
    Ob_SendMail1.Subject = "Cheetah Mail upload is completed for the day "+str(Current_Date)+"."
    Ob_SendMail1.SendMail()
    LogFile.write("\n END of the process")


main()
