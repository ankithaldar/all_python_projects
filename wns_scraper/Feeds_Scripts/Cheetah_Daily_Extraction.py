'''This program Extracts .zip file from Cheetah SFTP Webserver

Version History:
Version :   1.0
Author  :   Ranveer Sing (EMEA WNS)
Date    :   April, 2018

Important:
While deploying in other desktop/server Please change below parameter
"C:/WNS/Projects/"
This is reading configuration file
'''
import requests
import datetime
import os
import ssl
import urllib3
import sys
import smtplib
from email.mime.text import MIMEText
import cx_Oracle
urllib3.disable_warnings()
sys.path.append('../')
try:
    from Feeds_Scripts import Cheetah_Extraction_config as Conf
    # print(Conf.Cheetah_Last_Logical_Date)
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise
#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='EXTRACTION'
ApplicationPath=Conf.ApplicationPath
Daily_Input_Files=Conf.Daily_Input_Files
LogPath=Conf.LogPath
RunningFlag=Conf.RunningFlag
OraConnectionString=Conf.OraConnectionString
url=Conf.LogInurl
user=Conf.CheetahLogIn_UserName
password=Conf.CheetahLogIn_Password


Current_Date=datetime.date.today().isoformat()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
CurrDate=datetime.datetime.today().date()
LogFile=LogPath+'Cheetah_Daily_Extraction_'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
LogFeed.write("\nProcess started for {} at {}".format(Current_Date,Current_DTTM))

print('read logical date from Running flag')
with open (RunningFlag,'r') as f:
    LogicalDate=f.readline()
if(LogicalDate==''):
    print("exit from the process \n no date read")
    LogFeed.write("\nexit from the process \n no date read \n Check Running flag file")
    exit(1)
else:
    LogicalDate=datetime.datetime.strptime(LogicalDate,'%Y-%m-%d').date()
    print("logical date is : "+str(LogicalDate))
class VelodateCycyleDate():
    def VelodateCycyleDate(self,query,Type):
        self.query=query
        self.Type=Type
        try:
            self.con = cx_Oracle.connect(OraConnectionString)
            LogFeed.write('\nDatabase connection established successfully.. \n')
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
            raise
        finally:
            self.con.close()

class ExtractFile():
    def ExtractFile(self,FileName,ShortName):
        self.FileName=FileName
        self.ShortName=ShortName
        LogFeed.write("extracting file {}".format(self.ShortName))
        # url = 'https://tt.cheetahmail.com/ttview/'
        data = {"user":user, "password":password,"path":"",'login_ind': '1'}
        headersInfo={"user_agent":"Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)","Accept-Encoding": "gzip, deflate, br"}

        context = ssl.create_default_context()
        with requests.session() as s:
            Page1 = s.post(url, data=data, headers=headersInfo, verify=False)
            cookies = dict(Page1.cookies)
            h=dict(Page1.headers)
            headersInfo.update(h)
            # print(headersInfo.values())
            try:
                Page2=s.get(self.FileName,stream=True,verify=False)
                print(Page2.status_code)
                if(int(Page2.status_code) != 200):
                    LogFeed.write("\nFail the process as per response\n Error code is:{}".format(Page2.status_code))
                    if (os.path.exists(RunningFlag)):
                        print("removing running flag")
                        LogFeed.write("\nRemove the flag")
                        os.remove(RunningFlag)
                    exit(1)

                LogFeed.write("\nextracted succcessfully")
            except requests.exceptions.RequestException as e:
                LogFeed.write("\nFailed with error while extracting the resource: {}".format(e))
                raise
            # print(Page2.content)
            with open(Daily_Input_Files + self.ShortName+'.txt', 'wb') as f:
                f.write(Page2.content)
            if(os.path.exists( Daily_Input_Files +self.ShortName+'.gz')):
                LogFeed.write("\nremoving file")
                LogFeed.write(Daily_Input_Files +self.ShortName+'.gz')
                os.remove( Daily_Input_Files +self.ShortName+'.gz')
            os.rename(Daily_Input_Files + self.ShortName+'.txt',Daily_Input_Files + self.ShortName+'.gz')
            return 0
def main():
    LogFeed.write("\nStarting the process.. on {}".format(CurrDate))
    LogFeed.write("\nExtraction is completed till {}".format(LogicalDate))
    print("Extraction is completed till {}".format(LogicalDate))
    if (LogicalDate < CurrDate - datetime.timedelta(days=1)):
        LogFeed.write("\nstart process for logical_date :{}".format(LogicalDate+datetime.timedelta(days=1)))
        print("start process for logical_date :{}".format(LogicalDate+datetime.timedelta(days=1)))
    elif(LogicalDate >= CurrDate):
        LogFeed.write("\ninvalid dates !!! is given {}".format(LogicalDate))
        if (os.path.exists(RunningFlag)):
            print("removing running flag")
            LogFeed.write("\nRemove the flag")
            os.remove(RunningFlag)
        LogFeed.close()
        exit(1)
    else:
        LogFeed.write("\nFiles already extracted for logical Date {}".format(LogicalDate))
        if (os.path.exists(RunningFlag)):
            print("removing running flag")
            LogFeed.write("\nRemove the flag")
            os.remove(RunningFlag)
        LogFeed.close()
        exit(1)
    NextLogicalDate = LogicalDate
    while(NextLogicalDate < CurrDate - datetime.timedelta(days=1)):
        NextLogicalDate = NextLogicalDate + datetime.timedelta(days=1)
        LogFeed.write("\nstart loop : for logical date {}".format(NextLogicalDate))
        iid_keys_ukfr="https://tt.cheetahmail.com/ttview//iid_keys_ukfr_"+NextLogicalDate.strftime('%Y%m%d')+".dat.gz?path=fromcheetah&file=iid_keys_ukfr_"+NextLogicalDate.strftime('%Y%m%d')+".dat.gz&action=f"
        data_ukfr="https://tt.cheetahmail.com/ttview//data_ukfr_"+ NextLogicalDate.strftime('%Y%m%d') +".dat.gz?path=fromcheetah&file=data_ukfr_"+ NextLogicalDate.strftime('%Y%m%d') +".dat.gz&action=f"
        Ob_ExtractFile=ExtractFile()
        Retcode1=Ob_ExtractFile.ExtractFile(iid_keys_ukfr,"iid_keys_ukfr_"+NextLogicalDate.strftime('%Y%m%d'))
        Retcode2=Ob_ExtractFile.ExtractFile(data_ukfr,"data_ukfr_"+NextLogicalDate.strftime('%Y%m%d'))
        if(Retcode1 == 0 and Retcode2==0):
            LogFeed.write("\n files have been successfully extracted")
        else:
            LogFeed.write("{} and {}".format(Retcode1,Retcode2))
            if (os.path.exists(RunningFlag)):
                print("removing running flag")
                LogFeed.write("\nRemove the flag")
                os.remove(RunningFlag)
            exit(1)

        LogFeed.write("\nwrite config file")

        with open(RunningFlag ,'w') as f:
            f.write(str(NextLogicalDate.strftime('%Y-%m-%d')))
        Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
            APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME,NextLogicalDate)
        Ob_VelodateCycyleDate = VelodateCycyleDate()
        Ob_VelodateCycyleDate.VelodateCycyleDate(Query, 'INSERT')

        if(NextLogicalDate == CurrDate - datetime.timedelta(days=1)):
            LogFeed.write("\nAll files have been extracted till : {} \n Exiting out !".format(NextLogicalDate))
            LogFeed.write("\nsend mail...")
            # exit(0)
main()