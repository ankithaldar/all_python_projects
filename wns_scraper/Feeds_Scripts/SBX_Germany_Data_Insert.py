import datetime
import sys
import cx_Oracle
import time
import math
import pandas
import re
import os

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

APPLICATION_NAME = Conf.APPLICATION_NAME
PROCESS_NAME = Conf.PROCESS_NAME
SUBPROCESS_NAME = 'INSERT'
ApplicationPath = Conf.ApplicationPath
Daily_Input_Files = Conf.Daily_Input_Files
LogPath = Conf.LogPath
RunningFlag = Conf.RunningFlag
OraConnectionString = Conf.OraConnectionString
Archive_Path=Conf.Archive_Path
PEMFile = Conf.PEMFile
PEMFilePassword = Conf.PEMFilePassword
SSHConnectionIP = Conf.SSHConnectionIP
SplitValue=Conf.SplitValue
SSHUserName = Conf.SSHUserName
Email_TO_DL = Conf.Email_TO_DL
Alert_TO_DL = Conf.Alert_TO_DL

# LogicalDate=datetime.datetime.strptime(Conf.Cheetah_Last_Logical_Date,'%Y%m%d').date()
Current_Date=datetime.date.today().isoformat()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
CurrDate=datetime.datetime.today().date()
LogFile=LogPath+'SBX_Germany_Data_Insert'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
LogFeed.write("Process started for {} at {}".format(Current_Date,Current_DTTM))
LogFile=open(LogFile,'w')


class ConnectOraExecute():
    def ConnectOraExecute(self):
        self.RetryCon=0
        print(type(self.DataSet))
        print(self.DataSet[:3])
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
                msgString = "SBX Germany Daliy Load Insert failed,  File  Count does not match.\nLoaded Count is {} and File Count is {} ".format(self.LoadedCount[0][0],self.FileRowCount)
                Subject = "Failed ! SBX Germany Daliy Load Insert  Validation process" + str(Current_Date) + "."
                Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
                exit(1)
            else:
                print("\nCount matched , Table count is : {} and FileCount is : {} .\n".format(self.LoadedCount[0][0],self.FileRowCount))
                LogFile.write("\nCount matched , Table count is : {} and FileCount is : {} .\n".format(self.LoadedCount[0][0],self.FileRowCount))

        except cx_Oracle.DatabaseError as e:

            LogFile.write("\n Count does not match")
            msgString = "SBX Germany Daliy Load Insert failed,  File  Count does not match    "
            Subject = "Failed ! SBX Germany Daliy Load Insert  Validation process" + str(Current_Date) + "."
            Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
            raise


class PrepareQueryToExc():

    def TempTruncData(self):
        print("Inside TempTruncData")
        if (self.InstanceChk == 'SERV'):
            self.Query = 'truncate table DEPT_INTL.T_DE_SERVICECHARGES'
        elif (self.InstanceChk == 'TRAX'):
            self.Query = 'truncate table DEPT_INTL.T_DE_TRANSACTIONS'
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
        if (self.InstanceChk == 'SERV'):
            self.Query = '''insert into DEPT_INTL.T_DE_SERVICECHARGES(GUESTCHECKID,COSTCENTER,ACTION_TYPE,TRANSDATETIME
            ,SERVICECHARGETOTAL,REPORTLINETOTAL,FILEDATE,MOD_DT)
            values (:1,:2,:3,:4,:5,:6,:7,:8)'''
            self.ExtractCount='''select count(*) from DEPT_INTL.T_DE_SERVICECHARGES'''
        elif (self.InstanceChk == 'TRAX'):
            self.Query = '''insert into DEPT_INTL.T_DE_TRANSACTIONS (GUESTCHECKID,GUESTCHECKLINEITEMID,IDSALESCHANNEL,COSTCENTER,IDITEM,IDCOMBO,ITEMTYPE
            ,TRANS_DATE,TRANS_HOUR,IDDISCOUNT,SALESNET,TAX,DISCOUNT,COS,COMBOMEALNUM,LINECOUNT,FILEDATE,MOD_DT) 
            values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18)'''
            self.ExtractCount = '''select count(*) from DEPT_INTL.T_DE_TRANSACTIONS'''
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
        if (self.InstanceChk == 'SERV'):
            self.Query = '''insert into DEPT_INTL.T_DE_SERVICECHARGES_HIST select 
                            to_number(GUESTCHECKID ) as GUESTCHECKID,
                            to_number(COSTCENTER) as COSTCENTER,
                            ACTION_TYPE ,
                            TRANSDATETIME ,
                            to_number(SERVICECHARGETOTAL) as SERVICECHARGETOTAL,
                            to_number(REPORTLINETOTAL) as REPORTLINETOTAL,
                            to_date(FILEDATE,'YYYY-MM-DD') as FILEDATE,
                            to_date(MOD_DT,'YYYY-MM-DD') as MOD_DT
                            from  DEPT_INTL.T_DE_SERVICECHARGES'''
        elif (self.InstanceChk == 'TRAX'):
            self.Query = '''insert into DEPT_INTL.T_DE_TRANSACTIONS_HIST select
                             to_number(GUESTCHECKID) as GUESTCHECKID
                            ,to_number(GUESTCHECKLINEITEMID) as GUESTCHECKLINEITEMID
                            ,to_number(IDSALESCHANNEL) as IDSALESCHANNEL
                            ,to_number(COSTCENTER) as COSTCENTER
                            ,to_number(IDITEM) as IDITEM
                            ,to_number(IDCOMBO) as IDCOMBO
                            ,to_number(ITEMTYPE) as ITEMTYPE
                            ,to_date(TRANS_DATE,'YYYYMMDD') as TRANS_DATE
                            ,TRANS_HOUR 
                            ,to_number(IDDISCOUNT) as IDDISCOUNT
                            ,to_number(SALESNET) as SALESNET
                            ,to_number(TAX) as TAX
                            ,to_number(DISCOUNT) as DISCOUNT
                            ,to_number(COS) as COS
                            ,to_number(COMBOMEALNUM) as COMBOMEALNUM
                            ,to_number(LINECOUNT) as LINECOUNT
                            ,to_date(FILEDATE,'YYYY-MM-DD') as FILEDATE
                            ,to_date(MOD_DT,'YYYY-MM-DD') as MOD_DT
                            from DEPT_INTL.T_DE_TRANSACTIONS'''
        else:
            print("No Instance found.. Fail the process")
        LogFile.write("HistInsert Running for " + str(self.InstanceChk) + "\n")
        Ob_ConnectOraExecute3 = ConnectOraExecute()
        Ob_ConnectOraExecute3.Query = self.Query
        Ob_ConnectOraExecute3.DataSet = []
        Ob_ConnectOraExecute3.ConnectOraExecute()
        # self.cur.execute(Query)
        # self.cur.execute("COMMIT")

class CreateDF():
    def CreateDF(self):
        #print("Filename : %s" %(FileName))
        print("Filename : {}".format(self.FileName))

        self.df=pandas.read_csv(self.FileName, header=0)
        self.df['FILEDATE']=self.DateOnFile
        self.df['MOD_DT'] = self.Current_Date

        self.df.rename(columns={'DATE': 'TRANS_DATE','HOUR':'TRANS_HOUR'},inplace=True)
        # print(self.df.describe())
        # print(self.df.head(4))

        # self.rows = [tuple(x) for x in self.df.values]
        self.rows = [tuple(map(lambda x :str(x),x)) for x in self.df.values]
        return self.rows

class GetFileDetails():
    def GetFileDetails(self,OsDir):
        self.OsDir=OsDir
        self.FileList=[]
        for self.FileName in os.listdir(self.OsDir):
            # print(self.FileName)
            if self.FileName.endswith('.csv') and os.path.getsize(os.path.join(self.OsDir, self.FileName)) > 0:
                print(os.path.join(self.OsDir, self.FileName))
                self.FileList.append(os.path.join(self.OsDir, self.FileName))
        LogFile.write("GetFileDetails Running to get the file list \n File List is \n"+str(self.FileList))
        # print(str(self.FileList))
        return self.FileList

def main():
    print("Starting process on {} at {}".format(Current_Date,Current_DTTM))
    LogFile.write("Starting process on {} at {}\n".format(Current_Date,Current_DTTM))

    Ob_GetFileDetails=GetFileDetails()
    FileList=Ob_GetFileDetails.GetFileDetails(Daily_Input_Files)

    if not FileList:
        print("no File at Input Path {}. PLease check..".format(Daily_Input_Files))
        LogFile.write("\n no File at Input Path {}. PLease check..\n Sending Mail".format(Daily_Input_Files))
        LogFeed.write("Exiting from the script\nFailed ! Cheetah Mail Daily Insert Process")
        LogFeed.close()
        exit(1)

    else:
        print("We have file for the day")
        LogFile.write("Got File List\n Exiting from loop")
        # break
    for FileName in FileList:
        LogFile.write("Looping to process File {} \n".format(FileName))
        head, Tail = os.path.split(FileName)
        print("tail : {}".format(Tail))
        DateOnFile=re.search(r'\d+-\d+-\d+',Tail).group()
        print("DateOnFile : {}".format(DateOnFile))
        print(str(DateOnFile))
        if(re.search('SBX_DE_SERVICECHARGES',FileName)):
            InstanceChk='SERV'
        elif(re.search('SBX_DE_TRANSACTIONS',FileName)):
            InstanceChk='TRAX'
        else:
            print("Please check the files")
            # InstanceChk = 'REF'
        print("Call CreateDF object")
        LogFile.write("Call CreateDF object \n")
        Ob_CreateDF=CreateDF()
        Ob_CreateDF.FileName=FileName

        Ob_CreateDF.DateOnFile=DateOnFile
        Ob_CreateDF.Current_Date=Current_Date
        CleanedDataList=Ob_CreateDF.CreateDF()
        LogFile.write("got List from CreateDF object \n")
        print("call OracleConnExecute")
        LogFile.write("Call OracleConnExecute object \n")
        Ob_PrepareQueryToExc=PrepareQueryToExc()
        Ob_PrepareQueryToExc.DataSet=CleanedDataList
        Ob_PrepareQueryToExc.InstanceChk = InstanceChk
        Ob_PrepareQueryToExc.TempTruncData()
        Ob_PrepareQueryToExc.DeltaLoadTable()
        Ob_PrepareQueryToExc.HistInsert()
        #TCYCLE_STATUS date insert

        Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
            APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME+'_'+InstanceChk, DateOnFile)
        Ob_ConnectOraExecute3 = ConnectOraExecute()
        Ob_ConnectOraExecute3.Query = Query
        Ob_ConnectOraExecute3.DataSet = []
        Ob_ConnectOraExecute3.ConnectOraExecute()

        LogFile.write("OracleConnExecute completed for filr {}\n".format(FileName))
        LogFeed.write("Process completed for file {} \n Move file to the archive path".format(FileName))

        os.rename(FileName,Archive_Path+Tail)
    print("\n END of the process \n")

    LogFile.write("\n END of the process")


main()


'''

create table DEPT_INTL.T_DE_SERVICECHARGES(
GUESTCHECKID integer,
COSTCENTER integer,
ACTION_TYPE varchar(10),
TRANSDATETIME varchar(20),
SERVICECHARGETOTAL float,
REPORTLINETOTAL float,
FILE_DT varchar(10),
MOD_DT varchar(10)
);

create table DEPT_INTL.T_DE_TRANSACTIONS(
 GUESTCHECKID integer
,GUESTCHECKLINEITEMID integer
,IDSALESCHANNEL integer
,COSTCENTER integer
,IDITEM integer
,IDCOMBO integer
,ITEMTYPE integer
,TRANS_DATE varchar(10)
,TRANS_HOUR varchar(20)
,IDDISCOUNT float
,SALESNET float
,TAX float
,DISCOUNT float
,COS float
,COMBOMEALNUM  number(3)
,LINECOUNT number(3)
,FILE_DT varchar(10)
,MOD_DT varchar(10)
)
'''