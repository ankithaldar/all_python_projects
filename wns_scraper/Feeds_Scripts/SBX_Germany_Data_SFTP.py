import datetime
import sys
import paramiko
#import os

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


APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='EXTRACTION'
ApplicationPath=Conf.ApplicationPath
Daily_Input_Files=Conf.Daily_Input_Files
LogPath=Conf.LogPath
RunningFlag=Conf.RunningFlag
OraConnectionString=Conf.OraConnectionString
PEMFile=Conf.PEMFile
PEMFilePassword=Conf.PEMFilePassword
SSHConnectionIP=Conf.SSHConnectionIP
SSHUserName=Conf.SSHUserName
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
DataDealyDays=Conf.DataDealyDays

Current_Date=datetime.datetime.today().date()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'SBX_Germany_Data_SFTP_'+str(Current_DTTM)+'.txt'
LogFeed=open(LogFile,'w')
LogFeed.write("Process started for {} at {}".format(Current_Date,Current_DTTM))

class DownloadFile():
    def DownloadFile(self,ld,OutputDir):
        try:
            LogFeed.write("\nSSH connection Negotiation started...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            paramiko.util.log_to_file('paramiko.log')
            P_key = paramiko.RSAKey.from_private_key_file(PEMFile,password=PEMFilePassword)
            ssh.connect(SSHConnectionIP, username=SSHUserName, pkey=P_key)
            print("SSH connection done")
            sftp = ssh.open_sftp()
            LogFeed.write("\nSSH connection Negotiation compeleted...")
            print("SFTP connection done")
            sftp.chdir('/sbx_wns/')
            sftp.get('SBX_DE_SERVICECHARGES_'+str(ld)+'.csv',OutputDir+'SBX_DE_SERVICECHARGES_'+str(ld)+'.csv')
            print("SBX_DE_SERVICECHARGES_ done")
            sftp.get('SBX_DE_TRANSACTIONS_' + str(ld) + '.csv', OutputDir + 'SBX_DE_TRANSACTIONS_' + str(ld) + '.csv')
            print("SBX_DE_TRANSACTIONS_ done")
            print("SFTP get done")
            LogFeed.write("\nSSH SFTP get done...")
            sftp.rename('SBX_DE_SERVICECHARGES_'+str(ld)+'.csv','SBX_DE_SERVICECHARGES_'+str(ld)+'.csv'+'.done')
            sftp.rename('SBX_DE_TRANSACTIONS_' + str(ld) + '.csv', 'SBX_DE_TRANSACTIONS_' + str(ld) + '.csv' + '.done')
            sftp.close()
            ssh.close()
            LogFeed.write("\nSSH connection closed...")
            return 0
        except Exception as e:
            print(e)
            msgString = "\n Error:" + str(e)+" for "+str(ld) +"\nNo file present at source end.\nPlease check with source"
            LogFeed.write("\nSSH exception occured... \n error : "+msgString)
            Subject = "SBX Germany Data process SFTP by-passed."
            Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
            return 1001
            # exit(1)




def main():
    print('read logical date from Running flag')
    with open(RunningFlag, 'r') as f:
        LogicalDate = f.readline()
    if (LogicalDate == ''):
        print("exit from the process \n no date read")
        LogFeed.write("\nexit from the process \n no date read \n Check Running flag file")
        exit(1)
    else:
        LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
        print("logical date is : " + str(LogicalDate))
    while (LogicalDate < Current_Date - datetime.timedelta(days=DataDealyDays)):
        LogicalDate = LogicalDate + datetime.timedelta(days=1)
        print("running for logicaldate: {}".format(LogicalDate))
        ob_DownloadFile=DownloadFile()
        Retcode1=ob_DownloadFile.DownloadFile(LogicalDate,Daily_Input_Files)


        if (Retcode1 == 0 ):
            LogFeed.write("\n files have been successfully extracted")
            LogFeed.write("\nwrite running falg file")

            with open(RunningFlag, 'w') as f:
                f.write(str(LogicalDate.strftime('%Y-%m-%d')))
        else:
            LogFeed.write("{} ".format(Retcode1))
            # if (os.path.exists(RunningFlag)):
            #     print("removing running flag")
            #     LogFeed.write("\nRemove the flag")
            #     os.remove(RunningFlag)
            break



main()