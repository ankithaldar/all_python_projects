'''This is config file used by Cheeha Daily process.
No parameter should be withour value
Tese values are used by below python scripts
1.  Cheetah_Daily_Extraction
2.  Cheetah_Daily_Insert
3.  Cheetah_DailyLoadSchedule

Version History:
Version :   1.0
Author  :   Ranveer Sing (EMEA WNS)
Date    :   April, 2018

'''

try:
    from Feeds_Scripts import PasswordFile as Pwx
except ImportError:
    raise

APPLICATION_NAME='SBX_GERMANY'
PROCESS_NAME='DAILY'

# ApplicationPath='C:/WNS/Projects/CheetahMail/'
ApplicationPath='//ms12495/WNS Reporting/Ranveer/SBX_Germany_Data_SFTP/'
Daily_Input_Files=ApplicationPath+'Daily_Input_Files/'
Archive_Path=ApplicationPath+'Archive_Daily_Input_Files/'
LogPath=ApplicationPath+'Logs/'

RunningFlag=ApplicationPath+'SBX_Germany_LoadSchedule_RunningFlag.txt'


OraConnectionString=Pwx.OraConnectionString
#Load balance while loading into Oracle DB can be increased or lowered
SplitValue=40000

DataDealyDays=2
SSHConnectionIP='195.190.24.39'
SSHUserName='sbx_wns'
PEMFile='C:/WNS/Projects/Impo_keys/pri_key-rasingh-20180430.pem'
PEMFilePassword='1234qwer'


#Add or remove e-mail ids in below parameter
Email_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
                "shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
                "tshahir@starbucks.com","gc@starbucks.com"'''
Alert_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
                "shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
                "tshahir@starbucks.com","gc@starbucks.com"'''