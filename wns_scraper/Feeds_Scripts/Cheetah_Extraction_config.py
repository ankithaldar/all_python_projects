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

APPLICATION_NAME='CHEETAH_DIGITAL'
PROCESS_NAME='DAILY'

# ApplicationPath='C:/WNS/Projects/CheetahMail/'
ApplicationPath='//ms12495/WNS Reporting/Ranveer/CheetahMail/'
Daily_Input_Files=ApplicationPath+'Daily_Input_Files/'
Archive_Path=ApplicationPath+'Archive_Daily_Input_Files/'
LogPath=ApplicationPath+'Logs/'

RunningFlag=ApplicationPath+'Cheetah_DailyLoadSchedule_RunningFlag.txt'


OraConnectionString=Pwx.OraConnectionString
#Load balance while loading into Oracle DB can be increased or lowered
SplitValue=40000

#Cheetah Digital Login credentilas, Update password below
LogInurl = 'https://tt.cheetahmail.com/ttview/'
CheetahLogIn_UserName=Pwx.CheetahLogIn_UserName
CheetahLogIn_Password=Pwx.CheetahLogIn_Password

#Add or remove e-mail ids in below parameter
Email_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
                "gc@starbucks.com","shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
                "tshahir@starbucks.com","gc@starbucks.com"'''