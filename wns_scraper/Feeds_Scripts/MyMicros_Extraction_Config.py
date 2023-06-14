'''This is config file used by Oracle MyMicros Daily process.
No parameter should be without values
These values are used by below python scripts
1.

Version History:
Version :   1.0
Author  :   Ranveer Singh (EMEA WNS)
Date    :   April, 2018

'''
import sys
sys.path.append('../')
sys.path.append('C:/WNS/Projects/')
APPLICATION_NAME='MYMICROS'
PROCESS_NAME='DAILY'

try:
    from Feeds_Scripts import PasswordFile as Pwx
except ImportError:
    raise


# ApplicationPath='C:/WNS/Projects/MyMicrosExtraction/'
ApplicationPath='//ms12495/WNS Reporting/Ranveer/MyMicrosExtraction/'
ProcessingPath=ApplicationPath+'Processing/'
TargetPath=ApplicationPath+'Target_path/'
LogPath=ApplicationPath+'Logs/'
RunningFlag=ApplicationPath+'MyMicros_ExtractionSchedule.txt'

#Replace the User credentials "tshahir[DEPT_INTL]/Glory2018" in below line
OraConnectionString=Pwx.OraConnectionString
#Load balance while loading into Oracle DB can be increased or lowered
SplitValue=40000

#Cheetah Digital Login credentilas, Update password below
#For SKS
urllogon = 'https://www.mymicroseu2.net/servlet/PortalLogIn/'
username_SKS = 'WNS'
password_SKS = Pwx.password_SKS
company_SKS = 'SKS'

#For EUG
username_EUG = 'WNS'
password_EUG = Pwx.password_EUG
company_EUG = 'EUG'

DataLagDays=3
ImplWaitWebDrv=200
#Add or remove e-mail ids in below parameter
Email_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
                "gc@starbucks.com","shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
                "tshahir@starbucks.com","gc@starbucks.com"'''
Alert_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
                "gc@starbucks.com","shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
                "tshahir@starbucks.com","gc@starbucks.com"'''