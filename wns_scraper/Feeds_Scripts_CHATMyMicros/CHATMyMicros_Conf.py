
"""
This is config file used by Oracle MyMicros Daily process.
No parameter should be without values
These values are used by below python scripts
1.

Version History:
Version :   1.0
Author  :   Unnati Khandelwal (EMEA WNS)
Date    :   Dec, 2018

"""

import pathlib

APPLICATION_NAME = 'CHATMYMICROS'
PROCESS_NAME = 'DAILY'

try:
    current_dir = pathlib.Path(__file__).parent
    current_file = pathlib.Path(__file__)
    print(current_dir)
    print(current_file)
    from Feeds_Scripts import CHATPasswordFile as Pwx
except ImportError:
    raise

#ApplicationPath='//ms12495/WNS Reporting/Unnati/Python/WebScraping/CHATMyMicros/'
ApplicationPath='C:/WNS/CHATMyMicros/'
ProcessingPath=ApplicationPath+'Processing/'
TargetPath=ApplicationPath+'Target_path/'
LogPath=ApplicationPath+'Logs/'
RunningFlag=ApplicationPath+'CHATMyMicros_ExtractionSchedule.txt'

#Replace the User credentials "slal[DEPT_INTL]/Sbux2018@xd04-scan:15031/SBP416" in below line
current_dir = pathlib.Path(__file__).parent
current_file = pathlib.Path(__file__)
print(current_dir)
print(current_file)
OraConnectionString=Pwx.OraConnectionString
#Load balance while loading into Oracle DB can be increased or lowered
SplitValue=40000

#Chat MyMicros Login credentilas, Update password below

urllogon = 'https://sbuxsimmm.ch/login.jsp'
username = 'slal'
password = Pwx.password
company = 'SSA'


DataLagDays=3
ImplWaitWebDrv=200
# Add or remove e-mail ids in below parameter
Email_TO_DL='"SBUXEMEA@retail.starbucks.com"'
Alert_TO_DL='"ahaldar@starbucks.com"'
# Email_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
#                 "gc@starbucks.com","shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
#                 "tshahir@starbucks.com","gc@starbucks.com"'''
# Alert_TO_DL='''"rasingh@starbucks.com","sdey@starbucks.com","sgoyal@starbucks.com","harsingh@starbucks.com",
#                 "gc@starbucks.com","shbiswas@starbucks.com","slal@starbucks.com","megoyal@starbucks.com",
#                 "tshahir@starbucks.com","gc@starbucks.com"'''
