'''Version History:
Version :   1.0
Author  :   Unnati Khandelwal (EMEA WNS)
Date    :   Dec, 2018
'''

# import os
# import os.path
# import pandas as p
import sys
import datetime
import xlsxwriter
sys.path.append('../')

try:
    from Feeds_Scripts import CHATMyMicros_Conf as Conf
    # print(Conf.CHATMyMicros_Last_Logical_Date)
except ImportError as e:
    print("Error occurred while reading the config file:" + e)
    raise
# below parameters are getting read from configuration file
APPLICATION_NAME = Conf.APPLICATION_NAME
PROCESS_NAME = Conf.PROCESS_NAME
SUBPROCESS_NAME = 'MERGER_CHAT'
ApplicationPath = Conf.ApplicationPath
ProcessingPath = Conf.ProcessingPath
TargetPath = Conf.TargetPath
LogPath = Conf.LogPath
OraConnectionString = Conf.OraConnectionString
RunningFlag = Conf.RunningFlag
DataLagDays = Conf.DataLagDays
# RunningFlag = ApplicationPath+'Merger_Runningflag.txt'

# while(LogicalDate < datetime.datetime.strptime('2018-04-01', '%Y-%m-%d').date()):
with open(RunningFlag, 'r') as f:
    LogicalDate = f.readline()
    LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()

print("prepare Merger")

Current_Date = datetime.datetime.today().date()
Current_DTTM = datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile = LogPath+'MyMicros_CHAT_Log'+str(Current_DTTM)+'.txt'
HTMLParseFile = ProcessingPath + 'Details_HTMLParseFile_CHATMyMicros_' + str(LogicalDate) + '.txt'
# HTMLParseFile1 = ProcessingPath +'Details_HTMLParseFile_MyMicros_EUG_'+ str(LogicalDate) + '.txt'
XLWorkbook = TargetPath+'Daily_Extrct_MyMicros_CHAT_'+str(LogicalDate)+'.xlsx'
XLWorkSheet_CHAT = 'CHAT_Daily_Extrct_'+str(LogicalDate)
XLWorkSheet_TotalCount = 'Total_Count_CHAT_'+str(LogicalDate)


OverAllCountFile_CHAT = ProcessingPath+'OverallCount_MyMicros_CHAT_'+str(LogicalDate)+'.txt'


# a = p.read_table(HTMLParseFile,sep='||',skiprows=1)
# Create local history in Excel file by datewise

workbook = xlsxwriter.Workbook(XLWorkbook)
worksheet_CHAT = workbook.add_worksheet(XLWorkSheet_CHAT)
headerStr = "Card_Program,Gender,Language,Not_Activated_Cnt,Deleted_Cnt,Active_Cnt,Welcome_Status_Cnt,Welcome_Status_Stars,Green_Status_Cnt,Green_Status_Stars,Gold_Status_Cnt,Gold_Status_Stars,Email_Special_Offers_Cnt,Email_Newsletter_Cnt,SMS_Special_Offers_Cnt,mymicros_file_date, mod_date"
LogFeed = open(LogFile, 'w')
LogFeed.write('Starting merger.. \nwriting CHAT feed\n')
with open(HTMLParseFile, 'r') as f:
    row = 0
    col = 0
    for i in range(3):
        f.readline()
    # write the header
    # worksheet_CHAT.set_header(header=headerStr)
    for line in f:
        # f.readlines()[8]
        worksheet_CHAT.write(row, col,   line.split("||")[0].strip())
        worksheet_CHAT.write(row, col+1, line.split("||")[1].strip())
        worksheet_CHAT.write(row, col+2, line.split("||")[2].strip())
        worksheet_CHAT.write(row, col+3, float(line.split("||")[3].strip().replace(',','')))
        worksheet_CHAT.write(row, col+4, float(line.split("||")[4].strip().replace(',','')))
        worksheet_CHAT.write(row, col+5, float(line.split("||")[5].strip().replace(',','')))
        worksheet_CHAT.write(row, col+6, float(line.split("||")[6].strip().replace(',','')))
        worksheet_CHAT.write(row, col+7, float(line.split("||")[7].strip().replace(',','')))
        worksheet_CHAT.write(row, col+8, float(line.split("||")[8].strip().replace(',','')))
        worksheet_CHAT.write(row, col+9, float(line.split("||")[9].strip().replace(',','')))
        worksheet_CHAT.write(row, col+10, float(line.split("||")[10].strip().replace(',','')))
        worksheet_CHAT.write(row, col+11, float(line.split("||")[11].strip().replace(',','')))
        worksheet_CHAT.write(row, col+12, float(line.split("||")[12].strip().replace(',','')))
        worksheet_CHAT.write(row, col+13, float(line.split("||")[13].strip().replace(',','')))
        worksheet_CHAT.write(row, col+14, float(line.split("||")[14].strip().replace(',','')))

        
        # worksheet_CHAT.write(row, col+1, float(line.split("||")[1].strip().replace(',','').strip(")").replace("(","-")))
        # worksheet_CHAT.write(row, col+2, 'SKS')
        
        #Logical_date = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').strftime('%d-%b-%Y')
        #CurrentDate = datetime.datetime.strptime(Current_Date, '%Y-%m-%d').strftime('%d-%b-%Y')
        worksheet_CHAT.write(row, col+15, str(LogicalDate))
        worksheet_CHAT.write(row, col+16, str(Current_Date))
        row+=1

workbook.close()
print ('CHAT excel')


XLWorkbook1=TargetPath+'Daily_Extrct_MyMicros_CHAT_AVG_'+str(LogicalDate)+'.xlsx'
XLWorkSheet_CHAT1='CHAT_Daily_Extrct_AVG'+str(LogicalDate)
XLWorkSheet_TotalCount1='Total_Count_CHAT_AVG'+str(LogicalDate)


OverAllCountFile_CHAT1=ProcessingPath+'OverallCount_MyMicros_CHAT_AVG'+str(LogicalDate)+'.txt'


#a=p.read_table(HTMLParseFile,sep='||',skiprows=1)
#Create local history in Excel file by datewise

workbook1=xlsxwriter.Workbook(XLWorkbook1)
worksheet_CHAT1=workbook1.add_worksheet(XLWorkSheet_CHAT1)
headerStr1="Card_Program,Not_Activated_Cnt,Deleted_Cnt,Active_Cnt,Welcome_Status_Cnt,Welcome_Status_Stars,Green_Status_Cnt,Green_Status_Stars,Gold_Status_Cnt,Gold_Status_Stars,Email_Special_Offers_Cnt,Email_Newsletter_Cnt,SMS_Special_Offers_Cnt,mymicros_file_date, mod_date"
LogFeed=open(LogFile,'w')
LogFeed.write('Starting merger Average.. \nwriting CHAT feed\n')
#with open(HTMLParseFile,'r') as f:
    #write the header
# worksheet_CHAT1.set_header(header=headerStr1)
   # for line in f:
line = open(HTMLParseFile, "r").readlines()[2]

row=0
col=0

worksheet_CHAT1.write(row,col,datetime.datetime.strptime(line.split("||")[0].strip(), '%m/%d/%Y').strftime('%Y-%m-%d'))
# worksheet_CHAT1.write_datetime(row, col, line.split("||")[0].strip(),'%Y-%m-%d')

worksheet_CHAT1.write(row, col+1, float(line.split("||")[1].strip().replace(',','')))
worksheet_CHAT1.write(row, col+2, float(line.split("||")[2].strip().replace(',','')))
worksheet_CHAT1.write(row, col+3, float(line.split("||")[3].strip().replace(',','')))
worksheet_CHAT1.write(row, col+4, float(line.split("||")[4].strip().replace(',','')))
worksheet_CHAT1.write(row, col+5, float(line.split("||")[5].strip().replace(',','')))
worksheet_CHAT1.write(row, col+6, float(line.split("||")[6].strip().replace(',','')))
worksheet_CHAT1.write(row, col+7, float(line.split("||")[7].strip().replace(',','')))
worksheet_CHAT1.write(row, col+8, float(line.split("||")[8].strip().replace(',','')))
worksheet_CHAT1.write(row, col+9, float(line.split("||")[9].strip().replace(',','')))
worksheet_CHAT1.write(row, col+10,float(line.split("||")[10].strip().replace(',','')))
worksheet_CHAT1.write(row, col+11,float(line.split("||")[11].strip().replace(',','')))
worksheet_CHAT1.write(row, col+12,float(line.split("||")[12].strip().replace(',','')))

        
        
        # Logical_date = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').strftime('%d-%b-%Y')
        # CurrentDate = datetime.datetime.strptime(Current_Date, '%Y-%m-%d').strftime('%d-%b-%Y')
worksheet_CHAT1.write(row, col+13, str(LogicalDate))
worksheet_CHAT1.write(row, col+14, str(Current_Date))
    # row+=1
workbook1.close()
print ('CHAT AVG excel')
LogFeed.close()
