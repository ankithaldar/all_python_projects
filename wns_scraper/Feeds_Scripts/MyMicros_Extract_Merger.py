import os
import os.path
import pandas as p
import datetime
import xlsxwriter


try:
    from Feeds_Scripts import MyMicros_Extraction_Config as Conf
    # print(Conf.Cheetah_Last_Logical_Date)
    from Feeds_Scripts import CommonFunction
    Ob_CommonFunction = CommonFunction.SendMail()
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise
#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='MERGER_SKS_EUG'
ApplicationPath=Conf.ApplicationPath
ProcessingPath=Conf.ProcessingPath
TargetPath=Conf.TargetPath
LogPath=Conf.LogPath
OraConnectionString=Conf.OraConnectionString
RunningFlag=Conf.RunningFlag
DataLagDays=Conf.DataLagDays
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
with open(RunningFlag, 'r') as f:
    LogicalDate = f.readline()
    LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
print("prepare Merger")

Current_Date=datetime.datetime.today().date()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'MyMicros_Merger_Log'+str(Current_DTTM)+'.txt'
HTMLParseFile=ProcessingPath+'Details_HTMLParseFile_MyMicros_SKS_'+str(LogicalDate)+'.txt'
HTMLParseFile1=ProcessingPath+'Details_HTMLParseFile_MyMicros_EUG_'+str(LogicalDate)+'.txt'
XLWorkbook=TargetPath+'Daily_Extrct_MyMicros_Merge_'+str(LogicalDate)+'.xlsx'
XLWorkSheet_SKS='SKS_Daily_Extrct_'+str(LogicalDate)
XLWorkSheet_EUG='EUG_Daily_Extrct_'+str(LogicalDate)
XLWorkSheet_TotalCount='Total_Count_SKS_EUG_'+str(LogicalDate)


OverAllCountFile_SKS=ProcessingPath+'OverallCount_MyMicros_SKS_'+str(LogicalDate)+'.txt'
OverAllCountFile_EUG=ProcessingPath+'OverallCount_MyMicros_EUG_'+str(LogicalDate)+'.txt'


#a=p.read_table(HTMLParseFile,sep='||',skiprows=1)
#Create local history in Excel file by datewise
try:

    workbook=xlsxwriter.Workbook(XLWorkbook)
    worksheet_SKS=workbook.add_worksheet(XLWorkSheet_SKS)
    headerStr="Location,RevenueCenter,Check1,TransactionTime,Check2,Transaction,DiscountTotal,CheckTotal,Company,Extraction_date"
    LogFeed=open(LogFile,'w')
    LogFeed.write('Starting merger.. \nwriting SKS feed\n')
    with open(HTMLParseFile,'r') as f:
        grb=f.readline()
        row=0
        col=0
        #write the header
        worksheet_SKS.set_header(header=headerStr)
        for line in f:
            worksheet_SKS.write(row, col,   line.split("||")[0].strip())
            worksheet_SKS.write(row, col+1, line.split("||")[1].strip())
            worksheet_SKS.write(row, col+2, int(line.split("||")[2].strip()))
            worksheet_SKS.write(row, col+3, line.split("||")[3].strip())
            worksheet_SKS.write(row, col+4, line.split("||")[4].strip())
            worksheet_SKS.write(row, col+5, line.split("||")[5].strip())
            worksheet_SKS.write(row, col+6, float(line.split("||")[6].strip().strip(")").replace('(','-')))
            worksheet_SKS.write(row, col+7, float(line.split("||")[7].replace('\n','').strip().strip(")").replace('(','-')))
            worksheet_SKS.write(row, col+8, 'SKS')
            worksheet_SKS.write(row, col+9, str(LogicalDate))
            row+=1
    worksheet_EUG = workbook.add_worksheet(XLWorkSheet_EUG)
    LogFeed.write('Starting merger.. \nwriting EUG feed\n')
    with open(HTMLParseFile1,'r') as f:
        grb=f.readline()
        row=0
        col=0
        #write the header
        worksheet_EUG.set_header(header=headerStr)
        for line in f:
            worksheet_EUG.write(row, col,   line.split("||")[0].strip())
            worksheet_EUG.write(row, col+1, line.split("||")[1].strip())
            worksheet_EUG.write(row, col+2, int(line.split("||")[2].strip()))
            worksheet_EUG.write(row, col+3, line.split("||")[3].strip())
            worksheet_EUG.write(row, col+4, line.split("||")[4].strip())
            worksheet_EUG.write(row, col+5, line.split("||")[5].strip())
            worksheet_EUG.write(row, col+6, float(line.split("||")[6].strip().strip(")").replace('(','-')))
            worksheet_EUG.write(row, col+7, float(line.split("||")[7].replace('\n','').strip().strip(")").replace("(",'-')))
            worksheet_EUG.write(row, col+8, 'EUG')
            worksheet_EUG.write(row, col+9, str(LogicalDate))
            row+=1

    #adding Daily Counts in the file
    LogFeed.write('Starting merger.. \nwriting total count feed\n')
    worksheet_Total_Count = workbook.add_worksheet(XLWorkSheet_TotalCount)
    with open(OverAllCountFile_SKS,'r') as f:
        row = 0
        col = 0
        # write the header

        for line in f:
            worksheet_Total_Count.write(row, col, line.split("||")[0].strip())
            worksheet_Total_Count.write(row, col + 1, float(line.split("||")[1].strip()))
            worksheet_Total_Count.write(row, col + 2, int(line.split("||")[2].strip()))
            worksheet_Total_Count.write(row, col + 3, line.split("||")[3].strip())
            worksheet_Total_Count.write(row, col + 4, line.split("||")[4].strip())
            row += 1
    with open(OverAllCountFile_EUG,'r') as f:
        row = 1
        col = 0
        # write the header

        for line in f:
            worksheet_Total_Count.write(row, col, line.split("||")[0].strip())
            worksheet_Total_Count.write(row, col + 1, float(line.split("||")[1].strip()))
            worksheet_Total_Count.write(row, col + 2, int(line.split("||")[2].strip()))
            worksheet_Total_Count.write(row, col + 3, line.split("||")[3].strip())
            worksheet_Total_Count.write(row, col + 4, line.split("||")[4].strip())
            row += 1

    workbook.close()
except:
    msgString = "Exiting form the script due to wrong format of data"
    Subject = "MyMicros process MyMicros_MERGER.py DB failed"
    Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
    raise

LogFeed.write('merger finished.. \n')
LogFeed.close()