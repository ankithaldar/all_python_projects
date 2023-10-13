'''LSRMyMicros_Extraction_EUG
Version History:
Version :   1.0
Author  :   Ranveer Singh (EMEA WNS)
Date    :   May, 2018
'''
from bs4 import BeautifulSoup
import cx_Oracle
import sys
import os
import os.path
import sys
import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select
import time
sys.path.append('../')
try:
    from Feeds_Scripts import LSRMyMicros_Conf as Conf
    from Feeds_Scripts import CommonFunction
    Ob_CommonFunction = CommonFunction.SendMail()
    # print(Conf.Cheetah_Last_Logical_Date)
except ImportError as e:
    print("Error occured while reading the config file:" + e)
    raise
#below parameters are getting read from configuration file
APPLICATION_NAME=Conf.APPLICATION_NAME
PROCESS_NAME=Conf.PROCESS_NAME
SUBPROCESS_NAME='EXTRACTION_EUG'
ApplicationPath=Conf.ApplicationPath
ProcessingPath=Conf.ProcessingPath
TargetPath=Conf.TargetPath
LogPath=Conf.LogPath
urllogon = Conf.urllogon
username = Conf.username_EUG
password = Conf.password_EUG
company = Conf.company_EUG
OraConnectionString=Conf.OraConnectionString
RunningFlag=Conf.RunningFlag
ImplWaitWebDrv=Conf.ImplWaitWebDrv
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
# RunningFlag=ApplicationPath+'EUG_Runningfal16Dec.txt'
with open(RunningFlag, 'r') as f:
    LogicalDate = f.readline()
    LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
print("prepare extraction")

Current_Date=datetime.date.today().isoformat()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'EUG_MyMicrosExtraction_Log_'+str(Current_DTTM)+'.txt'
DeltaExtractFile=ProcessingPath+'DeltaExtract_MyMicros_EUG_'+str(LogicalDate)+'.xlsx'
HTMLParseFile = ProcessingPath + 'Details_HTMLParseFile_MyMicros_EUG_' + str(LogicalDate) + '.txt'

LogFeed=open(LogFile,'w')
StartTime=datetime.datetime.now().replace(microsecond=0)

def ValidateCycyleDate(query,Type):
    try:
        con = cx_Oracle.connect(OraConnectionString)
        LogFeed.write('Database connection established successfully.. \n')
        cur = con.cursor()
        print(query)

        if (Type=='SELECT'):
            cur.execute(query)
            DateVal = cur.fetchone()
            print(str(DateVal))
            return str(DateVal[0])
        else:
            cur.execute(query)
            cur.execute("COMMIT")
    except cx_Oracle.DatabaseError as e:
        print('Error:' + str(e))
        LogFeed.write("\n Error:' + str(e)\n")
        msgString = "\n Error:' + str(e)\n"
        Subject = "MyMicros process LSRMyMicros_EUG.py DB failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        raise
    finally:
        con.close()


def DownloadRawData(HTMLParseFile,LogicalDate):
    try:

        print(time.ctime())
        LogFeed.write("\n EUG extraction process started at {}.".format(time.ctime()))
        # driver = webdriver.Chrome('C:\\WNS\chromedriver.exe',service_args=["--verbose"])
        driver = webdriver.Firefox()
        driver.implicitly_wait(ImplWaitWebDrv)
        driver.get(urllogon)
        try:
            alert = driver.switch_to_alert()
            alert.accept()
            LogFeed.write("\nAlert accepted")
        except:
            print("no alert to accept")
        LogFeed.write("\n Logging-in into MyMicros")
        driver.find_element_by_id('usr').send_keys(username)
        driver.find_element_by_id('cpny').send_keys(company)
        driver.find_element_by_id('pwd').send_keys(password)
        driver.find_element_by_id('Login').click()
        driver.switch_to.frame('sideMenu')
        # time.sleep(30)
        # WebDriverWait(driver, ImplWaitWebDrv).until(ec.element_to_be_clickable((By.ID, '3Items'))).click()
        time.sleep(60)
        print("2-call")
        driver.find_element_by_link_text('More Reports...').click()
        time.sleep(60)
        driver.switch_to.default_content()
        driver.switch_to.frame('myPage')
        time.sleep(120)
        print("3-call")
        driver.find_element_by_xpath("//h2[text()='Comparison Reports']").click()
        print("4-call")
        time.sleep(20)
        driver.find_element_by_xpath("//a[text()='KPIs by Location']").click()
        print("5-call")
        time.sleep(20)
        # driver.execute_script()
        # driver.switch_to.default_content()
        # driver.switch_to.frame('myPage')
        driver.find_element_by_id('calendarBtn').click()
        time.sleep(20)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10)
        driver.switch_to.frame('calendarFrame')
        print("6-call")
        FormatDate = datetime.datetime.strftime(LogicalDate, '%A, %b %#d, %Y')
        SetDate = '//*[@title="Select ' + str(FormatDate) + '"]'
        FormatYr = datetime.datetime.strftime(LogicalDate, '%Y')
        select = Select(driver.find_element_by_id('selectYear'))
        select.select_by_visible_text(FormatYr)
        print(SetDate)
        time.sleep(20)
        # driver.find_element_by_id('clear0').click()
        # time.sleep(10)
        WebDriverWait(driver, ImplWaitWebDrv).until(ec.element_to_be_clickable((By.XPATH, SetDate))).click()
        print("7-call")
        time.sleep(10)
        driver.switch_to.default_content()
        driver.switch_to.frame('myPage')
        time.sleep(10)
        driver.find_element_by_id('Run Report').click()
        time.sleep(120)
        driver.switch_to.frame('reportsFrame')
        print("8-call")
        time.sleep(20)
        HtmlPage = driver.page_source
        print("Call ParseHtml2 with inner page")
        RetCode=ParseHtml2(HtmlPage,HTMLParseFile)
        driver.close()
        if (RetCode==101):
            return 101
        else:
            return 2001
    except Exception as e:
        print(e)
        try:
            driver.close()
            print("Closing Driver with error")
            return 2001
        except Exception as e:
            print(e)
            print("Closing Driver with error")
            return 2001




def ParseHtml2(HTMLPage,HTMLParseFile):
    soup2 = BeautifulSoup(HTMLPage, 'html.parser')
    # with open(ProcessingPath + 'tttt.txt', 'w') as f:
    #     f.write(str(soup2))
    # print("inner")
    html1 = list(soup2.children)[0]
    body1 = list(html1.children)[2]
    div = list(body1.children)[9]
    t1 = list(div.children)[1]
    t3 = list(t1.children)[3]
    sys.stdout.flush()
    with open(HTMLParseFile, 'w') as f1:
        for item in list(t3.children):
            print("**********")
            print(item)
            try:
                line1 = item.text.replace('\n', '||')
                # print("%%%%%%%%%\n{}".format(line1))

                line = line1.replace("\\", "'").strip().strip('||').strip()
                f1.write(line + '\n')
                print(line)
            except Exception as e:
                print(e)
                pass
    return 101


def main():
    with open(RunningFlag, 'r') as f:
        LogicalDate = f.readline()
        LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
    print("call extarction in loop for 10 times from Logical_date {}".format(LogicalDate))
    LogFeed.write("LSRMyMicros_Extraction_EUG Extraction Process has been started at {} \n".format(datetime.datetime.now().replace(microsecond=0)))
    LogFeed.write("Starting Mymicros_ext process from logicaldate : {}\n".format(LogicalDate))
    print("This is temporary code for history insert")
    # while (LogicalDate < datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()):
    #     HTMLParseFile = ProcessingPath + 'Details_HTMLParseFile_MyMicros_EUG_' + str(LogicalDate) + '.txt'
    i=0
    while(i < 10):
        LogFeed.write("\n Calling EUG DownloadRawData for {} times".format(i+1))
        print("\n Calling EUG DownloadRawData for {} times for logical Date {}".format(i+1,LogicalDate))
        RetCode=DownloadRawData(HTMLParseFile,LogicalDate)
        print(str(RetCode))
        if(RetCode == 101):
            LogFeed.write("\n No Exception occured so breaking loop at loop number : {}".format(i+1))
            print("no Exception occured")
            break;
        else:
            print("loop again to extract records")
            LogFeed.write("\n Exception Occured looping again for {}".format(i+1))
            i+=1
            if(i >=10 ):
                print("exit from the script")
                LogFeed.write("\n Exiting form the script after looping for 10 times")
                msgString = "Exiting form the script after looping for 10 times"
                Subject = "MyMicros process LSRMyMicros_EUG.py DB failed"
                Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
                LogFeed.close()
                exit(1)
    print("verify  downloaded files")
    if ( os.path.exists(HTMLParseFile)):
        print("file exits")
    else:
        print("File does not exists")
        msgString = "Extracted file does not exist.\n Failing the process"
        Subject = "MyMicros process LSRMyMicros_EUG.py DB failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        LogFeed.close()
        exit (1)

    Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
        APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME, LogicalDate)
    ValidateCycyleDate(Query, 'INSERT')
    print("LSRMyMicros_Extraction_EUG Process Took '{}' time\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.write("LSRMyMicros_Extraction_EUG Process Took '{}' time\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    # LogicalDate = LogicalDate + datetime.timedelta(days=1)
    # with open(RunningFlag,'w') as f:
    #     f.write(str(LogicalDate))
    LogFeed.close()
main()