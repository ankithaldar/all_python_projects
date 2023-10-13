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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
sys.path.append('../')
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
SUBPROCESS_NAME='EXTRACTION_SKS'
ApplicationPath=Conf.ApplicationPath
ProcessingPath=Conf.ProcessingPath
TargetPath=Conf.TargetPath
LogPath=Conf.LogPath
urllogon = Conf.urllogon
username = Conf.username_SKS
password = Conf.password_SKS
company = Conf.company_SKS
OraConnectionString=Conf.OraConnectionString
RunningFlag=Conf.RunningFlag
ImplWaitWebDrv=Conf.ImplWaitWebDrv
Email_TO_DL=Conf.Email_TO_DL
Alert_TO_DL=Conf.Alert_TO_DL
with open(RunningFlag, 'r') as f:
    LogicalDate = f.readline()
    LogicalDate = datetime.datetime.strptime(LogicalDate, '%Y-%m-%d').date()
print("prepare SKS extraction")

Current_Date=datetime.date.today().isoformat()
Current_DTTM=datetime.datetime.today().strftime("%Y-%m-%d@%H.%M.%S")
LogFile=LogPath+'SKS_MyMicrosExtraction_Log_'+str(Current_DTTM)+'.txt'
DeltaExtractFile=ProcessingPath+'DeltaExtract_MyMicros_SKS_'+str(LogicalDate)+'.xlsx'
OverAllCountFile=ProcessingPath+'OverallCount_MyMicros_SKS_'+str(LogicalDate)+'.txt'
HTMLParseFile=ProcessingPath+'Details_HTMLParseFile_MyMicros_SKS_'+str(LogicalDate)+'.txt'
StartTime=datetime.datetime.now().replace(microsecond=0)

LogFeed=open(LogFile,'w')

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
        msgString = "Exiting form the scriptdue to connection issue"
        Subject = "MyMicros process MyMicros_SKS.py DB failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        raise
    finally:
        con.close()


def DownloadRawData():
    try:

        print(time.ctime())
        LogFeed.write("\n SKS extraction process started at {}.".format(time.ctime()))
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
        LogFeed.write("\n Sleep for 30 secs")
        time.sleep(60)
        WebDriverWait(driver, ImplWaitWebDrv).until(ec.element_to_be_clickable((By.LINK_TEXT, 'Daily Operations SKS'))).click()
        time.sleep(60)
        print("2-call")
        driver.switch_to.default_content()
        driver.switch_to.frame('myPage')
        time.sleep(20)
        driver.find_element_by_id('calendarBtn').click()
        time.sleep(20)
        driver.switch_to.frame('calendarFrame')
        print("4-call")
        FormatDate=datetime.datetime.strftime(LogicalDate,'%A, %b %#d, %Y')
        SetDate='//*[@title="Select '+str(FormatDate)+'"]'
        print(SetDate)
        time.sleep(10)
        # driver.find_element_by_xpath(SetDate).click()
        WebDriverWait(driver, ImplWaitWebDrv).until(ec.element_to_be_clickable((By.XPATH, SetDate))).click()
        print("5-call")
        driver.switch_to.default_content()
        driver.switch_to.frame('myPage')
        time.sleep(10)
        driver.find_element_by_id('Run Report').click()
        time.sleep(10)
        driver.switch_to.frame('reportsFrame')
        print("7-call")
        time.sleep(10)
        WebDriverWait(driver, ImplWaitWebDrv).until(
            ec.element_to_be_clickable((By.CSS_SELECTOR, "div[onclick*='SKS_DiscDailyDetail_VAT']"))).click()
        # driver.find_element_by_css_selector("div[onclick*='SKS_DiscDailyDetail_VAT']").click()
        print("8-call")
        # outer value
        time.sleep(120)
        OuterPage = driver.page_source
        time.sleep(100)
        WebDriverWait(driver, ImplWaitWebDrv).until(ec.element_to_be_clickable((By.CSS_SELECTOR, "div[onclick*='discountreportid=405388056']"))).click()
        # driver.find_element_by_css_selector("div[onclick*='discountreportid=405388056']").click()
        time.sleep(300)
        InnerPage = driver.page_source
        print(type(InnerPage))
        print("9-call")
        print("Parsing file 1")
        soup = BeautifulSoup(OuterPage, 'html.parser')
        # soup=BeautifulSoup(OuterPage,'html.parser')
        # print(list(soup.children)[0])
        # print(list(soup.children)[1])
        # print(list(soup.children)[2])
        with open(ProcessingPath + 'tttt1.txt', 'w') as f:
            f.write(str(soup))
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        # soup = BeautifulSoup(open(ProcessingPath + 'tttt.txt'), 'html.parser')
        # print(soup)
        html = list(soup.children)[0]
        print("44")
        body = list(html.children)[2]
        print("7777")
        div1 = list(body.children)[9]
        print("888")
        table = list(div1.children)[1]
        print("888")
        tbody = list(table.children)[3]
        print("9999")
        for DivFieldText in list(tbody.children):
            # checkText=''
            print("00000000000000")
            # print(DivFieldText)
            try:
                checkText = DivFieldText.find('div').text
                # print("tryBlock")
                # print(checkText)
                if (str(checkText) == 'Vitality'):
                    print("Found the point !")
                    LogFeed.write('Found the Vitality end point \n')
                    # print(DivFieldText)
                    # list(tbody.children)[3].text -- all fields Future use

                    StrName = list(DivFieldText.children)[1].text.strip()
                    # print(type(StrName))
                    # print(StrName)
                    TotalDisc = float(list(DivFieldText.children)[5].text.strip(')').replace(',', '').replace('(','-'))
                    # print(type(TotalDisc))
                    # print(TotalDisc)
                    # Get the count
                    TotalCount = int(list(DivFieldText.children)[9].text.strip(')').replace(',', '').replace('(','-'))
                    print("TotalCount is for SKS : " + str(TotalCount))
                    # Validate Feed
                    if (StrName == 'Vitality' and TotalDisc != 0.0 and TotalCount != 0):
                        print("Successfully Extracted required information")
                        LogFeed.write('Successfully Extracted required information for Vitality end point \n')
                        with open(OverAllCountFile, 'w') as fb:
                            fb.write(
                                StrName + '||' + str(TotalDisc) + '||' + str(TotalCount) + '||SKS||' + str(LogicalDate))
                            print(StrName + '||' + str(TotalDisc) + '||' + str(TotalCount) + '||SKS||' + str(LogicalDate))
                            LogFeed.write(
                                'File for total count' + OverAllCountFile + ' has been generated for the' + LogicalDate + ' \n')
                        fb.close()
                        break
                    else:
                        print("please check with source, source might have chnaged something at their end \n")
                        LogFeed.write(
                            'Failed !!! \n\nplease check with source, source might have chnaged something at their end\n')
                        msgString = "please check with source, source might have chnaged something at their end"
                        Subject = "MyMicros process MyMicros_SKS.py failed"
                        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
                        # sendmail
                        exit(1)
                # break
            except Exception as e:
                print(e)
        print("Call ParseHtml2 with inner page")
        RetCode=ParseHtml2(InnerPage)
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




def ParseHtml2(Page6):
    soup2 = BeautifulSoup(Page6, 'html.parser')
    # with open(ProcessingPath + 'tttt.txt', 'w') as f:
    #     f.write(str(soup2))
    #print(soup2)
    print("inner")
    html1 = list(soup2.children)[0]

    body1 = list(html1.children)[2]
    l1 = list(body1.children)[9]
    t1 = list(l1.children)[1]
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
    print("call extarction in loop for 10 times from Logical_date {}".format(LogicalDate))
    LogFeed.write("MyMicros Extraction Process has been started at {} \n".format(datetime.datetime.now().replace(microsecond=0)))
    LogFeed.write("Starting Mymicros_ext process from logicaldate : {}\n".format(LogicalDate))

    i=0
    while(i < 10):
        LogFeed.write("\n Calling SKS DownloadRawData for {} times".format(i+1))
        print("\n Calling SKS DownloadRawData for {} times for logical Date {}".format(i+1,LogicalDate))
        RetCode=DownloadRawData()
        print(str(RetCode))
        if(RetCode == 101):
            LogFeed.write("\n No Exception occured so beaking loop at loop number : {}".format(i+1))
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
                Subject = "MyMicros process MyMicros_SKS.py failed"
                Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
                LogFeed.close()
                exit(1)
    print("verify  downloaded files")
    if (os.path.exists(OverAllCountFile) and os.path.exists(HTMLParseFile)):
        print("both file exits")
    else:
        print("File does not exists")
        msgString = "File does not exists"
        Subject = "MyMicros process MyMicros_SKS.py failed"
        Ob_CommonFunction.SendMail(Alert_TO_DL, msgString, Subject)
        LogFeed.close()
        exit (1)

    Query = "INSERT INTO DEPT_INTL.TCYCLE_STATUS VALUES('{}','{}','{}',to_date('{}','YYYY-MM-DD'),'COMPLETE',Current_timestamp(0))".format(
        APPLICATION_NAME, PROCESS_NAME, SUBPROCESS_NAME, LogicalDate)
    ValidateCycyleDate(Query, 'INSERT')
    print("MyMicros Process Took '{}' time\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.write("MyMicros Process Took '{}' time\n******BYE******".format(datetime.datetime.now().replace(microsecond=0)-StartTime))
    LogFeed.close()
main()