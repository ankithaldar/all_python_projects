import sys
import smtplib
import sys
import math
from email.mime.text import MIMEText
sys.path.append('../')
sys.path.append('C:/WNS/Projects/')

class SendMail():
    def SendMail(self,Email_TO_DL,msgString,Subject):
        self.Email_TO_DL = Email_TO_DL
        self.msgString = msgString
        self.Subject = Subject
        print("Semding mail ...")
        sys.stdout.flush()
        self.s = smtplib.SMTP(host='smtp-prod.starbucks.net')
        self.msg = MIMEText(self.msgString)
        self.msg['Subject'] = self.Subject
        self.msg['From'] = 'prod-wns-EMEA@starbucks.com'
        self.msg['To'] = self.Email_TO_DL

        self.s.sendmail('prod-wns-EMEA@starbucks.com',list(self.Email_TO_DL.split(',')), self.msg.as_string())
        self.s.quit()