# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 17:23:03 2019

@author: sliu439
"""
import smtplib
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

class msg:
    
     def __init__(self,sender,recipients):
         
         self.sender=sender
         self.recipients=recipients


     def send(self,subject,body,attachment,filename):
         
        s=smtplib.SMTP('relay.corp.bloomberg.com')
        #rcpts=[r.strip() for r in self.recipients.split(',') if r]
        msg=MIMEMultipart()
        msg['Subject']=subject
        msg['Body'] = body
        msg['To']= self.recipients
        msg.attach(MIMEText(msg['Body'],'text',_charset='utf-8'))
        if attachment:
            part = MIMEBase('application', 'vnd.ms-excel')
            part.set_payload(open(attachment, 'rb').read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment',filename=filename)
            msg.attach(part)

        s.sendmail(self.sender, self.recipients, msg.as_string() )
        s.close()    

        