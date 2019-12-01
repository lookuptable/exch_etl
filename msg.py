# -*- coding: utf-8 -*-
"""
Module used to send msg

Parameters
---------
SMTP_SERVER : str
     smtp server
"""
import smtplib
from email.mime.text import MIMEText
from email import Encoders

from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase

SMTP_SERVER="relay.corp.bloomberg.com"


class Msg(object):

    def __init__(self, sender, recipients):
        self.sender = sender
        self.recipients = recipients

    def send(self, subject, body, attachment, filename):
        """send msg with subject, body, attachment"""
        s = smtplib.SMTP(SMTP_SERVER)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['Body'] = body
        msg['To'] = self.recipients
        msg.attach(MIMEText(msg['Body'], 'text', _charset='utf-8'))
        if attachment:
            part = MIMEBase('application', 'vnd.ms-excel')
            part.set_payload(open(attachment, 'rb').read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(part)

        s.sendmail(self.sender, self.recipients, msg.as_string())
        s.close()

