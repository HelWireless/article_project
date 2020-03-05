#-*- coding:utf-8 -*-  
""" 
Author:hel 
License: Apache Licence 
File: send_email.py 
Time: 2020/01/06
Version: 1.0
@Function:
"""

import smtplib
from email.mime.text import MIMEText
from email.header import Header
QQ_OPTIONS = {
                "server": "smtp.qq.com",
                "port": 465,
                "from": "745572795@qq.com",
                "subject":"监控邮件",
                "pwd": "obwslktmfnbxbcec",
                "send_to": "liu.he@tinyzk.com"
               }


def send_email(mail, host, error):
    message = MIMEText('服务器{}挂了，其中错误原因是{}，赶快来修理！'.format(host, error), 'plain', 'utf-8')
    message['From'] = Header("监控机器", 'utf-8')
    message['To'] = Header("liu.he@tinyzk.com", 'utf-8')
    message['Subject'] = Header("监控邮件", 'utf-8')
    smtp = smtplib.SMTP_SSL(mail["server"], mail["port"])
    smtp.login(mail["from"], mail["pwd"])
    smtp.sendmail(mail['from'], [mail['send_to']], message.as_string())
    smtp.quit()

if __name__=='__main__':
    test_host = "大数据"
    error = "你太胖啦"
    send_email(QQ_OPTIONS, test_host, error)
    print("发送完成")