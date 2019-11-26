# -*- coding: utf-8 -*-
# @Time     :2019/8/28 12:45
# @Author   :ZhangYichao
# @Site     :
# @File     :testSmtp.py
# @Software :

# post请求提交用户信息到服务器
import urllib.request
import urllib.parse
import ssl
# 发送邮件
import smtplib
from email.mime.text import MIMEText
from email.header import Header
# 日志
import logging
# 系统判断
import platform


logFilename="/root/doc/getHttp.Log"
if platform.system()=="Windows":
    logFilename="getHttp.Log"


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
handler=logging.FileHandler(logFilename,encoding='utf-8')
formatter=logging.Formatter(LOG_FORMAT) # 实例化formatter
handler.setFormatter(formatter)# 为handler添加formatter
logger=logging.getLogger("testSmtp") # 获取名为testSmtp的logger
logger.addHandler(handler) # 为logger添加handler
logger.setLevel(logging.DEBUG)

def sentEmailOutLook(contentStr,subject):
    # 设置邮件服务地址及默认端口号，这里选择的是outlook邮箱
    smtp_server = "smtp.office365.com:587"
    # 设置发送来源的邮箱地址
    mail_account = "yichao0803@outlook.com"
    mail_passwd = "123abcABC!@#"
    sender_email = "yichao0803@outlook.com"
    receiver_email = ["yichao0803@gmail.com"]

    # subject = '资源已可以申请，请尽快填写资料'

    message = MIMEText('邮件正文：'+contentStr, 'plain', 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')

    server = smtplib.SMTP(smtp_server)
    server.connect(smtp_server)  # 25 为 SMTP 端口号
    # 返回服务器特性
    server.ehlo()
    # 进行TLS安全传输
    server.starttls()
    server.login(mail_account, mail_passwd)
    server.sendmail(sender_email, receiver_email, message.as_string())
    # 关闭服务器连接
    server.close()
    logger.info(u"邮件发送成功")

def httpGet(url):
    data = {}

    url_para = urllib.parse.urlencode(data)
    full_url = url + '?' + url_para
    headers = {"User-Agent": "Mozilla"}
    context = ssl._create_unverified_context()
    req = urllib.request.Request(full_url)
    req.add_header("User-Agent",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36")
    with urllib.request.urlopen(req,context=context) as response:
        html = response.read()
        logger.info("response.code: " + str(response.code))
        if response.code==200:
            return html
        else:
            logger.error("response.content: " +html)

if __name__ == '__main__':
    url = 'https://www.bphc.com.cn/front/noroomstaff/checkHavePlanShow'
    urlFirst="https://www.bphc.com.cn/front/first/checkHavePlanShow"
    content = httpGet(url)
    contentStr = content.decode('utf-8')

    content=httpGet(urlFirst);
    contentFirstStr=content.decode("utf-8")


    logger.debug(u"contentStr:" + repr(contentStr)+u" contentFirstStr: "+repr(contentFirstStr))
    logger.debug(u'Result:' + str(not contentStr == '{"code":0,"msg":"操作成功","data":false}') )
    logger.debug(u'ResultFirst:' + str(not contentFirstStr == '{"code":0,"msg":"操作成功","data":false}'))


    if contentStr == '{"code":0,"msg":"操作成功","data":false}' and contentFirstStr == '{"code":0,"msg":"操作成功","data":false}':
        subject = '[Flase]无资源'
        sentEmailOutLook(contentStr+contentFirstStr,subject)
    else:
        subject = '[True]资源已可以申请，请尽快填写资料'
        sentEmailOutLook(contentStr+contentFirstStr,subject)
