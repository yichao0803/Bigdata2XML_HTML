# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 22:48:07 2017

@author: Zhangyichao
"""
import logging
import logging.handlers
#import sys

#reload(sys)
#sys.setdefaultencoding('gbk')

logDir=r"C:\logs"

class Logger():
    def __init__(self, logName, logLevel, logger):

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(logName,encoding='utf-8')
        fh.setLevel(logLevel)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logLevel)

        # 定义handler的输出格式    
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s - %(message)s -[%(filename)s:%(lineno)d]')
        fh.setFormatter(log_format)
        ch.setFormatter(log_format)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
   
    def getlog(self):
        return self.logger

