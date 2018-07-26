# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 13:11:17 2017

@author: Zhangyichao
"""
import os
import cx_Oracle
from log import Logger 
import datetime

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
logger = Logger(logName='oracleHelper.log',logLevel="DEBUG", logger="oracleHelper.py").getlog()

class oracleHelper:
    'Oracle 数据库操作类'
    @staticmethod
    def fetchall(connStr, sql):
        connection = cx_Oracle.connect(connStr);
        cursor = connection.cursor()
        try:
            btime = datetime.datetime.now()
            cursor.execute(sql)
            etime = datetime.datetime.now()
            d1 = etime - btime;
            logger.debug("time:")
            logger.debug(d1)
            logger.debug("sql")
            logger.debug(sql)
            return cursor.fetchall()
        except BaseException, Argument:
            logger.error(u"Argument:%s" % Argument)
            logger.error(u"sql: %s" % sql)
        finally:
            cursor.close()
            connection.close()
    @staticmethod

    def fetchall(connStr,sql,*arg):
        connection = cx_Oracle.connect(connStr);
        cursor=connection.cursor()
        try:           
            btime=datetime.datetime.now()
            cursor.execute(sql,arg)
            etime=datetime.datetime.now()
            d1=etime-btime;
            logger.debug("time:")
            logger.debug(d1)
            logger.debug("sql")
            logger.debug(sql)
            return cursor.fetchall()
        except BaseException,Argument:
            logger.error(u"Argument:%s" % Argument)
            logger.error(u"sql: %s" % sql)
        finally:
            cursor.close()
            connection.close()
    @staticmethod 
    def fetchallXlaidUp(connStr,sql,pid,vid):
        connection = cx_Oracle.connect(connStr);
        cursor=connection.cursor()
        try:    
            btime=datetime.datetime.now()
            cursor.execute(sql,pid=pid,vid=vid)
            etime=datetime.datetime.now()
            d1=etime-btime
            logger.debug("time:")
            logger.debug(d1)
            logger.debug("sql")
            logger.debug(sql)
            return cursor.fetchall()
        except BaseException,Argument:
            logger.error(u"Argument:%s" % Argument)
            logger.error(u"sql: %s" % sql)
        finally:
            cursor.close()
            connection.close()