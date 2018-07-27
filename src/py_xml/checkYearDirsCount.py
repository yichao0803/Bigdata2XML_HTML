#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by ZhangYichao on 2018/5/28

import os
import time
import json
from log import Logger


logger = Logger(logName='checkYearDirsCount.log',logLevel="DEBUG", logger="checkYearDirsCount.py").getlog()
# 打开文件
pathRootDir = "../../output/" + time.strftime('%Y%m%d', time.localtime(time.time())) + "/"
checkFile = u"../统计年份病历.txt"

class checkYearDirsCount:
    '检测每年导出的文件夹数是否正确'
    @staticmethod
    def checkYear(year, count):
        file = open(checkFile)
        dictYear = json.loads(file.read());
        if dictYear.has_key(year):
            return dictYear[year], dictYear[year] == count;
        return False;

    @staticmethod
    def checks():
        dirs=os.listdir(pathRootDir)
        # 输出所有文件和文件夹
        for dir in dirs:# %Y%m%d
            child=os.path.join(pathRootDir,dir)
            if os.path.isdir(child):
                logger.info("-------------"+dir+"----------------")
                for subDir in os.listdir(child):# html /xml
                    subchild = os.path.join(child, subDir)
                    subChildLen=len(os.listdir(subchild))
                    if os.path.isdir(subchild):
                        result=checkYearDirsCount.checkYear(subDir,subChildLen)
                        msg="{0}年： 实际导出文件夹[{1:10}] 应导出文件夹[{2:10}] 结果：{3:5}".format(subDir,
                                                                                   subChildLen,result[0],str(result[1]))
                        msgFals=msg+" 差值：[{0}]".format(result[0]-subChildLen)
                        if result[1]:
                            logger.info(msg)
                            #print(msg)
                        else:
                            logger.warning(msgFals)
                            #print("\033[0;31m"+msgFals+ "\033[0m")
                logger.info("-------------" + dir + "----------------")
            else:
                logger.info(len(child))
                logger.info(file)