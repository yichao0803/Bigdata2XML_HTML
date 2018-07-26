# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 13:10:40 2017

@author: Zhangyichao
"""
import traceback
from log import Logger 
from ldr2habseinterfacePool import handlePatVisit

logger = Logger(logName='begin.txt',logLevel="INFO", logger="begin.py").getlog()

if __name__=="__main__":       
    # connstr="workbench_tt/workbench_tt@JHEMR"
    # emrConnstr="workbench_tt/workbench_tt@JHEMR"
    # htmlUrlBase="http://172.28.10.106/wwwroot/patinfo/EmrFileInfoapi.aspx"
    connstr="workbench_tt/workbench_tt@JHOD"
    emrConnstr="emr/emr@JHEMR"
    htmlUrlBase="http://172.28.10.106/jh.workbench.web/patinfo/EmrFileInfoapi.aspx"
    isProduction = True
    isSaveXml=False
    isSaveHtml=True
    processescount=1
    try:
        
        handlePatVisit.patVisit4Time(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,processescount)
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction)
        #414656
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'353891','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'330335','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'524197','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'540350','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'401176','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'76577','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'551752','2')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,isProduction,'392136','1')
        #401176 76577#1 392136
        #handlePatVisit.patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml,isProduction, '521517', '1')
        #handlePatVisit.patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, '438322', '2')
        #handlePatVisit.patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, '535696', '1')
        #handlePatVisit.patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, '552702', '1')
        #handlePatVisit.patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, '328485', '1')


    except IOError as info:
        logger.error(u"info: %s" % info)
        logger.error(u"traceback: %s" % traceback.print_exc())
        logger.error(u'traceback.format_exc():\n%s' % traceback.format_exc())
        logger.error(u'########################################################')
        logger.error(u'\n########################################################')