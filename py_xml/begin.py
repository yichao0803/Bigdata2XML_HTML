# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 13:10:40 2017

@author: Administrator
"""
import traceback
from log import Logger 
from ldr2habseinterfacePool import handlePatVisit

logger = Logger(logName='begin.txt', logLevel="INFO", logger="begin.py").getlog()

if __name__=="__main__":       
# =============================================================================
#     connstr="workbench_tt/workbench_tt@JHEMR"
#     emrConnstr="workbench_tt/workbench_tt@JHEMR"
#     htmlUrlBase="http://172.28.10.106/wwwroot/patinfo/EmrFileInfoapi.aspx"
# =============================================================================
    
    connstr="workbench_tt/workbench_tt@JHOD"
    emrConnstr="emr/emr@JHEMR"
    htmlUrlBase="http://172.28.10.106/jh.workbench.web/patinfo/EmrFileInfoapi.aspx"
    isSaveXml=False
    isSaveHtml=True
    processescount=1
    try:
        
        handlePatVisit.patVisit4Time(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,processescount)
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml)
        #414656
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'353891','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'330335','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'524197','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'540350','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'401176','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'76577','1')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'551752','2')
        #handlePatVisit.patVisit(connstr,emrConnstr,htmlUrlBase,isSaveXml,isSaveHtml,'392136','1')
        #401176 76577#1 392136
        
    except  Exception,e:
        logger.error('str(Exception):\t', str(e))
        logger.error('str(e):\t\t', str(e))
        logger.error('repr(e):\t', repr(e))
        logger.error('e.message:\t', e.message)
        logger.error('traceback.print_exc():' % traceback.print_exc()) 
        logger.error('traceback.format_exc():\n%s' % traceback.format_exc())
        logger.error('########################################################')
        logger.error('\n########################################################')