# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 22:48:07 2017

@author: Zhangyichao
"""
import pandas as pd
import os
import sys
import urllib2
import time
import codecs
import multiprocessing
import datetime
# import traceback

#import logging
#import traceback

from urllib import quote
from log import Logger
from oracleHelp import oracleHelper

reload(sys)
sys.setdefaultencoding('UTF-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
logger = Logger(logName='log.txt', logLevel="INFO", logger="ldr2habseinterface2.py").getlog()

binganshouye = True
binglizhenduan = True
shouyezhenduan = True
shouyeshoushu = True
yizhushuju = False
sample_instock = True
sample_outstock = True
emr_analyse = True
jianchashuju = True
jianyanshuju_zhubiao = True
jianyanshuju_mingxi = True
xlaidup = True
DQMC_INPAT_ORDER_IPM_ALL = True

saveHtml4Dashuju = True
saveHtml4Ldr = False

class ldr2HbaseInterface:
    '天坛脑血管资源库对大数据接口处理xml方法'
    # _pathRootDir="../output20171213/"
    _pathRootDir = "../output" + time.strftime('%Y%m%d', time.localtime(time.time())) + "/"
    _split = "#"
    _root = "<ArrayList>\n%s</ArrayList>\n"
    _items = " <item>\n%s  </item>\n"
    _value = "   <%s>%s</%s>\n"
    _huishangBaseDir = "X:/"

    def __init__(self, connstr, emrConnstr, patient_id, visit_id, patient_name, year,isProduction):
        self._connstr = connstr
        self.emrConnstr = emrConnstr
        self.patient_id = patient_id
        self.visit_id = visit_id
        self.patient_name = patient_name
        self.year = year
        self.isProduction=isProduction

        self._pathXmlDir = self._pathRootDir + "xml/" + self.year + "/"
        self._pathHtmlDir = self._pathRootDir + "html/" + self.year + "/"
        self._pathHtml4LdrDir = self._pathRootDir + "html4ldr/"

        if (not os.path.exists(self._pathXmlDir)):
            os.makedirs(self._pathXmlDir)
        if (not os.path.exists(self._pathHtmlDir)):
            os.makedirs(self._pathHtmlDir)

    def saveXmlFile(self, fileName, xml):
        xmlDir = (self._pathXmlDir + self.patient_id + self._split + self.visit_id + "/")
        fileName += self._split + self.patient_id + self._split + self.visit_id + ".xml"
        if (not os.path.exists(xmlDir)):
            os.mkdir(xmlDir)

        fileHandle = codecs.open(xmlDir + fileName, "w", "utf-8")
        fileHandle.write(xml.decode("utf-8"))
        fileHandle.close()

    def saveHtmlFile4Ldr(self, topic, mr_code, mr_name, _time, html):
        htmlDir = (self._pathHtml4LdrDir + self.patient_id[
                                           len(self.patient_id) - 2:len(self.patient_id)] + "/" + self.patient_id[0:len(
            self.patient_id) - 2].zfill(8) + "/")
        fileName = mr_name + ".html"

        if (not os.path.exists(htmlDir)):
            os.makedirs(htmlDir)

        fileHandle = codecs.open(unicode(htmlDir + fileName), "w", "utf-8")
        fileHandle.write(html.decode("utf-8"))
        fileHandle.close()

    def saveHtmlFile(self, topic, mr_code, mr_name, _time, html):
        timestruct = time.localtime(time.mktime(time.strptime(_time, '%Y-%m-%d %H:%M:%S')))
        htmlDir = (self._pathHtmlDir + self.patient_id + self._split + self.visit_id + "/")
        fileName = topic + self._split + mr_code + self._split + mr_name + self._split + time.strftime(
            '%Y-%m-%d %H_%M_%S', timestruct) + ".html"
        # fileName=topic+self._split+mr_code+self._split+mr_name+self._split+".html"
        if (not os.path.exists(htmlDir)):
            os.mkdir(htmlDir)

        fileHandle = codecs.open(unicode(htmlDir + fileName), "w", "utf-8")
        fileHandle.write(html.decode("utf-8"))
        fileHandle.close()

    def getHttp(self, httpUrl):
        req = urllib2.Request(httpUrl)
        response = urllib2.urlopen(req)
        encoding = response.headers['content-type'].split('charset=')[-1]
        unConten = unicode(response.read(), encoding)
        if unConten.find('<meta http-equiv="content-type" content="text/html;charset=utf-8">') < 0:
            if unConten.find('<title></title>') > 0:
                unConten = unConten.replace('<title></title>',
                                            '<title></title>\n<meta http-equiv="content-type" content="text/html;charset=utf-8">')
            elif unConten.find(u'<title>首都医科大学附属北京天坛医院</title>') > 0:
                unConten = unConten.replace(u'<title>首都医科大学附属北京天坛医院</title>',
                                            u'<title>首都医科大学附属北京天坛医院</title>\n<meta http-equiv="content-type" content="text/html;charset=utf-8">')
        return unConten

    def getHuiShang(self, file_name):
        i = len(self.patient_id)  # 获取病案号的长度
        fileId = self.patient_id[-2:]  # 文件编号
        fileId1 = self.patient_id[0:i - 2]  # 文件次编号

        j = len(fileId1);
        _tffile = ""
        if j >= 8:
            _tffile = "%s%s/%s/%s.html" % (self._huishangBaseDir, fileId, fileId1, file_name)
        # h=0 #//需要补齐位数的大小
        fileId1 = fileId1.zfill(8)
        _tffile = "%s%s/%s/%s.html" % (self._huishangBaseDir, fileId, fileId1, file_name)

        try:
            f = open(_tffile)  # 返回一个文件对象
            line = f.readline()
            html = ""
            while line:
                line = line.replace(self.patient_id, "".rjust(i, "*"))
                html += line.replace(self.patient_name, "".rjust(len(self.patient_name.decode('utf-8')), "*"))
                line = f.readline()
            return html
        except BaseException:
            logger.error(_tffile)
        return " "

    def comment(self, _fileName, _tffile, _tableName, _timefild):
        ignoret=["诊断类型名称"]
        _sql = "select t.PATIENT_ID,t.VISIT_ID"
        _columns = [];
        _columns.append("PATIENT_ID");
        _columns.append("VISIT_ID");
        f = open(_tffile)  # 返回一个文件对象
        line = f.readline()
        while line:
            if line.find("#") < 0:
                tup = tuple(eval(line))
                if tup[1] == "":
                    _sql += (",'' as " + tup[0])
                else:
                    if(tup[2] in ignoret):
                        _sql += ("," + tup[1] + " as " + tup[0])
                    else:
                        _sql += (",t." + tup[1] + " as " + tup[0])
                _columns.append(tup[0]);

            line = f.readline()
        if not _timefild == None:
            _sql += " from " + _tableName + " t inner join dqmc_pat_visit v on t." + _timefild + " between v.admission_date_time and v.discharge_date_time and v.patient_id=t.patient_id and v.patient_id='"
            _sql += self.patient_id
            _sql += "' and v.visit_id='"
            _sql += self.visit_id
            _sql += "'"
        else:
            _sql += " from " + _tableName + " t where t.patient_id='"
            _sql += self.patient_id
            _sql += "' and t.visit_id='"
            _sql += self.visit_id
            _sql += "'"
        f.close()

        rows = oracleHelper.fetchall(self._connstr, _sql)
        data = pd.DataFrame(rows, columns=_columns)

        items = ""
        for index, row in data.iterrows():
            values = ""
            for c in _columns:
                values += self._value % (c, Utility.txml(Utility.toStr(row[c])), c)
            items += self._items % (values)
        xml = self._root % (items)
        self.saveXmlFile(_fileName, xml)

    def order(self, _fileName, _tffile, _tableName, _timefild):
        #  tffile="../transform/"
        fileName_01 = "../transform/yizhushuju.txt"
        tableName_01 = "dqmc_inpat_order"
        _sql_01 = "select t.PATIENT_ID,t.VISIT_ID"

        f = open(fileName_01)  # 返回一个文件对象
        line = f.readline()
        while line:
            if line.find("#") < 0:
                tup = tuple(eval(line))
                if tup[1] == "":
                    _sql_01 += (",'' as " + tup[0])
                else:
                    _sql_01 += (",t." + tup[1] + " as " + tup[0])

            line = f.readline()

        _sql_01 += " from " + tableName_01 + " t where t.patient_id='"
        _sql_01 += self.patient_id
        _sql_01 += "' and t.visit_id='"
        _sql_01 += self.visit_id
        _sql_01 += "' union "
        f.close()
        # logger.info("_sql_01: "+_sql_01)

        fileName_01 = "../transform/DQMC_MZ_EXECUTE_ORDERS_DUA_A.txt"
        tableName_01 = "DQMC_MZ_EXECUTE_ORDERS_DUA_A"
        _sql_01 += "select t.INP_NO as PATIENT_ID,t.VISIT_ID"

        f = open(fileName_01)  # 返回一个文件对象
        line = f.readline()
        while line:
            if line.find("#") < 0:
                tup = tuple(eval(line))
                if tup[1] == "":
                    _sql_01 += (",'' as " + tup[0])
                else:
                    _sql_01 += ("," + tup[1] + " as " + tup[0])
            line = f.readline()

        _sql_01 += " from " + tableName_01 + " t where t.INP_NO='"
        _sql_01 += self.patient_id
        _sql_01 += "' and t.visit_id='"
        _sql_01 += self.visit_id
        _sql_01 += "' union "
        f.close()
        # logger.info("_sql_01: "+_sql_01)

        _sql = "select t.PATIENT_ID,t.VISIT_ID"
        _columns = [];
        _columns.append("PATIENT_ID");
        _columns.append("VISIT_ID");
        f = open(_tffile)  # 返回一个文件对象
        line = f.readline()
        while line:
            if line.find("#") < 0:
                tup = tuple(eval(line))
                if tup[1] == "":
                    _sql += (",'' as " + tup[0])
                else:
                    _sql += (",t." + tup[1] + " as " + tup[0])
                _columns.append(tup[0]);

            line = f.readline()

        _sql += " from " + _tableName + " t where t.cstype in('检查','处置') and t.patient_id='"
        _sql += self.patient_id
        _sql += "' and t.visit_id='"
        _sql += self.visit_id
        _sql += "'"
        f.close()

        # logger.info("_sql: "+_sql_01+_sql)

        rows = oracleHelper.fetchall(self._connstr, _sql_01 + _sql)
        data = pd.DataFrame(rows, columns=_columns)

        items = ""
        for index, row in data.iterrows():
            values = ""
            for c in _columns:
                values += self._value % (c, Utility.txml(Utility.toStr(row[c])), c)
            items += self._items % (values)
        xml = self._root % (items)
        self.saveXmlFile(_fileName, xml)

    def labItem(self, _fileName, _tffile, _tableName):

        try:
            _sql = "select t.PATIENT_ID,t.VISIT_ID"
            _columns = []
            _columns.append("PATIENT_ID")
            _columns.append("VISIT_ID")

            f = open(_tffile)  # 返回一个文件对象
            line = f.readline()

            while line:
                if line.find("#") < 0:
                    tup = tuple(eval(line))
                    if tup[1] == "":
                        _sql += (",'' as " + tup[0])
                    else:
                        if tup[1] == "REPORT_DATE_TIME":
                            _sql += (
                                    ",to_char(to_date(to_char(lr.report_date_time,'yyyy-mm-dd')||' '||replace(lpad(lr.order_no,4),' ','0')||'00','yyyy/mm/dd hh24miss'),'yyyy-mm-dd hh24:mi:ss') as " +
                                    tup[0])
                        else:
                            _sql += (",t." + tup[1] + " as " + tup[0])
                    _columns.append(tup[0]);

                line = f.readline()
            _sql += " from " + _tableName + """ t 
            inner join dqmc_lab_report lr on lr.lab_apply_no=t.lab_apply_no
        inner join dqmc_pat_visit v on v.patient_id=lr.patient_id 
        and lr.report_date_time between v.admission_date_time and v.discharge_date_time
        and v.patient_id='"""
            _sql += self.patient_id
            _sql += "' and v.visit_id='"
            _sql += self.visit_id
            _sql += "'"
            f.close()

            rows = oracleHelper.fetchall(self._connstr, _sql)
            data = pd.DataFrame(rows, columns=_columns)

            items = ""
            for index, row in data.iterrows():
                values = ""
                for c in _columns:
                    values += self._value % (c, Utility.txml(Utility.toStr(row[c])), c)
                items += self._items % (values)
            items = self._root % (items)
            self.saveXmlFile(_fileName, items)
        except  Exception, e:
            logger.error('str(Exception):' + str(Exception))
            logger.error('str(e):' + str(e))
            logger.error('repr(e):' + repr(e))
            logger.error('e.message:' + e.message)

    def shouYe(self, _fileName, _tffile):
        # connection = cx_Oracle.connect(self.emrConnstr)
        # cursor = connection.cursor()
        _sql = "select v.PATIENT_ID,v.VISIT_ID,V.patient_id as INP_NO,v.IDENTITY,v.UNIT_IN_CONTRACT,v.NEXT_OF_KIN_PHONE,v.NEXT_OF_KIN_ADDR,v.RELATIONSHIP,v.NEXT_OF_KIN,v.MAILING_ADDRESS as MAILING_ADDR_CITY_NAME,pv.OUT_REGISTER_DATE,'' as PHONE_NUMBER,'' as BUSINESS_PHONE_PHONE"
        _columns = []
        _columns.append("PATIENT_ID")
        _columns.append("VISIT_ID")
        _columns.append("INP_NO")
        _columns.append("IDENTITY")
        _columns.append("UNIT_IN_CONTRACT")
        _columns.append("NEXT_OF_KIN_PHONE")
        _columns.append("NEXT_OF_KIN_ADDR")
        _columns.append("RELATIONSHIP")
        _columns.append("NEXT_OF_KIN")
        _columns.append("MAILING_ADDR_CITY_NAME")
        _columns.append("OUT_REGISTER_DATE")
        _columns.append("PHONE_NUMBER")
        _columns.append("BUSINESS_PHONE_PHONE")

        f = open(_tffile)  # 返回一个文件对象
        line = f.readline()
        while line:
            if line.find("#") < 0:
                tup = tuple(eval(line))
                if tup[1] == "":
                    _sql += (",'' as " + tup[0])
                else:
                    if tup[1] == "DISCHARGE_PASS":
                        _sql += ", case v.DISCHARGE_PASS when '1' then '医嘱离院'when '2' then '医嘱转院'when '3' then '医嘱转社区'when '4' then '非医嘱离院'when '5' then '死亡'when '9' then '其他' else '其他' end as" + \
                                tup[0]
                    else:
                        _sql += (", " + tup[1] + " as " + tup[0])
                _columns.append(tup[0]);

            line = f.readline()

        if self.isProduction:
            _sql +=""" from pat_visit@toJHEMR v 
            inner join pat_master_index@toJHEMR m on m.patient_id=v.patient_id 
            inner join dqmc_pat_visit pv on pv.patient_id=v.patient_id and pv.visit_id=v.visit_id and v.patient_id='""" + self.patient_id + """' and v.visit_id='""" + self.visit_id + """'
            left join DEPT_DICT@toJHEMR d on d.dept_code=v.DEPT_ADMISSION_TO
            left join DEPT_DICT@toJHEMR d1 on d1.dept_code=v.DEPT_DISCHARGE_FROM            
            """
        else:
            _sql += """ from pat_visit v 
            inner join pat_master_index m on m.patient_id=v.patient_id 
            inner join dqmc_pat_visit pv on pv.patient_id=v.patient_id and pv.visit_id=v.visit_id and v.patient_id='""" + self.patient_id + """' and v.visit_id='""" + self.visit_id + """'
            left join DEPT_DICT d on d.dept_code=v.DEPT_ADMISSION_TO
            left join DEPT_DICT d1 on d1.dept_code=v.DEPT_DISCHARGE_FROM
            """
            #_sql += " from pat_visit v inner join pat_master_index m on m.patient_id=v.patient_id  inner join dqmc_pat_visit pv on pv.patient_id=v.patient_id and pv.visit_id=v.visit_id and v.patient_id='" + self.patient_id + "' and v.visit_id='" + self.visit_id + "'"

        f.close()
        rows = oracleHelper.fetchall(self._connstr, _sql)
        data = pd.DataFrame(rows, columns=_columns)

        items = ""
        for index, row in data.iterrows():
            values = ""
            for c in _columns:
                values += self._value % (c, Utility.txml(Utility.toStr(row[c])), c)
            items += self._items % (values)
        xml = self._root % (items)
        self.saveXmlFile(_fileName, xml)

    def xLaidUp(self, _fileName):
        _sql = """select distinct d.class_code,d.class_name
    from  dqmc_local_sas_dict d 
    left join x_laid_up_standard t  on ( d.map_name like  t.name or d.map_fullname like t.name)
    inner join x_laid_file f on f.patient_id=t.patient_id and f.visit_id=t.visit_id and f.file_no=t.file_no and f.file_name=t.file_name and f.flag_sccf=0 and f.file_flag>='1' and file_flag!='T'
    left join x_laid_time lt on d.sas_name=lt.date_code        
    where d.class_code is not null and t.patient_id=:pid and t.visit_id=:vid 
    order by d.class_code asc"""
        _columns = [];
        _columns.append("class_code");
        _columns.append("class_name");
        rows = oracleHelper.fetchallXlaidUp(self._connstr, _sql, self.patient_id, self.visit_id)
        classData = pd.DataFrame(rows, columns=_columns)

        _sql = """select distinct d.class_code,d.class_name,d.class_childcode,d.class_childname
      from  dqmc_local_sas_dict d 
    left join x_laid_up_standard t  on ( d.map_name like  t.name or d.map_fullname like t.name)
    inner join x_laid_file f on f.patient_id=t.patient_id and f.visit_id=t.visit_id and f.file_no=t.file_no and f.file_name=t.file_name and f.flag_sccf=0 and f.file_flag>='1' and file_flag!='T'
    left join x_laid_time lt on d.sas_name=lt.date_code        
    where d.class_code is not null and t.patient_id=:pid and t.visit_id=:vid 
    order by d.class_code,class_childname asc"""
        _columns.append("class_childcode")
        _columns.append("class_childname")
        rows = oracleHelper.fetchallXlaidUp(self._connstr, _sql, self.patient_id, self.visit_id)
        class_childnameData = pd.DataFrame(rows, columns=_columns)

        _sql = """select distinct d.class_code,d.class_name,d.class_childcode,d.class_childname,t.name,t.values_name,
        lt.time_name
    from  dqmc_local_sas_dict d 
    left join x_laid_up_standard t  on ( d.map_name like  t.name or d.map_fullname like t.name)
    inner join x_laid_file f on f.patient_id=t.patient_id and f.visit_id=t.visit_id and f.file_no=t.file_no and f.file_name=t.file_name and f.flag_sccf=0 and f.file_flag>='1' and file_flag!='T'
    left join x_laid_time lt on d.sas_name=lt.date_code        
    where d.class_code is not null  and t.patient_id=:pid and t.visit_id=:vid 
    order by d.class_code,d.class_childname asc"""
        _columns.append("name");
        _columns.append("values_name");
        _columns.append("time_name");

        rows = oracleHelper.fetchallXlaidUp(self._connstr, _sql, self.patient_id, self.visit_id)
        values_nameData = pd.DataFrame(rows, columns=_columns)

        items = ""
        for index, row in classData.iterrows():
            items += " <" + Utility.tNodeXml(Utility.toStr(row['class_name'])) + ">\n"
            for j, rowc in (class_childnameData[['class_childcode','class_childname']][class_childnameData["class_code"] == Utility.toStr(row['class_code'])]).iterrows():
                items += "  <" + Utility.tNodeXml(Utility.toStr(rowc['class_childname'])) + ">\n"
                for k, rowv in (values_nameData[values_nameData['class_childname'].isin([Utility.toStr(rowc['class_childname'])])]).iterrows():
                    x_varlue = ""
                    x_varlue = Utility.toStr(rowv["values_name"])

                    if Utility.toStr(rowv["time_name"]) != "":
                        timeData = values_nameData[values_nameData["name"].isin([Utility.toStr(rowv["time_name"])])]
                        if len(timeData) > 0:
                            timeRow = next(timeData.iterrows())[1]
                            x_varlue += " "
                            x_varlue += Utility.toStr(timeRow["values_name"])
                            x_varlue = Utility.tDateTimeXml(x_varlue)
                        else:
                            x_varlue = Utility.tDateTimeXml(x_varlue + " 00点00分")

                    items += self._value % (Utility.tNodeXml(Utility.toStr(rowv["name"])), Utility.txml(x_varlue),
                                            Utility.tNodeXml(Utility.toStr(rowv["name"])))

                 # 感觉症状
                if rowc['class_childcode'] == 'B':
                    xnGjzz=u"常见症状"
                    x_value=self.getGanJueZZ(values_nameData[values_nameData['class_childname'].isin([Utility.toStr(rowc['class_childname'])])][['name','values_name']])
                    items += self._value % (Utility.tNodeXml(xnGjzz), Utility.txml(x_value),Utility.tNodeXml(xnGjzz))

                items += "  </" + Utility.tNodeXml(Utility.toStr(rowc['class_childname'])) + ">\n"
            # 既往疾病
            if row['class_code'] == 'C':
                xn_JiwangJB = u"既往疾病"
                x_value = self.getJiwangJibing(
                    values_nameData[values_nameData['class_code'].isin([Utility.toStr(row['class_code'])])][
                        ['name', 'values_name']])
                items += self._value % (Utility.tNodeXml(xn_JiwangJB), Utility.txml(x_value), Utility.tNodeXml(xn_JiwangJB))

            items += " </" + Utility.tNodeXml(Utility.toStr(row['class_name'])) + ">\n"

        xml = self._root % (items)
        self.saveXmlFile(_fileName, xml)

    def toXmlfile(self, emrConnstr):

        fileName = "binglizhenduan"
        tffile = "../transform/"
        tableName = "dqmc_pat_diagnosis"
        if binglizhenduan:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        fileName = "shouyezhenduan"
        tableName = "dqmc_pat_diagnosis_shouye"
        if shouyezhenduan:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        fileName = "shouyeshoushu"
        tableName = "dqmc_pat_operation_copy"
        if shouyeshoushu:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        # fileName="yizhushuju"
        # tableName="dqmc_inpat_order"
        # if yizhushuju:
        #    self.comment(fileName,tffile+fileName+".txt",tableName,None)

        fileName = "yizhushuju"
        tableName = "DQMC_INPAT_ORDER_IPM_ALL"
        if DQMC_INPAT_ORDER_IPM_ALL:
            self.order(fileName, tffile + tableName + ".txt", tableName, None)

        fileName = "sample_instock"
        tableName = "dqmc_semple_instock"
        if sample_instock:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        fileName = "sample_outstock"
        tableName = "dqmc_semple_outstock"
        if sample_outstock:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        fileName = "emr_analyse"
        tableName = "EMR_ANALYSE"
        if emr_analyse:
            self.comment(fileName, tffile + fileName + ".txt", tableName, None)

        fileName = "jianchashuju"
        tableName = "dqmc_exam_report"
        if jianchashuju:
            self.comment(fileName, tffile + fileName + ".txt", tableName, "exam_date_time")

        fileName = "jianyanshuju_zhubiao"
        tableName = "DQMC_LAB_REPORT"
        if jianyanshuju_zhubiao:
            self.comment(fileName, tffile + fileName + ".txt", tableName, "REPORT_DATE_TIME")

        fileName = "jianyanshuju_mingxi"
        tableName = "dqmc_lab_report_items"
        if jianyanshuju_mingxi:
            self.labItem(fileName, tffile + fileName + ".txt", tableName)

        fileName = "binganshouye"
        tableName = "binganshouye"
        if binganshouye:
            self.shouYe(fileName, tffile + fileName + ".txt")

        fileName = "xlaidup"
        if xlaidup:
            self.xLaidUp(fileName)

    def toHtmlfile(self, connstrEmr, htmlUrlBase):
        sqlList = "SELECT PATIENT_ID, VISIT_ID, FILE_NO, FILE_NAME, TOPIC,LAST_MODIFY_DATE_TIME,MR_CODE FROM MR_FILE_INDEX I "
        sqlList += "WHERE VISIT_ID = '" + self.visit_id + "' AND PATIENT_ID = '" + self.patient_id + "' AND regexp_like(i.MR_CODE,'AB030303|AB030302|AB030301|AB030305|AB030306|AB03031001|AB03031002|AB03030902|AP9901|AP9902|AP9904|AP9906|AP9908|AL03030102|AL03030101|AL9904') ORDER BY I.CREATE_DATE_TIME  "
        # +"'AND (TOPIC LIKE '%脑血管住院病历' OR TOPIC LIKE '%脑血管病住院病历' OR TOPIC LIKE '%会商记录' OR TOPIC LIKE '%其他药物治疗' OR TOPIC LIKE '%非药物治疗' OR TOPIC LIKE '%住院期间血管事件及并发症' OR TOPIC LIKE '%出院时情况' OR TOPIC LIKE '%医疗质量评估')  ORDER BY I.CREATE_DATE_TIME "
        rows = oracleHelper.fetchall(connstrEmr, sqlList)
        for i, row in pd.DataFrame(rows, columns=["PATIENT_ID", "VISIT_ID", "FILE_NO", "FILE_NAME", "TOPIC",
                                                  "LAST_MODIFY_DATE_TIME", "MR_CODE"]).iterrows():
            htmlUrl = htmlUrlBase + "?patient_id=" + Utility.toStr(row["PATIENT_ID"]) + "&visit_id="
            htmlUrl += Utility.toStr(row["VISIT_ID"]) + "&file_no="
            htmlUrl += Utility.toStr(row["FILE_NO"]) + "&file_name=" + Utility.toStr(
                row["FILE_NAME"]) + "&topic=" + quote(Utility.toStr(row["TOPIC"])) + "&patient_name=" + quote(
                self.patient_name)
            try:
                if Utility.toStr(row["TOPIC"]).find("会商") > 0:
                    html = self.getHuiShang(Utility.toStr(row["FILE_NAME"]))
                    if saveHtml4Dashuju:
                        self.saveHtmlFile(Utility.toStr(row["TOPIC"]), Utility.toStr(row["MR_CODE"]),
                                          Utility.toStr(row["FILE_NAME"]), Utility.toStr(row["LAST_MODIFY_DATE_TIME"]),
                                          html)
                    if saveHtml4Ldr:
                        self.saveHtmlFile4Ldr(Utility.toStr(row["TOPIC"]), Utility.toStr(row["MR_CODE"]),
                                              Utility.toStr(row["FILE_NAME"]),
                                              Utility.toStr(row["LAST_MODIFY_DATE_TIME"]), html)
                else:
                    html = self.getHttp(htmlUrl)
                    if saveHtml4Dashuju:
                        self.saveHtmlFile(Utility.toStr(row["TOPIC"]), Utility.toStr(row["MR_CODE"]),
                                          Utility.toStr(row["FILE_NAME"]), Utility.toStr(row["LAST_MODIFY_DATE_TIME"]),
                                          html)
                    if saveHtml4Ldr:
                        self.saveHtmlFile4Ldr(Utility.toStr(row["TOPIC"]), Utility.toStr(row["MR_CODE"]),
                                              Utility.toStr(row["FILE_NAME"]),
                                              Utility.toStr(row["LAST_MODIFY_DATE_TIME"]), html)
            except Exception as E:
                # raise TypeError('bad input') from E
                # logger.error(htmlUrl)
                logger.error(E)

    def getGanJueZZ(self,t_rows):
        dict ={u'肌力下降':u'肌力下降',u'失语':u'是',u'构音障碍':u'是',u'痫性发作':u'是',u'单眼盲或视物不清':u'是',
               u'视野缺失':u'是',u'眩晕':u'是',u'复视':u'是',u'头痛':u'是',u'恶心':u'是',u'呕吐':u'是',
               u'咖啡色呕吐物':u'是',u'认知功能障碍':u'是',u'吞咽困难':u'是',u'意识障碍':u'是'}
        s_result=''
        ganjueZZ=[]
        for k,row in t_rows.iterrows():
            if dict.has_key(unicode(row["name"])) and dict[unicode(row["name"])]==unicode(row["values_name"]):
                ganjueZZ.append(unicode(row["name"]))
        if ganjueZZ:
            s_result=' '.join(ganjueZZ)
        return  s_result

    def getJiwangJibing(self, t_rows):
        dict = {u'高血压史': u'有高血压病史', u'高脂血压': u'是', u'冠心病史': u'有冠心病史', u'脑出血': u'有脑出血', u'深静脉血栓': u'是',
                u'糖尿病史': u'有糖尿病史', u'烟雾病': u'有烟雾病史', u'周围血管病': u'有周围血管病', u'蛛网膜下腔出血': u'有蛛网膜下腔出血'}
        s_result = ''
        jiwangJb = []
        for k, row in t_rows.iterrows():
            if dict.has_key(unicode(row["name"])) and dict[unicode(row["name"])] == unicode(row["values_name"]):
                jiwangJb.append(unicode(row["name"]))
        if jiwangJb:
            s_result = ' '.join(jiwangJb)
        return s_result

class Utility:
    '公共静态方法操作类'

    @staticmethod
    def toStr(obj):
        if str(obj) == "None":
            return ""
        else:
            return str(obj)

    @staticmethod
    def txml(_str):
        _str = _str.replace("<", "&lt;")
        _str = _str.replace(">", "&gt;")
        _str = _str.replace("&", "&amp;")
        _str = _str.replace("\"", "&quot;")
        _str = _str.replace("'", "&apos;")
        return _str

    @staticmethod
    def tDateTimeXml(_str):
        _str = _str.replace("年", "-")
        _str = _str.replace("月", "-")
        _str = _str.replace("日", "")
        _str = _str.replace("点", ":")
        _str = _str.replace("分钟", ":")
        _str = _str.replace("分", ":")
        _str += "00"
        return _str

    @staticmethod
    def tNodeXml(_str):
        i = 1

        _str = _str.replace("，", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace(",", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("、", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("/", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("\\", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace(":", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("：", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace(" ", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("(", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace(")", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("（", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("）", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("[", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("]", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("<", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace(">", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("=", "_" + Utility.toStr(i) + "_")
        i += 1
        _str = _str.replace("＿", "_" + Utility.toStr(i) + "_")
        return _str


class handlePatVisit:
    '根据不同的患者集处理信息'

    @staticmethod
    def patVisit(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, pid, vid):
        sql = """select t.patient_id,t.visit_id,M.PATIENT_NAME from dqmc_pat_visit t 
left join dqmc_pat_master m on T.PATIENT_ID=M.PATIENT_ID where t.patient_id='""" + pid + """' and t.visit_id='""" + vid + """' order by t.admission_date_time desc"""
        rows = oracleHelper.fetchall(connstr, sql)
        for i, row in pd.DataFrame(rows, columns=["patient_id", "visit_id", "PATIENT_NAME"]).iterrows():
            lhi = ldr2HbaseInterface(connstr, emrConnstr, Utility.toStr(row["patient_id"]),
                                     Utility.toStr(row["visit_id"])
                                     , Utility.toStr(row["PATIENT_NAME"]), "Test", isProduction)
            if isSaveXml:
                try:
                    lhi.toXmlfile(emrConnstr)
                except Exception, e:
                    logger.error(
                        Utility.toStr(row["patient_id"]) + "#" + Utility.toStr(row["visit_id"]) + "#" + Utility.toStr(
                            row["PATIENT_NAME"]))
                    logger.exception(e)
                    logger.error('str(Exception):' + str(Exception))
                    logger.error('str(e):' + str(e))
                    logger.error('repr(e):' + repr(e))
                    logger.error('e.message:' + e.message)
                    logger.error('########################################################')
                    logger.error('########################################################')
            if isSaveHtml:
                try:
                    lhi.toHtmlfile(emrConnstr, htmlUrlBase)
                except Exception, e:
                    logger.error(
                        Utility.toStr(row["patient_id"]) + "#" + Utility.toStr(row["visit_id"]) + "#" + Utility.toStr(
                            row["PATIENT_NAME"]))
                    logger.error('str(Exception):' + str(Exception))
                    logger.error('str(e):' + str(e))
                    logger.error('repr(e):' + repr(e))
                    logger.error('e.message:' + e.message)
                    logger.error('########################################################')
                    logger.error('########################################################')

    @staticmethod
    def patVisit4Time(connstr, emrConnstr, htmlUrlBase, isSaveXml, isSaveHtml, isProduction, processescount):
        dBegin = datetime.datetime.strptime("2017-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        dEnd = datetime.datetime.strptime("2017-09-30 23:59:59", "%Y-%m-%d %H:%M:%S")
        pool = multiprocessing.Pool(processes=processescount)
        for k in range(0, (dEnd.year - dBegin.year) + 1):

            d1 = datetime.datetime.strptime(str(dBegin.year + k) + "-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            d2 = datetime.datetime.strptime(str(dBegin.year + k) + "-12-31 23:23:59", "%Y-%m-%d %H:%M:%S")
            if dBegin.year - d1.year == 0:
                d1 = dBegin

            if dEnd.year - d2.year == 0:
                d2 = dEnd

            sql = "select t.patient_id,t.visit_id,M.PATIENT_NAME from dqmc_pat_visit t left join dqmc_pat_master m on T.PATIENT_ID=M.PATIENT_ID where t.register_date between  to_date('"
            sql += d1.strftime("%Y-%m-%d %H:%M:%S") + "','yyyy-mm-dd hh24:mi:ss') and  to_date('" + d2.strftime(
                "%Y-%m-%d %H:%M:%S") + "','yyyy-mm-dd hh24:mi:ss') order by t.admission_date_time desc"
            logger.info(sql)
            rows = oracleHelper.fetchall(connstr, sql)
            dataR = pd.DataFrame(rows, columns=["patient_id", "visit_id", "PATIENT_NAME"])
            for i, row in dataR.iterrows():
                if isSaveXml:
                    pool.apply_async(patVisit4Time2Xml, (connstr, emrConnstr,
                                                         Utility.toStr(row["patient_id"]),
                                                         Utility.toStr(row["visit_id"]),
                                                         Utility.toStr(row["PATIENT_NAME"]),
                                                         Utility.toStr(dBegin.year + k), isProduction))
                if isSaveHtml:
                    pool.apply_async(patVisit4Time2Html, (
                        connstr, emrConnstr, htmlUrlBase, Utility.toStr(row["patient_id"]),
                        Utility.toStr(row["visit_id"]),
                        Utility.toStr(row["PATIENT_NAME"]), Utility.toStr(dBegin.year + k), isProduction))
        pool.close()
        pool.join()  # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束


def patVisit4Time2Xml(connstr, emrConnstr, patient_id, visit_id, PATIENT_NAME, years, isProduction):
    # logger.info(patient_id+"#"+visit_id)
    try:
        lhi = ldr2HbaseInterface(connstr, emrConnstr, patient_id, visit_id, PATIENT_NAME, years, isProduction)
        lhi.toXmlfile(emrConnstr)
    except Exception, e:
        logger.error(patient_id + "#" + visit_id + "#" + PATIENT_NAME)
        logger.error('str(Exception):' + str(Exception))
        logger.error('str(e):' + str(e))
        logger.error('repr(e):' + repr(e))
        logger.error('e.message:' + e.message)
        logger.error('########################################################')
        logger.error('\n########################################################')


def patVisit4Time2Html(connstr, emrConnstr, htmlUrlBase, patient_id, visit_id, PATIENT_NAME, years, isProduction):
    try:
        lhi = ldr2HbaseInterface(connstr, emrConnstr, patient_id, visit_id, PATIENT_NAME, years, isProduction)
        lhi.toHtmlfile(emrConnstr, htmlUrlBase)
    except Exception, e:
        logger.error(patient_id + "#" + visit_id + "#" + PATIENT_NAME)
        logger.error('str(Exception):' + str(Exception))
        logger.error('str(e):' + str(e))
        logger.error('repr(e):' + repr(e))
        logger.error('e.message:' + e.message)
        logger.error('########################################################')
        logger.error('\n########################################################')
