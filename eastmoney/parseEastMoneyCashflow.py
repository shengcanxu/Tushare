##
# 将cashflow表的json数据解释到数据库
# 创建数据库的代码：
# CREATE TABLE `cashflow` (
#   `SECUCODE` varchar(20),
#   `REPORT_DATE` varchar(20),
# `SECURITY_NAME_ABBR`  varchar(20),
# `REPORT_TYPE` varchar(20),
# `UPDATE_DATE` varchar(20),
# `CURRENCY`    varchar(20),
#   UNIQUE KEY `idx_cashflow_SECUCODE_REPORT_DATE` (`SECUCODE`,`REPORT_DATE`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
##

import sys
sys.path.append("C:/project/Tushare")
from helper.logger import FileLogger
import time
import json
import pandas as pd
from sqlalchemy import create_engine
import eastmoney.dataprocess.DBHelper as dataGetter


ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")


# read file from filePath
def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        FileLogger.error(ex)
        return False


# get json object from file
def getJsonFromFile(path):
    text = readFile(path)
    if text:
        jsonObjects = json.loads(text)
        return jsonObjects
    else:
        return []


# change XXXXXX.SZ to SZXXXXXX
def tidySecucode(code):
    return code[7:9] + code[0:6]


# change XXXX-XX-XX 00:00:00 to XXXX-XX-XX
def tidyTime(t):
    return t[0:10]


# parse basic infomation of a single cashflow object
def parseCashflowBasicObject(jsonObject):
    sqlstr = "insert into eastmoney.cashflow(SECUCODE,SECURITY_NAME_ABBR,REPORT_DATE,REPORT_TYPE,UPDATE_DATE,CURRENCY) values('%s','%s','%s','%s','%s','%s')"
    sqlstr = sqlstr % (
        tidySecucode(jsonObject["SECUCODE"]),
        jsonObject["SECURITY_NAME_ABBR"],
        tidyTime(jsonObject["REPORT_DATE"]),
        jsonObject["REPORT_TYPE"],
        tidyTime(jsonObject["UPDATE_DATE"]),
        jsonObject["CURRENCY"]
    )
    ENGINE.execute(sqlstr)


# type = 'double' or 'varchar(20)'
def addColumnToDB(jsonObjects, column, type='double'):
    dataGetter.createColunnIfNotExists('cashflow', column, type)
    for jsonObject in jsonObjects:
        value = jsonObject[column]
        secucode = tidySecucode(jsonObject["SECUCODE"])
        reportDate = tidyTime(jsonObject["REPORT_DATE"])
        if value is not None:
            if type == 'double':
                sqlstr = "update eastmoney.cashflow set `%s` = %f where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)
            else:
                sqlstr = "update eastmoney.cashflow set `%s` = '%s' where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)


# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def gatherColumnInfo(jsonSql, jsonObjects, column, type='double'):
    dataGetter.createColunnIfNotExists('cashflow', column, type)
    for jsonObject in jsonObjects:
        value = jsonObject[column]
        secucode = tidySecucode(jsonObject["SECUCODE"])
        if secucode not in jsonSql:
            jsonSql[secucode] = {}
        reportDate = tidyTime(jsonObject["REPORT_DATE"])
        if reportDate not in jsonSql[secucode]:
            jsonSql[secucode][reportDate] = []

        if value is not None:
            if type == 'double':
                jsonSql[secucode][reportDate].append([column, str(value)])
            else:
                jsonSql[secucode][reportDate].append([column, "'"+value+"'"])


# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def executeSql(jsonSql):
    for code in jsonSql.keys():
        for date in jsonSql[code].keys():
            columns = jsonSql[code][date]

            setStr = ""
            for column in columns:
                if len(setStr) == 0:
                    setStr = "set %s=%s" % (column[0], column[1])
                else:
                    setStr = setStr + ", %s=%s" % (column[0], column[1])
            
            sql = "update eastmoney.cashflow %s where SECUCODE='%s' and REPORT_DATE='%s'" % (setStr, code, date)
            ENGINE.execute(sql)


if __name__ == "__main__":
    # 查询语句：SELECT ts_code FROM stock.stockdata;
    stockdf = pd.read_csv("C:/project/Tushare/eastmoney/codewithcompanytype.csv")
    stockList = stockdf[['ts_code', 'companytype']].to_numpy()

    # stockList = [['SZ000002', 4]]

    # add the base info into DB
    for item in stockList:
        code = item[0]
        companyType = item[1]
        #need to process companyType 1-3
        if companyType != 4:
            continue

        FileLogger.info("running on code: %s" % code)
        try:
            path = "C:/project/stockdata/EastMoneyCashflow/%s.json" % code
            jsonObjects = getJsonFromFile(path)

            # add the base info into DB
            for jsonObject in jsonObjects:
                parseCashflowBasicObject(jsonObject)

            # add other info into DB
            jsonSql = {}
            gatherColumnInfo(jsonSql, jsonObjects, 'SALES_SERVICES', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RECEIVE_OTHER_OPERATE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OPERATE_INFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'BUY_SERVICES', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_STAFF_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_ALL_TAX', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_OTHER_OPERATE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OPERATE_OUTFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETCASH_OPERATE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'WITHDRAW_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RECEIVE_INVEST_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DISPOSAL_LONG_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DISPOSAL_SUBSIDIARY_OTHER', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RECEIVE_OTHER_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_INVEST_INFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CONSTRUCT_LONG_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVEST_PAY_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OBTAIN_SUBSIDIARY_OTHER', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_OTHER_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_INVEST_OUTFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETCASH_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ACCEPT_INVEST_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SUBSIDIARY_ACCEPT_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RECEIVE_LOAN_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ISSUE_BOND', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RECEIVE_OTHER_FINANCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_FINANCE_INFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_DEBT_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ASSIGN_DIVIDEND_PORFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SUBSIDIARY_PAY_DIVIDEND', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PAY_OTHER_FINANCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FINANCE_OUTFLOW_OTHER', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_FINANCE_OUTFLOW', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETCASH_FINANCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RATE_CHANGE_EFFECT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CCE_ADD', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'BEGIN_CCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'END_CCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETPROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ASSET_IMPAIRMENT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FA_IR_DEPR', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'IA_AMORTIZE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DISPOSAL_LONGASSET_LOSS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FAIRVALUE_CHANGE_LOSS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FINANCE_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVEST_LOSS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DEFER_TAX', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DT_ASSET_REDUCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DT_LIAB_ADD', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVENTORY_REDUCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_RECE_REDUCE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_PAYABLE_ADD', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_NETCASH_OTHERNOTE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETCASH_OPERATENOTE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'END_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'BEGIN_CASH', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CCE_ADDNOTE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPINION_TYPE', 'varchar(100)')

            executeSql(jsonSql)
                
            time.sleep(0.1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("parse cashflow error on code: %s" % code)
            time.sleep(3)

    

