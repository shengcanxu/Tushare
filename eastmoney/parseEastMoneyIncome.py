##
# 将income表的json数据解释到数据库
# 创建数据库的代码：
# CREATE TABLE `income` (
#   `SECUCODE` varchar(20),
#   `REPORT_DATE` varchar(20),
# `SECURITY_NAME_ABBR`  varchar(20),
# `REPORT_TYPE` varchar(20),
# `UPDATE_DATE` varchar(20),
# `CURRENCY`    varchar(20),
#   UNIQUE KEY `idx_income_SECUCODE_REPORT_DATE` (`SECUCODE`,`REPORT_DATE`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
##

import sys
sys.path.append("C:/project/Tushare")
from helper.logger import FileLogger
import time
import json
import pandas as pd
from sqlalchemy import create_engine


ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")
COLUMNS = {}


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


# parse basic infomation of a single income object
def parseIncomeBasicObject(jsonObject):
    sqlstr = "insert into eastmoney.income(SECUCODE,SECURITY_NAME_ABBR,REPORT_DATE,REPORT_TYPE,UPDATE_DATE,CURRENCY) values('%s','%s','%s','%s','%s','%s')"
    sqlstr = sqlstr % (
        tidySecucode(jsonObject["SECUCODE"]),
        jsonObject["SECURITY_NAME_ABBR"],
        tidyTime(jsonObject["REPORT_DATE"]),
        jsonObject["REPORT_TYPE"],
        tidyTime(jsonObject["UPDATE_DATE"]),
        jsonObject["CURRENCY"]
    )
    ENGINE.execute(sqlstr)


def createColunnIfNotExists(column, type='double'):
    if column in COLUMNS:
        return

    sqlstr = "select count(*) from information_schema.columns where table_schema='eastmoney' and table_name = 'income' and column_name = '%s'"
    sqlstr = sqlstr % column
    cursor = ENGINE.execute(sqlstr)
    (result,) = cursor.fetchone()
    if result == 0:
        addColumnStr = "alter table eastmoney.income add column %s %s DEFAULT NULL;"
        addColumnStr = addColumnStr % (column, type)
        ENGINE.execute(addColumnStr)
        COLUMNS[column] = True


# type = 'double' or 'varchar(20)'
def addColumnToDB(jsonObjects, column, type='double'):
    createColunnIfNotExists(column, type)
    for jsonObject in jsonObjects:
        value = jsonObject[column]
        secucode = tidySecucode(jsonObject["SECUCODE"])
        reportDate = tidyTime(jsonObject["REPORT_DATE"])
        if value is not None:
            if type == 'double':
                sqlstr = "update eastmoney.income set `%s` = %f where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)
            else:
                sqlstr = "update eastmoney.income set `%s` = '%s' where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)


# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def gatherColumnInfo(jsonSql, jsonObjects, column, type='double'):
    createColunnIfNotExists(column, type)
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
            
            sql = "update eastmoney.income %s where SECUCODE='%s' and REPORT_DATE='%s'" % (setStr, code, date)
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
            path = "C:/project/stockdata/EastMoneyIncome/%s.json" % code
            jsonObjects = getJsonFromFile(path)

            # add the base info into DB
            for jsonObject in jsonObjects:
                parseIncomeBasicObject(jsonObject)

            # add other info into DB
            jsonSql = {}
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OPERATE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OPERATE_COST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_COST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'RESEARCH_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_TAX_ADD', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SALE_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MANAGE_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FINANCE_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FE_INTEREST_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FE_INTEREST_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FAIRVALUE_CHANGE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVEST_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVEST_JOINT_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ASSET_DISPOSAL_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ASSET_IMPAIRMENT_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CREDIT_IMPAIRMENT_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OPERATE_PROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NONBUSINESS_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NONBUSINESS_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_PROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INCOME_TAX', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NETPROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CONTINUED_NETPROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PARENT_NETPROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MINORITY_INTEREST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DEDUCT_PARENT_NETPROFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'BASIC_EPS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DILUTED_EPS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_COMPRE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PARENT_OCI', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MINORITY_OCI', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_COMPRE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PARENT_TCI', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MINORITY_TCI', 'double')

            executeSql(jsonSql)
                
            time.sleep(0.1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("parse income error on code: %s" % code)
            time.sleep(3)

    

