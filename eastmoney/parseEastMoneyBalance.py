##
# 将balance表的json数据解释到数据库
# 创建数据库的代码：
# CREATE TABLE `balance` (
#   `SECUCODE` varchar(20),
#   `REPORT_DATE` varchar(20),
# `SECURITY_NAME_ABBR`  varchar(20),
# `REPORT_TYPE` varchar(20),
# `UPDATE_DATE` varchar(20),
# `CURRENCY`    varchar(20),
#   UNIQUE KEY `idx_balance_SECUCODE_REPORT_DATE` (`SECUCODE`,`REPORT_DATE`)
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
from helper.util import getJsonFromFile


ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")


# change XXXXXX.SZ to SZXXXXXX
def tidySecucode(code):
    return code[7:9] + code[0:6]


# change XXXX-XX-XX 00:00:00 to XXXX-XX-XX
def tidyTime(t):
    return t[0:10]


# parse basic infomation of a single balance object
def parseBalanceBasicObject(jsonObject):
    sqlstr = "insert into eastmoney.balance(SECUCODE,SECURITY_NAME_ABBR,REPORT_DATE,REPORT_TYPE,UPDATE_DATE,CURRENCY) values('%s','%s','%s','%s','%s','%s')"
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
    dataGetter.createColunnIfNotExists('balance', column, type)
    for jsonObject in jsonObjects:
        value = jsonObject[column]
        secucode = tidySecucode(jsonObject["SECUCODE"])
        reportDate = tidyTime(jsonObject["REPORT_DATE"])
        if value is not None:
            if type == 'double':
                sqlstr = "update eastmoney.balance set `%s` = %f where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)
            else:
                sqlstr = "update eastmoney.balance set `%s` = '%s' where SECUCODE='%s' and REPORT_DATE='%s'" % (column, value, secucode, reportDate)
                ENGINE.execute(sqlstr)


# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def gatherColumnInfo(jsonSql, jsonObjects, column, type='double'):
    dataGetter.createColunnIfNotExists('balance', column, type)
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
            
            sql = "update eastmoney.balance %s where SECUCODE='%s' and REPORT_DATE='%s'" % (setStr, code, date)
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
            path = "C:/project/stockdata/EastMoneyBalance/%s.json" % code
            jsonObjects = getJsonFromFile(path)

            # add the base info into DB
            for jsonObject in jsonObjects:
                parseBalanceBasicObject(jsonObject)

            # add other info into DB
            jsonSql = {}
            gatherColumnInfo(jsonSql, jsonObjects, 'ACCOUNTS_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ACCOUNTS_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'ADVANCE_RECEIVABLES', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'BOND_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CAPITAL_RESERVE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CIP', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CONTRACT_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'CONTRACT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DEFER_TAX_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DEFER_TAX_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DERIVE_FINASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DERIVE_FINLIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DIVIDEND_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'DIVIDEND_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'FIXED_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'GOODWILL', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'HOLDSALE_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INTANGIBLE_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INTEREST_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVENTORY', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'INVEST_REALESTATE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'LEASE_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'LONG_EQUITY_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'LONG_LOAN', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'LONG_PREPAID_EXPENSE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MINORITY_EQUITY', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'MONETARYFUNDS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NONCURRENT_LIAB_1YEAR', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NOTE_ACCOUNTS_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NOTE_ACCOUNTS_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NOTE_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'NOTE_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_COMPRE_INCOME', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_CURRENT_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_CURRENT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_EQUITY_INVEST', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_NONCURRENT_ASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_NONCURRENT_FINASSET', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'OTHER_NONCURRENT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PREDICT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'PREPAYMENT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SHARE_CAPITAL', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SHORT_LOAN', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'STAFF_SALARY_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'SURPLUS_RESERVE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TAX_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_ASSETS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_CURRENT_ASSETS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_CURRENT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_EQUITY', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_LIAB_EQUITY', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_LIABILITIES', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_NONCURRENT_ASSETS', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_NONCURRENT_LIAB', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OTHER_PAYABLE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_OTHER_RECE', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TOTAL_PARENT_EQUITY', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'TRADE_FINASSET_NOTFVTPL', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'UNASSIGN_RPOFIT', 'double')
            gatherColumnInfo(jsonSql, jsonObjects, 'USERIGHT_ASSET', 'double')

            executeSql(jsonSql)
                
            time.sleep(0.1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("parse balance error on code: %s" % code)
            time.sleep(3)

    

