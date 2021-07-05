import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger

ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")
COLUMNS = {}


# return dataframe result or None
def _queryFromDB(sql):
    try:
        result = pd.read_sql_query(sql, con=ENGINE)
        return result
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("read from db error!")
        return None


def _getDataFromTable(table, code, columns=[], startDate=None, endDate=None):
    if not startDate:
        startDate = datetime.date(1990, 12, 10).strftime("%Y-%m-%d")
    if not endDate:
        endDate = datetime.datetime.now().strftime("%Y-%m-%d")

    sqlstr = "select * from  `eastmoney`.`%s` where SECUCODE = '%s' and REPORT_DATE between '%s' and '%s' order by REPORT_DATE desc" % (table, code, startDate, endDate)
    datadf = _queryFromDB(sqlstr)
    if len(columns) > 0:
        datadf = datadf[columns]
    return datadf


# 从数据库中查询出股票代码为code的income数据
def getDataFromIncome(code, columns=[],  startDate=None, endDate=None):
    return _getDataFromTable('income', code, columns=columns, startDate=startDate, endDate=endDate)


# 从数据库中查询出股票代码为code的balance数据
def getDataFromBalance(code, columns=[], startDate=None, endDate=None):
    return _getDataFromTable('balance', code, columns=columns, startDate=startDate, endDate=endDate)


# 从数据库中查询出股票代码为code的cashflow数据
def getDataFromCashflow(code, columns=[], startDate=None, endDate=None):
    return _getDataFromTable('cashflow', code, columns=columns, startDate=startDate, endDate=endDate)


# 从数据库中查询出股票代码为code的financialindex数据
def getDataFromFinIndex(code, columns=[], startDate=None, endDate=None):
    return _getDataFromTable('financialindex', code, columns=columns, startDate=startDate, endDate=endDate)


# 如果tablename里的column字段不存在，就创建它
def createColunnIfNotExists(tableName, column, type='double'):
    columnIndex = "%s_%s" % (tableName, column)
    if columnIndex in COLUMNS:
        return

    sqlstr = "select count(*) from information_schema.columns where table_schema='eastmoney' and table_name = '%s' and column_name = '%s'" % (tableName, column)
    cursor = ENGINE.execute(sqlstr)
    (result,) = cursor.fetchone()
    if result == 0:
        addColumnStr = "alter table `eastmoney`.`%s` add column %s %s DEFAULT NULL;" % (tableName, column, type)
        ENGINE.execute(addColumnStr)
        COLUMNS[columnIndex] = True
    else:
        COLUMNS[columnIndex] = True
        

if __name__ == "__main__":
    datadf = getDataFromIncome('SZ000002')
    print(datadf)