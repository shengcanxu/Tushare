import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger


# return dataframe result or None
def _queryFromDB(sql):
    try: 
        engine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")
        result = pd.read_sql_query(sql, con=engine)
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


def getDataFromIncome(code, columns=[],  startDate=None, endDate=None):
    return _getDataFromTable('income', code, columns=columns, startDate=startDate, endDate=endDate)


def getDataFromBalance(code, columns=[], startDate=None, endDate=None):
    return _getDataFromTable('balance', code, columns=columns, startDate=startDate, endDate=endDate)


def getDataFromCashflow(code, columns=[], startDate=None, endDate=None):
    return _getDataFromTable('cashflow', code, columns=columns, startDate=startDate, endDate=endDate)


if __name__ == "__main__":
    datadf = getDataFromIncome('SZ000002')
    print(datadf)