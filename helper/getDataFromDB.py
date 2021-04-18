import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger


# indexes: str or array, 需要取的字段
def getDataFromDB(code, tableName, indexes=None, startDate=None, endDate=None, db='stock'):
    if not startDate:
        startDate = datetime.date(1990, 12, 10).strftime("%Y%m%d")
    if not endDate:
        endDate = datetime.datetime.now().strftime("%Y%m%d")
    if type(indexes) == list:
        indexes = ','.join(indexes)
    elif indexes is None or indexes == "*":
        indexes = "*"

    sqlstr = "select %s from  `%s`.`%s` where ts_code = '%s' and trade_date between '%s' and '%s'" % (indexes, db, tableName, code, startDate, endDate)
    return queryFromDB(sqlstr)


def getStockDataFromDB(value, by='ts_code'):
    sqlstr = "select * from `stock`.`stockdata` where `%s` = '%s'" % (by, value)
    return queryFromDB(sqlstr)


# return dataframe result or None
def queryFromDB(sql):
    try: 
        engine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
        result = pd.read_sql_query(sql, con=engine)
        return result
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("read from db error!")
        return None

