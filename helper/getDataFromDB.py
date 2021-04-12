import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger


# indexes: str or array, 需要取的字段
def getDataFromDB(code, tableName, indexes, startDate=None, endDate=None):
    if not startDate:
        startDate = datetime.date(1990, 12, 10).strftime("%Y%m%d")
    if not endDate:
        endDate = datetime.datetime.now().strftime("%Y%m%d")
    if type(indexes) == list:
        indexes = ','.join(indexes)

    sqlstr = "select %s from  `%s` where ts_code = '%s' and trade_date between '%s' and '%s'" % (indexes, tableName, code, startDate, endDate)
    
    try: 
        engine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
        result = pd.read_sql_query(sqlstr, con=engine)
        return result

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("read from db error!")