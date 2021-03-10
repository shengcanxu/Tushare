# 周线数据

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")


def getWeeklyOnCode(code, tushare, dbEngine):
    try:
        indexWeekly = tushare.index_weekly(ts_code=code, start_date='19901210', end_date='20091231')
        indexWeekly2 = tushare.index_weekly(ts_code=code, start_date='20100101', end_date='20210228')
        
        if(len(indexWeekly) == 0 and len(indexWeekly2) == 0):
            print("no record on code: %s" % code)
            return

        df = indexWeekly.set_index(["ts_code", "trade_date"])
        df2 = indexWeekly2.set_index(["ts_code", "trade_date"])
        tableName = "weekly" + str(getDBIndex(code))
        print(tableName)
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        df2.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get weekly data successfully on code: %s with record: %d" % (code, len(indexWeekly)+len(indexWeekly2)))

    except Exception as ex:
        print(ex)
        print("get weekly data error on code: %s" % code)


def getDBIndex(code):
    sum = 0
    for i in range(0, len(code)): 
        sum = sum + ord(code[i])
    index = sum % 6
    return index


if __name__ == "__main__":
    # sqlstr = "SELECT ts_code FROM indexdata.indexdata"
    # stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    stockList = pd.read_csv("C:/project/Tushare/index/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]
        getWeeklyOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

