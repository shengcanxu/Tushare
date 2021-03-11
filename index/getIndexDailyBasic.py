# 基础指标数据

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")


def getDailyBasicOnCode(code, tushare, dbEngine):
    try:
        tableName = "dailybasic"

        indexDailyBasic = tushare.index_dailybasic(ts_code=code, start_date='19901210', end_date='19991231')
        df = indexDailyBasic.set_index(["ts_code", "trade_date"])
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")

        indexDailyBasic = tushare.index_dailybasic(ts_code=code, start_date='20000101', end_date='20091231')
        df = indexDailyBasic.set_index(["ts_code", "trade_date"])
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")

        indexDailyBasic = tushare.index_dailybasic(ts_code=code, start_date='20100101', end_date='20210228')
        df = indexDailyBasic.set_index(["ts_code", "trade_date"])
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get weight data successfully on code: %s with record: %d" % (code, len(indexDailyBasic)))

    except Exception as ex:
        print(ex)
        print("get weight data error on code: %s" % code)


if __name__ == "__main__":
    stockList = ["000001.SH", "000300.SH", "000905.SH", "399001.SZ", "399005.SZ", "399006.SZ", "399016.SZ", "399300.SZ", "000005.SH", "000006.SH", "000016.SH", "399905.SZ"]

    for code in stockList:
        getDailyBasicOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

