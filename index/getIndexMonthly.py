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


def getMonthlyOnCode(code, tushare, dbEngine):
    try:
        indexMonthly = tushare.index_monthly(ts_code=code, start_date='19901210', end_date='20091231')
 
        if(len(indexMonthly) == 0 ):
            return

        df = indexMonthly.set_index(["ts_code", "trade_date"])
        tableName = "monthly"
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get monthly data successfully on code: %s with record: %d" % (code, len(indexMonthly)))

    except Exception as ex:
        print(ex)
        print("get monthly data error on code: %s" % code)


if __name__ == "__main__":
    sqlstr = "SELECT ts_code FROM indexdata.indexdata"
    stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    for tsCode in stockList:
        code = tsCode[0]
        getMonthlyOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

