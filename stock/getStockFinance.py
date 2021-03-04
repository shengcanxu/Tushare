# 财务指标数据

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def getFinanceOnCode(tsCode, tushare, dbEngine):
    try:
        stockFinance = tushare.fina_indicator(ts_code=tsCode, start_date='19901210', end_date='20210228')
        df = stockFinance.drop_duplicates(['ts_code', 'end_date'])
        df = df.set_index(["ts_code", "end_date"])

        tableName = "finance"
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        print("get finance data successfully on code: %s" % tsCode)

    except Exception as ex:
        print(ex)
        print("get finance data error on code: %s" % tsCode)


if __name__ == "__main__":
    sqlstr = "SELECT ts_code FROM test.stockdata"
    stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    for tsCode in stockList:
        code = tsCode[0]
        getFinanceOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute
