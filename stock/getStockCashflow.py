# 现金流量表

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def getCashflowOnCode(tsCode, tushare, dbEngine):
    try:
        stockCashflow = tushare.cashflow(ts_code=tsCode, start_date='20210228', end_date='20210722')
        df = stockCashflow.drop_duplicates(['ts_code', 'end_date', 'report_type'])
        df = df.set_index(["ts_code", "end_date"])

        tableName = "cashflow"
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        print("get cashflow data successfully on code: %s" % tsCode)

    except Exception as ex:
        print(ex)
        print("get cashflow data error on code: %s" % tsCode)


if __name__ == "__main__":
    sqlstr = "SELECT ts_code FROM stock.stockdata"
    stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    for tsCode in stockList:
        code = tsCode[0]
        getCashflowOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute
