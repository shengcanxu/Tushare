# 公募基金净值

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundNavOnCode(code, tushare, dbEngine):
    try:
        nav = tushare.fund_nav(ts_code=code)    
        nav = nav.set_index(["ts_code", "end_date"])
        tableName = "nav" + str(getDBIndex(code))
        print(tableName)
        nav.to_sql(name=tableName, con=dbEngine, if_exists="append")
    
        print("get fund nav data successfully on code: %s with record: %d" % (code, len(share)))

    except Exception as ex:
        print(ex)
        print("get fund nav data error on code: %s" % code)


def getDBIndex(code):
    index = code[len(code)-5:len(code)-3]
    index = int(index) % 30
    return index


if __name__ == "__main__":
    stockList = pd.read_csv("C:/project/Tushare/fund/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]

        getFundNavOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

