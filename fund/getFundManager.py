# 基金管理人

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundManagerOnCode(code, tushare, dbEngine):
    try:
        manager = tushare.fund_manager(ts_code=code)
        if(len(manager) == 0):
            print("no record on code: %s" % code)
            return

        manager = manager.set_index(["ts_code", "name"])
        tableName = "manager"
        manager.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get fund manager data successfully on code: %s with record: %d" % (code, len(manager)))

    except Exception as ex:
        print(ex)
        print("get fund manager data error on code: %s" % code)


if __name__ == "__main__":
    stockList = pd.read_csv("C:/project/Tushare/index/code.csv").to_numpy()
    step = 50
    stockList = [stockList[i: i+step] for i in range(0, len(stockList), step)]

    for tsCodes in stockList:
        code = ""
        for c in tsCodes:
            code = code + c[0] + ','
        code = code[0:len(code)-1]
        getFundManagerOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

