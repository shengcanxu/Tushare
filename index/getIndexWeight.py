# 指数权重

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")


def getWeightOnCode(code, tushare, dbEngine):
    try:
        indexWeight = tushare.index_weight(index_code=code, start_date='19901210', end_date='20210228')
        if(len(indexWeight) == 0):
            print("no record on code: %s" % code)
            return

        df = indexWeight.set_index(["index_code", "con_code", "trade_date"])
        tableName = "weight"
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get weight data successfully on code: %s with record: %d" % (code, len(indexWeight)))

    except Exception as ex:
        print(ex)
        print("get weight data error on code: %s" % code)


if __name__ == "__main__":
    # sqlstr = "SELECT ts_code FROM indexdata.indexdata"
    # stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    stockList = pd.read_csv("C:/project/Tushare/index/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]
        getWeightOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

