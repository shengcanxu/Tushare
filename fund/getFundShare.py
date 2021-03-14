# 基金规模数据

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundShareOnCode(code, startDate, endDate, tushare, dbEngine):
    try:
        start = datetime.datetime.strptime(startDate, "%Y%m%d")
        delta = datetime.timedelta(days=2500)
        end = datetime.datetime.strptime(endDate, "%Y%m%d")

        d = start
        while d <= end:
            sd = d.strftime("%Y%m%d")
            d += delta
            ed = d.strftime("%Y%m%d")

            share = tushare.fund_share(ts_code=code, start_date=sd, end_date=ed)    
            share = share.set_index(["ts_code", "trade_date"])
            tableName = "share" 
            print(tableName)
            share.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
            print("get fund share data successfully on code: %s on start date: %s with record: %d" % (code, sd, len(share)))

    except Exception as ex:
        print(ex)
        print("get fund share data error on code: %s" % code)


def getFundShareOnCode2(code, tushare, dbEngine):
    try:
        share = tushare.fund_share(ts_code=code)    
        share = share.set_index(["ts_code", "trade_date"])
        tableName = "share" 
        print(tableName)
        share.to_sql(name=tableName, con=dbEngine, if_exists="append")
    
        if len(share) >= 2000:
            print("more record on code: %s" % code)

        print("get fund share data successfully on code: %s with record: %d" % (code, len(share)))

    except Exception as ex:
        print(ex)
        print("get fund share data error on code: %s" % code)





if __name__ == "__main__":
    stockList = pd.read_csv("C:/project/Tushare/fund/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]
        startDate = str(tsCode[1])
        endDate = str(tsCode[2])

        getFundShareOnCode(code, startDate, endDate, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

    # step = 10
    # stockList = [stockList[i: i+step] for i in range(0, len(stockList), step)]
    # for tsCodes in stockList:
    #     code = ""
    #     for c in tsCodes:
    #         code = code + c[0] + ','
    #     code = code[0:len(code)-1]
    #     getFundShareOnCode2(code, pro, engine)
    #     time.sleep(1.5) # make sure less than 60 query per minute


