# 场内基金日线行情

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundDailyOnCode(code, startDate, endDate, tushare, dbEngine):
    try:
        start = datetime.datetime.strptime(startDate, "%Y%m%d")
        delta = datetime.timedelta(days=1000)
        end = datetime.datetime.strptime(endDate, "%Y%m%d")

        d = start
        while d <= end:
            sd = d.strftime("%Y%m%d")
            d += delta
            ed = d.strftime("%Y%m%d")

            daily = tushare.fund_daily(ts_code=code, start_date=sd, end_date=ed)
            daily = daily.drop_duplicates(["ts_code", "trade_date"])
            daily = daily.set_index(["ts_code", "trade_date"])
            tableName = "daily" + str(getDBIndex(code))
            print(tableName)
            daily.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
            print("get fund daily data successfully on code: %s on start date: %s with record: %d" % (code, sd, len(daily)))

    except Exception as ex:
        print(ex)
        print("get fund daily data error on code: %s" % code)


def getDBIndex(code):
    index = code[len(code)-5:len(code)-3]
    index = int(index) % 30
    index = int(index / 10)
    return index


if __name__ == "__main__":
    # 查询语句：select ts_code, found_date, list_date, issue_date, due_date, delist_date from fundlist where market='E';
    stockList = pd.read_csv("C:/project/Tushare/fund/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]
        startDate = str(tsCode[1])
        endDate = str(tsCode[2])

        getFundDailyOnCode(code, startDate, endDate, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute



