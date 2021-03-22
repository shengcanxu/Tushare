# 港股日线数据

import tushare as ts
from sqlalchemy import create_engine
import time
import datetime
import pandas as pd

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def getDailyOnDate(date, tushare, dbEngine):
    date = date.strftime("%Y%m%d")

    try:
        stockDaily = tushare.hk_daily(trade_date=date)

        for index in range(0, 10):
            stocks = pd.DataFrame(data={}, columns=stockDaily.columns)
            for key in stockDaily.index:
                row = stockDaily.loc[key]
                if getDBIndex(row['ts_code']) == index:
                    stocks.loc[key] = row
            
            stocks = stocks.set_index(["ts_code", "trade_date"])
            tableName = "hkdaily" + str(index)
            stocks.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get HK daily data successfully on date: %s with record: %d" % (date, len(stockDaily)))

    except Exception as ex:
        print(ex)
        print("get HK daily data error on date: %s" % date)


def getDBIndex(code):
    sum = 0
    for i in range(0, len(code)): 
        sum = sum + ord(code[i])
    index = sum % 10
    return index


if __name__ == "__main__":
    
    begin = datetime.date(1990, 12, 10)
    end = datetime.date(2021, 2, 25)
    date = begin
    delta = datetime.timedelta(days=1)
    while date <= end:
        date += delta
        getDailyOnDate(date, pro, engine)
        time.sleep(1)

