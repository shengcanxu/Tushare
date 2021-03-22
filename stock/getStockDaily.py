# 日线数据

import tushare as ts
from sqlalchemy import create_engine
import time
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def getDailyOnDate(date, tushare, dbEngine):
    date = date.strftime("%Y%m%d")

    try:
        stockDaily = tushare.daily(trade_date=date)

        for tail in range(0, 100):
            r = tail % 30
            text = str(tail) if tail >= 10 else '0' + str(tail)
            df = stockDaily.loc[stockDaily['ts_code'].str.slice(4, 6) == text]
            df = df.set_index(["ts_code", "trade_date"])
            tableName = "daily" + str(r)
            df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        
        print("get daily data successfully on date: %s" % date)

    except Exception as ex:
        print(ex)
        print("get daily data error on date: %s" % date)


if __name__ == "__main__":
    
    begin = datetime.date(2021, 2, 25)
    end = datetime.date(2021, 2, 25)
    date = begin
    delta = datetime.timedelta(days=1)
    while date <= end:
        date += delta
        getDailyOnDate(date, pro, engine)
        time.sleep(0.5)

