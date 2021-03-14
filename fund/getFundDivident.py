# 基金分红

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundDividendOnDate(date, tushare, dbEngine):
    try:
        dividend = tushare.fund_div(ann_date=date)    
        dividend = dividend.set_index(["ts_code", "ann_date"])
        tableName = "dividend"
        dividend.to_sql(name=tableName, con=dbEngine, if_exists="append")
    
        print("get fund dividend data successfully on date: %s with record: %d" % (date, len(dividend)))

    except Exception as ex:
        print(ex)
        print("get fund dividend data error on code: %s" % date)


if __name__ == "__main__":
    begin = datetime.date(1990, 12, 10)
    end = datetime.date(2021, 2, 25)
    date = begin
    delta = datetime.timedelta(days=1)
    while date <= end:
        date += delta
        getFundDividendOnDate(date.strftime("%Y%m%d"), pro, engine)
        time.sleep(1)