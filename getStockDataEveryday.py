# 更新日线数据， 周线数据， 月线数据

import tushare as ts
from sqlalchemy import create_engine
import time
import datetime
import pandas as pd
from getStockDaily import getDailyOnDate
from getStockWeekly import getWeeklyOnDate
from getStockMonthly import getMonthlyOnDate

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


sqlstr = "SELECT max(trade_date) as maxdate FROM stock.daily2021"
maxDate = pd.read_sql_query(sqlstr, con=engine).loc[0, 'maxdate']
print("previous crawle date is %s, start updating!" % maxDate)

begin = datetime.datetime.strptime(maxDate, '%Y%m%d')
end = datetime.datetime.now()
date = begin
delta = datetime.timedelta(days=1)
while date < end:
    date += delta

    getDailyOnDate(date, pro, engine)
    time.sleep(1)
    getWeeklyOnDate(date, pro, engine)
    time.sleep(1)
    getMonthlyOnDate(date, pro, engine)
    time.sleep(1)
