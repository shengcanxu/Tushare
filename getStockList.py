#grap the stock List info and store to DB

import tushare as ts
from sqlalchemy import create_engine

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
stockData = pro.query('stock_basic', exchange='', list_status='L', fields="ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs")

#store to DB
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
stockData.to_sql("stockdata", con=engine, if_exists="replace")

