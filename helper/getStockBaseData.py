#grap the stock List info and store to DB

import tushare as ts
from sqlalchemy import create_engine

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")

# 股票基础数据
stockData = pro.query('stock_basic', exchange='', list_status='L', fields="ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs")
stockData.to_sql("stockdata", con=engine, if_exists="append")

# 指数基本数据
indexEngine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")
for market in ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'OTH']:
    indexData = pro.index_basic(market=market)
    indexData = indexData.set_index(["ts_code"])
    indexData.to_sql("indexdata", con=indexEngine, if_exists='append')

