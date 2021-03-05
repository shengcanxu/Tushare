# %%
import tushare as ts
from sqlalchemy import create_engine
import pandas as pd
import  datetime

#%%
ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()

# %%
df = pro.daily(ts_code='000001.sz', start_date='20180901', end_date='20180918')
df

# %%
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
# df.to_sql("daily", con=engine, if_exists="append")

# %%
# df["ts_code"] = df["ts_code"].astype("string")
# df["trade_date"] = df["trade_date"].astype("string")

# %% 
df2 = df.set_index(["ts_code", "trade_date"])
df2.to_sql(name="daily", con=engine, if_exists="append")
df2

# %%
sqlstr = "SELECT ts_code,list_date FROM test.stockdata"
stockList = pd.read_sql_query(sqlstr, con=engine) 
stockList

# %%
sqlstr = "SELECT max(trade_date) as maxdate FROM stock.daily2021"
maxdate = pd.read_sql_query(sqlstr, con=engine).loc[0, 'maxdate']
maxdate

# %%
for index, row in stockList.iterrows():
    print(row["ts_code"])

# %%
date = stockList[stockList.ts_code == '000002.SZ']["list_date"].to_numpy()[0]
date


# %%
codes = stockList["ts_code"].to_numpy()
for code in codes: 
    print(code)


# %%
df3 = pro.adj_factor(trade_date='19901231')
df3

# %% 
df3 = df3.set_index(["ts_code", "trade_date"])
df3.to_sql(name='adjustfactor1990', con=engine, if_exists="append")

# %%
begin = datetime.date(2014, 6, 1)
end = datetime.date(2014, 6, 7)
d = begin
delta = datetime.timedelta(days=1)
while d <= end:
    year = d.strftime(("%Y"))
    print(year)
    print(d.strftime("%Y%m%d"))
    d += delta



# %%
df6 = pro.fina_mainbz(ts_code='000002.SZ', type='P', start_date='19901210', end_date='20210228')
df6

# %%
df7 = df6.drop_duplicates(['ts_code', 'end_date'])
df7["type"] = "P"
df7

# %%
df7 = df7.set_index(["ts_code", "end_date"])
df7.to_sql(name='mainbusiness', con=engine, if_exists="append")

# %%
indexEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")

# %%
indexDaily = pro.index_daily(ts_code='399300.SZ')
indexDaily

# %%
count = pd.read_sql_query("select count(ts_code) as c from daily1;", con=engine)
count.loc[0,'c']

# %%
stockDaily = pro.daily(trade_date='20210225')
stockDaily

# %%
stockDaily.loc[stockDaily['ts_code'].str.slice(4, 6).isin(['01','02'])]

# %%
len(stockDaily)

# %%
stockDaily.shape
# %%
