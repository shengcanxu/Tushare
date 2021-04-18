# %%
import tushare as ts
from sqlalchemy import create_engine
import pandas as pd
import  datetime
import time
import json
import pymysql
import pyecharts

# from helper.getDataFromDB import getDataFromDB
import helper.getDataFromDB as DBLib
import helper.plot as plot


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
indexDaily = pro.index_weekly(ts_code='000004.SH', start_date='19901210', end_date='20091231')
indexDaily2 = pro.index_weekly(ts_code='000004.SH', start_date='20100101', end_date='20210228')
indexDaily
indexDaily2

# %%
indexDaily.to_sql(name='daily', con=indexEngine, if_exists='append')


# %%
indexDaily

# %%
csv = pd.read_csv("C:/project/Tushare/index/code.csv").to_numpy()
csv

# %%
indexWeight = pro.index_weight(index_code='399300.SZ', start_date='20180901', end_date='20180930')
indexWeight = indexWeight.set_index(["index_code", "con_code", "trade_date"])
indexWeight


# %%
indexWeight.to_sql(name='weight', con=indexEngine, if_exists='append')

# %%
indexDailyBasic = pro.index_dailybasic(trade_date='20181018')
indexDailyBasic = indexDailyBasic.set_index(["ts_code", "trade_date"])
indexDailyBasic

# %%
indexDailyBasic.to_sql(name='dailybasic', con=indexEngine, if_exists='append')


# %%
usEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")

# %%
usStockList = pro.us_basic(offset=18000, limit=6000)
usStockList = usStockList.set_index(["ts_code"])
usStockList.to_sql(name='stocklist', con=usEngine, if_exists='append')

# %% 
usStockList

# %%
usDaily = pro.us_daily(trade_date='20210309', offset=6000, limit=6000)
usDaily

# %% 
fundEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


# %%
company = pro.fund_basic(market='O')
company

# %% 
company = company.set_index("ts_code")
company.to_sql(name='fundlist', con=fundEngine, if_exists='append')


# %%
manager = pro.fund_manager(ts_code='150018.SZ')
manager 

# %%
manager = manager.set_index(['ts_code','name'])
manager.to_sql(name='manager', con=fundEngine, if_exists='append')

# %%
shares = pro.fund_share(ts_code='501022.SH')
# shares = shares.set_index(['ts_code', 'trade_date'])
shares

# %%
shares.to_sql(name='share', con=fundEngine, if_exists='append')

# %%
divident = pro.fund_div(ann_date='20081018')
divident


# %%
divident = divident.set_index(['ts_code', 'ann_date'])
divident.to_sql(name="divident", con=fundEngine, if_exists='append')

# %%
nav = pro.fund_nav(ts_code='000010.OF')
nav
# %%
nav.to_csv("C:/project/Tushare/fund/nav.csv")


# %%
portfolio = pro.fund_portfolio(ts_code='001753.OF')
portfolio

# %%
portfolio = portfolio.set_index(["ts_code", "ann_date", "symbol"])
portfolio.to_sql(name="portfolio", con=fundEngine, if_exists='append')

# %%
portfolio.to_csv("C:/project/Tushare/fund/portfolio.csv")

# %%
portfolio = portfolio.drop_duplicates(["ts_code", "ann_date", "symbol"])
portfolio

# %% 
funddaily = pro.fund_adj(ts_code='150018.SZ', start_date='20150101', end_date='20181029')
funddaily = funddaily.drop_duplicates(["ts_code", "trade_date"])
funddaily

# %% 
funddaily = funddaily.set_index(["ts_code", "trade_date"])
funddaily.to_sql(name="adjust", con=fundEngine, if_exists='append')


# %% 
macroEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/macro?charset=utf8")


# %%
shibor = pro.cn_gdp(start_q='1950Q1', end_q='2020Q4')
shibor

# %%
shibor = shibor.set_index(["quarter"])
shibor.to_sql(name="cngdp", con=macroEngine, if_exists='append')

# %%
hkStockList = pro.hk_basic(list_status='P')
hkStockList = hkStockList.drop_duplicates("ts_code")
hkStockList = hkStockList.set_index(["ts_code"])
hkStockList
# %%
hkStockList.to_sql(name="hkdaily", con=engine, if_exists='append')

# %%
stock = pro.hk_daily(trade_date='19901214')
stock
# %%
stock.to_sql(name="hkdaily", con=engine, if_exists='append')

# %%
test = pd.DataFrame(data={}, columns=stock.columns)
test

#%%
for key in stock.index:
    row = stock.loc[key]
    if getDBIndex(row['ts_code']) == 1:
        test.loc[key] = row

#%%
test

# %%
def getDBIndex(code):
    sum = 0
    for i in range(0, len(code)): 
        sum = sum + ord(code[i])
    index = sum % 10
    return index

# %%
exchange = pro.fx_obasic()
exchange
# %%
exchange = exchange.set_index(["ts_code"])
exchange.to_sql(name="exchangedata", con=macroEngine, if_exists="append")

# %%
fxDaily = pro.fx_daily(ts_code='USDCNH.FXCM', start_date='20190101', end_date='20190524')
fxDaily


# %%
fxDaily = fxDaily.set_index(["ts_code", "trade_date"])
fxDaily.to_sql(name="fxdaily", con=macroEngine, if_exists="append")

# %%
usDaily = pro.us_daily(ts_code='AAPL', start_date='19980101', end_date='20210328')
usDaily

# %%
tradecal = pro.us_tradecal(start_date='19800101', end_date='20040923')
tradecal

# %%
usstock = pro.us_basic(offset=18000, limit=6000)
usstock

# %%
usstock.to_csv("C:/project/Tushare/usstock/stock3.csv")

# %%
stockList = pd.read_csv("C:/project/Tushare/usstock/code.csv")
errorList = pd.read_csv("C:/project/Tushare/usstock/get_error_ts_code.csv")


# %%
def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        FileLogger.error("read file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False

# %%
path = "C:/project/stockdata/USIncome/%s.json" % 'AAPL'
text = readFile(path)

# %%
jsonObj = json.loads(text)
jsonData = jsonObj['data']
jsonObjList = jsonData['list']

# %%
del jsonData['list']

# %%
obj = jsonObjList[1]
obj['total_revenue']

# %%
BASE_COLUMNS = ('annual_settle_date', 'currency', 'currency_name', 'last_report_name', 'org_type', 'quote_name', 'sas', 'statuses', 'tip')
cashflowBaseDF = pd.DataFrame(columns=BASE_COLUMNS)
# %%
cashflowBaseDF.append(jsonData, ignore_index=True)

# %%
conn = pymysql.connect(host='localhost', user='root', password='4401821211', database='usstock', charset='utf8')
cursor = conn.cursor()
sql = "select * from daily16 where ts_code = 'AAPL'"

# %%
cursor.execute(sql)
# %%
result = cursor.fetchall()

# %%
time.localtime(1612501200000/1000)

# %%
explain = '每股ADR分配0.1771美元'
moneyStr = ""
pos = 0
for index in range(0, len(explain)):
    char = explain[index]
    if char.isnumeric() or char == '.': 
        moneyStr += char
        pos = index

print(moneyStr)
print(explain[pos+1:])
    
# %%
sqlstr = "select * from  `dailybasic15` where ts_code = '300015.SZ'"
dayList = pd.read_sql_query(sqlstr, con=engine) 
dayList


# %%
from pyecharts.charts import Line, Bar
from pyecharts import options as opts


line = (
    Line()
    .add_xaxis(dayList['trade_date'].tolist())
    .add_yaxis("pe", dayList['pe'].tolist())
    .add_yaxis("ps", dayList['ps'].tolist())
    .set_global_opts(tooltip_opts=opts.TooltipOpts(trigger='axis'),title_opts=opts.TitleOpts("title"))
)
line.render_notebook()


# %%
result = DBLib.getDataFromDB('002271.SZ', 'dailybasic11', ['trade_date', 'pe', 'ps', 'pe_ttm'])
line = createPlotLine(result, 'trade_date', 'pe_ttm')
line.render_notebook()


# %%
str = '002271.SZ'
int(str[len(str)-5 : len(str)-3]) % 30

# %% 
stockData = DBLib.getDataFromDB('002271.SZ', 'daily11')
stockData = stockData.iloc[2900:3001]
stockData


# %%
from pyecharts.charts import Kline, Line, Bar, Grid
from pyecharts import options as opts

stockName = DBLib.getStockDataFromDB('002271.SZ').loc[0, 'name']


def calculate_ma(day_count: int, data):
    result = []
    for i in range(len(data)):
        if i < day_count:
            result.append(None)
            continue
        sum_total = 0.0
        for j in range(day_count):
            sum_total += float(data[i - j][1])
        result.append(abs(float("%.3f" % (sum_total / day_count))))
    return result


kline_data = [[i[0], i[1], i[2], i[3]] for i in stockData[['open', 'close', 'low', 'high']].to_numpy()]
volumes = [i for i in stockData['vol'].to_numpy()]
xaxis_data = [i for i in stockData['trade_date'].to_numpy()]

kline = (
    Kline()
    .add_xaxis(xaxis_data=xaxis_data)
    .add_yaxis(
        series_name=stockName,
        y_axis=kline_data,
        itemstyle_opts=opts.ItemStyleOpts(color="#ec0000", color0="#00da3c"),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="股票日K线"),
        yaxis_opts=opts.AxisOpts(
            is_scale=True,
            splitarea_opts=opts.SplitAreaOpts(
                is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
            ),
        ),
        xaxis_opts=opts.AxisOpts(is_scale=True),
        datazoom_opts=[
            opts.DataZoomOpts(
                type_="inside",
                xaxis_index=[0, 1],
                range_start=int((len(stockData)-60)/len(stockData)*100),
                range_end=100,
            ),
            opts.DataZoomOpts(
                type_="slider",
                pos_top="85%",
                xaxis_index=[0, 1],
                range_start=int((len(stockData)-60)/len(stockData)*100),
                range_end=100,
            ),
        ],
        tooltip_opts=opts.TooltipOpts(
            trigger="axis",
            axis_pointer_type="cross",
            background_color="rgba(245, 245, 245, 0.8)",
            border_width=1,
            border_color="#ccc",
            textstyle_opts=opts.TextStyleOpts(color="#000"),
        ),
        axispointer_opts=opts.AxisPointerOpts(
            is_show=True,
            link=[{"xAxisIndex": "all"}],
            label=opts.LabelOpts(background_color="#777"),
        ),
    )
)

line = (
    Line()
    .add_xaxis(xaxis_data=xaxis_data)
    .add_yaxis(
        series_name="MA5",
        y_axis=calculate_ma(day_count=5, data=kline_data),
        is_smooth=True,
        is_hover_animation=False,
        is_symbol_show=False,
        linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
    .add_yaxis(
        series_name="MA10",
        y_axis=calculate_ma(day_count=10, data=kline_data),
        is_smooth=True,
        is_hover_animation=False,
        is_symbol_show=False,
        linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
    .add_yaxis(
        series_name="MA30",
        y_axis=calculate_ma(day_count=30, data=kline_data),
        is_smooth=True,
        is_hover_animation=False,
        is_symbol_show=False,
        linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
    .add_yaxis(
        series_name="MA60",
        y_axis=calculate_ma(day_count=60, data=kline_data),
        is_smooth=True,
        is_hover_animation=False,
        is_symbol_show=False,
        linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
)
# Kline And Line
overlap_kline_line = kline.overlap(line)

bar = (
    Bar()
    .add_xaxis(xaxis_data=xaxis_data)
    .add_yaxis(
        series_name="Volume",
        y_axis=volumes,
        xaxis_index=1,
        yaxis_index=1,
        label_opts=opts.LabelOpts(is_show=False),
    )
    .set_global_opts(
        legend_opts=opts.LegendOpts(is_show=False),
    )
)

# Grid Overlap + Bar
grid_chart = (
    Grid(init_opts=opts.InitOpts(
            width="1000px",
            height="600px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    .add(
        overlap_kline_line,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="50%"),
    )
    .add(
        bar,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="63%", height="16%"),
    )
) 

grid_chart.render_notebook()

# %%
chart = plot.createKlineOfData('002271.SZ', '20200101', '20201231')
chart.render_notebook()

# %%
