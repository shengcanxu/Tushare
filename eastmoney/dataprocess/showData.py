# %%
import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from helper.logger import FileLogger
import helper.plot as plot
import helper.getDataFromDB as DBLib
import eastmoney.dataprocess.getFinancailDataFromDB as dataGetter


datadf = dataGetter.getDataFromIncome('SZ000002')
datadf

# # %%
# chart = plot.createKlineOfData('000002.SZ', '20200101', '20210631')
# chart.render_notebook()

# result = DBLib.getDataFromDB('002594.SZ', 'dailybasic4', ['trade_date', 'pe', 'ps', 'pe_ttm'])
# line = plot.createPlotLine(result, 'trade_date', 'pe')
# line.render_notebook()
# %%
