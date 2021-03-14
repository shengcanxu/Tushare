# 公募基金持仓数据

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")


def getFundPortfolioOnCode(code, tushare, dbEngine):
    try:
        portfolio = tushare.fund_portfolio(ts_code=code)
        portfolio = portfolio.drop_duplicates(["ts_code", "ann_date", "symbol"])
        portfolio = portfolio.set_index(["ts_code", "ann_date", "symbol"])

        tableName = "portfolio" + str(getDBIndex(code))
        print(tableName)
        portfolio.to_sql(name=tableName, con=dbEngine, if_exists="append")
    
        print("get fund portfolio data successfully on code: %s with record: %d" % (code, len(portfolio)))

    except Exception as ex:
        print(ex)
        print("get fund portfolio data error on code: %s" % code)


def getDBIndex(code):
    index = code[len(code)-5:len(code)-3]
    index = int(index) % 30
    return index


if __name__ == "__main__":
    stockList = pd.read_csv("C:/project/Tushare/fund/code.csv").to_numpy()

    for tsCode in stockList:
        code = tsCode[0]

        getFundPortfolioOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute

