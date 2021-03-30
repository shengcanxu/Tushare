import pandas as pd
from logger import FileLogger
from sqlalchemy import create_engine
import time

usEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")


def parseUSDaily(code):
    path = "C:/project/stockdata/UShistory/%s.csv" % code
    daily = pd.read_csv(path)
    
    # only retrieve the needed columns
    daily = daily[['timestamp', 'volume', 'open', 'high', 'low', 'close', 'chg', 'percent', 'turnoverrate', 'amount', 'pe', 'pb', 'ps', 'pcf', 'market_capital']]
    daily['ts_code'] = code
    daily['trade_date'] = daily['timestamp'].map(lambda x: time.strftime('%Y/%m/%d', time.localtime(x/1000)))
    daily = daily.drop('timestamp', axis=1)

    # change datatype to float64
    daily = daily.replace(to_replace='None', value='0')
    datatype = {
        "volume":         "int64",
        "open":              "float64",
        "high":               "float64",
        "low":                "float64",
        "close":              "float64",
        "chg":                 "float64",
        "percent":             "float64",
        "turnoverrate":        "float64",
        "amount":              "float64",
        "pe":                  "float64",
        "pb":                  "float64",
        "ps":                  "float64",
        "pcf":                 "float64",
        "market_capital":      "float64",
        "ts_code":             "object",
        "trade_date":          "object"
    }
    daily = daily.astype(datatype)
    daily = daily.drop_duplicates(['ts_code', 'trade_date'])
    daily = daily.set_index(['ts_code', 'trade_date'])

    tableName = "daily" + str(getDBIndex(code))
    print(tableName)
    daily.to_sql(name=tableName, con=usEngine, if_exists="append")

    FileLogger.info("write data to Database successfully on code: %s" % code)


def getDBIndex(code):
    sum = 0
    for i in range(0, len(code)):
        sum = sum + ord(code[i])
    index = sum % 30
    return index


if __name__ == "__main__":
    # 查询语句：select ts_code from usstock.stocklist; 
    stockdf = pd.read_csv("C:/project/Tushare/usstock/code.csv")
    errordf = pd.read_csv("C:/project/Tushare/usstock/get_error_ts_code.csv")
    errorList = errordf['ts_code'].to_numpy()
    stockList = stockdf[~stockdf['ts_code'].isin(errorList)]
    stockList = stockList['ts_code'].to_numpy()

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            parseUSDaily(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)