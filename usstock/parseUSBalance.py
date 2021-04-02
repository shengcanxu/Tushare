import pandas as pd
from logger import FileLogger
from sqlalchemy import create_engine
import time
import json


usEngine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")
statistic = {}


def parseUSBalance(code):
    path = "C:/project/stockdata/USBalance/%s.json" % code
    text = readFile(path)
    jsonObj = json.loads(text)
    jsonObjList = jsonObj['data']['list']
    
    for data in jsonObjList: 
        for key, value in data.items():
            statisticCount(key)


def statisticCount(key):
    if statistic.keys().__contains__(key):
        statistic[key] += 1
    else:
        statistic[key] = 1


def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        FileLogger.error("read file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False


if __name__ == "__main__":
     
    stockdf = pd.read_csv("C:/project/stockdata/USBalance/code.csv")
    stockList = stockdf['code'].to_numpy()

    for code in stockList:
        # FileLogger.info("running on code: %s" % code)
        try:
            parseUSBalance(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)

    print(statistic)