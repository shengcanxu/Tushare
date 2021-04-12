import pandas as pd
from helper.logger import FileLogger
from sqlalchemy import create_engine
import time
import json


usEngine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")

COLUMNS = (
    'ts_code',
    'explain',
    'announce_date',
    'dividend_date',
    'exright_date',
    'money',
    'currency'
)


def parseUSDivident(code):
    path = "C:/project/stockdata/USDivident/%s.json" % code
    text = readFile(path)
    jsonObj = json.loads(text)
    jsonObjList = jsonObj['data']['items']
    
    dividentDF = pd.DataFrame(columns=COLUMNS)
    for data in jsonObjList:
        (moneyStr, currency) = getMoneyFromString(data['explain'])
        announceDate = time.strftime('%Y%m%d', time.localtime(data['announcement_date']/1000)) if data['announcement_date'] else None
        dividentDate = time.strftime('%Y%m%d', time.localtime(data['dividend_date']/1000)) if data['dividend_date'] else None
        exrightDate = time.strftime('%Y%m%d', time.localtime(data['exright_date']/1000)) if data['exright_date'] else None
        
        jsonData = {
            'ts_code': code,
            'explain': data['explain'],
            'announce_date': announceDate,
            'dividend_date': dividentDate,
            'exright_date': exrightDate,
            'money': float(moneyStr),
            'currency': currency
        }
        dividentDF = dividentDF.append([jsonData], ignore_index=True)

    dividentDF = dividentDF.drop_duplicates(['ts_code', 'announce_date'])
    dividentDF = dividentDF.set_index(['ts_code', 'announce_date'])
    dividentDF.to_sql(name='divident', con=usEngine, if_exists='append')


def getMoneyFromString(str):
    moneyStr = ""
    pos = 0
    for index in range(0, len(str)):
        char = str[index]
        if char.isnumeric() or char == '.':
            moneyStr += char
            pos = index
    currency = str[pos+1:]
    return (moneyStr, currency)


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
     
    stockdf = pd.read_csv("C:/project/stockdata/USDivident/code.csv")
    stockList = stockdf['code'].to_numpy()

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            parseUSDivident(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)